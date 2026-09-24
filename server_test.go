package kiln

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
	"github.com/rafaelaugustos/kiln/memstore"
)

type testArgs struct {
	K string
	N int
}

func (a testArgs) Kind() string { return a.K }

type ping struct{ N int }

func (ping) Kind() string { return "ping" }

func fastConfig() ServerConfig {
	return ServerConfig{
		Queues:            map[string]int{DefaultQueue: 8},
		PollInterval:      20 * time.Millisecond,
		FetchCooldown:     time.Millisecond,
		ShutdownTimeout:   time.Second,
		KillGrace:         50 * time.Millisecond,
		HeartbeatInterval: 50 * time.Millisecond,
		DeadAfter:         5200 * time.Millisecond,
		LeaderTTL:         300 * time.Millisecond,
	}
}

func runServer(t testing.TB, c *Client, m *Mux, cfg ServerConfig) (*Server, func()) {
	t.Helper()
	s, err := NewServer(c, m, cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			if err := <-done; err != nil {
				t.Errorf("run: %v", err)
			}
		})
	}
	t.Cleanup(stop)
	eventually(t, 5*time.Second, "server start", func() bool { return s.Healthy() == nil })
	return s, stop
}

func eventually(t testing.TB, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func receive[T any](t testing.TB, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting on channel")
	}
	var zero T
	return zero
}

func getJob(t testing.TB, c *Client, id int64) Record {
	t.Helper()
	r, err := c.Get(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func waitState(t testing.TB, c *Client, id int64, st State) Record {
	t.Helper()
	var r Record
	eventually(t, 5*time.Second, fmt.Sprintf("job %d to be %s", id, st), func() bool {
		r = getJob(t, c, id)
		return r.State == st
	})
	return r
}

func reasons(r Record) []string {
	var out []string
	for _, e := range r.History {
		out = append(out, e.Reason)
	}
	return out
}

func mustEnqueue(t testing.TB, c *Client, args Args, opts ...InsertOption) int64 {
	t.Helper()
	id, err := c.Enqueue(context.Background(), args, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

type bareStore struct{ driver.Store }

var errDown = errors.New("store down")

type faulty struct {
	driver.Store
	down        atomic.Bool
	finishFails atomic.Int32
}

func (f *faulty) Heartbeat(ctx context.Context, info driver.ServerInfo) (driver.Directives, error) {
	if f.down.Load() {
		return driver.Directives{}, errDown
	}
	return f.Store.Heartbeat(ctx, info)
}

func (f *faulty) Claim(ctx context.Context, q driver.ClaimQuery) ([]driver.Job, error) {
	if f.down.Load() {
		return nil, errDown
	}
	return f.Store.Claim(ctx, q)
}

func (f *faulty) Finish(ctx context.Context, server string, outs []driver.Outcome) ([]driver.Result, error) {
	if f.down.Load() || f.finishFails.Add(-1) >= 0 {
		return nil, errDown
	}
	return f.Store.Finish(ctx, server, outs)
}

func (f *faulty) Unregister(ctx context.Context, server string) error {
	if f.down.Load() {
		return errDown
	}
	return f.Store.Unregister(ctx, server)
}

type rejecting struct {
	driver.Store
	mu   sync.Mutex
	seen map[int64]bool
}

func (r *rejecting) Finish(ctx context.Context, server string, outs []driver.Outcome) ([]driver.Result, error) {
	res := make([]driver.Result, len(outs))
	var pass []driver.Outcome
	var idx []int
	r.mu.Lock()
	for i, o := range outs {
		if o.State == driver.Succeeded && !r.seen[o.ID] {
			r.seen[o.ID] = true
			res[i] = driver.Rejected
			continue
		}
		pass = append(pass, o)
		idx = append(idx, i)
	}
	r.mu.Unlock()
	if len(pass) == 0 {
		return res, nil
	}
	got, err := r.Store.Finish(ctx, server, pass)
	if err != nil {
		return nil, err
	}
	for k, i := range idx {
		res[i] = got[k]
	}
	return res, nil
}

func TestServerSucceeds(t *testing.T) {
	t.Parallel()
	c := NewClient(memstore.New())
	m := NewMux()
	type seen struct {
		client *Client
		job    *RawJob
		id     int64
	}
	got := make(chan seen, 1)
	Handle(m, func(ctx context.Context, j *Job[ping]) error {
		cl, _ := ClientFrom(ctx)
		raw, _ := JobFrom(ctx)
		got <- seen{cl, raw, j.ID}
		return j.SetOutput(map[string]int{"n": j.Args.N * 2})
	})
	runServer(t, c, m, fastConfig())
	id := mustEnqueue(t, c, ping{N: 21})
	r := waitState(t, c, id, Succeeded)
	if string(r.Output) != `{"n":42}` {
		t.Fatalf("output = %s", r.Output)
	}
	if r.Attempt != 1 || len(r.History) != 0 {
		t.Fatalf("attempt %d history %v", r.Attempt, r.History)
	}
	s := <-got
	if s.client != c || s.job == nil || s.job.ID != id || s.id != id || s.job.Kind != "ping" {
		t.Fatalf("context values: %+v", s)
	}
}

func TestServerRetriesThenFails(t *testing.T) {
	t.Parallel()
	c := NewClient(memstore.New())
	m := NewMux()
	var calls atomic.Int32
	m.HandleFunc("flaky", func(context.Context, *RawJob) error {
		calls.Add(1)
		return errors.New("boom\x00")
	}, Constant(5*time.Millisecond))
	runServer(t, c, m, fastConfig())
	id := mustEnqueue(t, c, testArgs{K: "flaky"}, MaxAttempts(3))
	r := waitState(t, c, id, Failed)
	if want := []string{"retry", "retry", "exhausted"}; !slices.Equal(reasons(r), want) {
		t.Fatalf("reasons = %v, want %v", reasons(r), want)
	}
	if r.Attempt != 3 || calls.Load() != 3 {
		t.Fatalf("attempt %d calls %d", r.Attempt, calls.Load())
	}
	if e := r.History[2].Error; e != "boom" {
		t.Fatalf("error = %q", e)
	}
}

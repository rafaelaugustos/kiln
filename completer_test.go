package kiln

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
	"github.com/rafaelaugustos/kiln/memstore"
)

func mustEnqueueMany(t testing.TB, c *Client, n int, args Args, opts ...InsertOption) []int64 {
	t.Helper()
	specs := make([]Spec, n)
	for i := range specs {
		specs[i] = Spec{Args: args, Options: opts}
	}
	res, err := c.EnqueueMany(context.Background(), specs...)
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]int64, n)
	for i, r := range res {
		ids[i] = r.ID
	}
	return ids
}

func TestCompleterRetriesFinish(t *testing.T) {
	t.Parallel()
	f := &faulty{Store: memstore.New()}
	f.finishFails.Store(4)
	c := NewClient(f)
	m := NewMux()
	m.HandleFunc("work", func(context.Context, *RawJob) error { return nil })
	s, _ := runServer(t, c, m, fastConfig())
	ids := mustEnqueueMany(t, c, 40, testArgs{K: "work"})
	eventually(t, 5*time.Second, "all succeeded", func() bool { return s.Stats().Succeeded == uint64(len(ids)) })
	for _, id := range ids {
		if r := getJob(t, c, id); r.State != Succeeded || r.Attempt != 1 {
			t.Fatalf("job %d: %s attempt %d", id, r.State, r.Attempt)
		}
	}
	if f.finishFails.Load() >= 0 {
		t.Fatal("finish never failed")
	}
}

func TestCompleterRewritesRejected(t *testing.T) {
	t.Parallel()
	c := NewClient(&rejecting{Store: memstore.New(), seen: make(map[int64]bool)})
	m := NewMux()
	m.HandleFunc("work", func(_ context.Context, j *RawJob) error { return j.SetOutput("fine") })
	runServer(t, c, m, fastConfig())
	id := mustEnqueue(t, c, testArgs{K: "work"})
	r := waitState(t, c, id, Failed)
	if !slices.Equal(reasons(r), []string{"rejected"}) || r.History[0].Error != "kiln: outcome rejected" || r.Output != nil {
		t.Fatalf("reasons %v history %+v output %s", reasons(r), r.History, r.Output)
	}
}

func TestServerShutdownRequeues(t *testing.T) {
	t.Parallel()
	c := NewClient(memstore.New())
	started, causes := make(chan int64, 1), make(chan error, 1)
	m := NewMux()
	m.HandleFunc("block", blocking(started, causes))
	cfg := fastConfig()
	cfg.ShutdownTimeout = 30 * time.Millisecond
	s, stop := runServer(t, c, m, cfg)
	id := mustEnqueue(t, c, testArgs{K: "block"})
	receive(t, started)
	stop()
	if cause := receive(t, causes); cause != ErrShutdown {
		t.Fatalf("cause = %v", cause)
	}
	r := getJob(t, c, id)
	if r.State != Enqueued || r.Attempt != 0 || !slices.Equal(reasons(r), []string{"shutdown"}) {
		t.Fatalf("state %s attempt %d reasons %v", r.State, r.Attempt, reasons(r))
	}
	if st := s.Stats(); st.Abandoned != 0 || st.Pending != 0 {
		t.Fatalf("stats %+v", st)
	}
	servers, _ := c.Store().Servers(context.Background())
	if len(servers) != 0 {
		t.Fatalf("servers after stop: %v", servers)
	}
}

func TestServerAbandonsStuckJobs(t *testing.T) {
	t.Parallel()
	c := NewClient(memstore.New())
	started := make(chan int64, 1)
	release := make(chan struct{})
	m := NewMux()
	m.HandleFunc("stuck", func(_ context.Context, j *RawJob) error {
		started <- j.ID
		<-release
		return nil
	})
	cfg := fastConfig()
	cfg.ShutdownTimeout = 20 * time.Millisecond
	cfg.KillGrace = 20 * time.Millisecond
	s, stop := runServer(t, c, m, cfg)
	id := mustEnqueue(t, c, testArgs{K: "stuck"})
	receive(t, started)
	stop()
	close(release)
	r := getJob(t, c, id)
	if r.State != Enqueued || r.Attempt != 0 || !slices.Equal(reasons(r), []string{"shutdown"}) || !strings.Contains(r.History[0].Error, "abandoned") {
		t.Fatalf("state %s attempt %d history %+v", r.State, r.Attempt, r.History)
	}
	if s.Stats().Abandoned != 1 {
		t.Fatalf("abandoned = %d", s.Stats().Abandoned)
	}
}

type unfinished struct {
	driver.Store
	ok chan struct{}
}

func (u *unfinished) Finish(ctx context.Context, server string, outs []driver.Outcome) ([]driver.Result, error) {
	select {
	case <-u.ok:
		return u.Store.Finish(ctx, server, outs)
	default:
		return nil, errDown
	}
}

func TestServerStopsWithStalledCompleter(t *testing.T) {
	t.Parallel()
	store := &unfinished{Store: memstore.New(), ok: make(chan struct{})}
	c := NewClient(store)
	m := NewMux()
	m.HandleFunc("work", func(context.Context, *RawJob) error { return nil })
	cfg := fastConfig()
	cfg.ShutdownTimeout, cfg.KillGrace = 50*time.Millisecond, 50*time.Millisecond
	s, err := NewServer(c, m, cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	mustEnqueueMany(t, c, 700, testArgs{K: "work"})
	eventually(t, 10*time.Second, "stalled completer", func() bool {
		st := s.Stats()
		return st.Pending >= 2*flushSize && st.Running == st.Capacity
	})
	go func() {
		<-s.comp.quit
		close(store.ok)
	}()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return")
	}
}

func TestDrainIgnoresDisplacedTask(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	m := NewMux()
	m.HandleFunc("stuck", func(context.Context, *RawJob) error {
		<-release
		return nil
	})
	cfg := fastConfig()
	cfg.ShutdownTimeout, cfg.KillGrace = 20*time.Millisecond, 20*time.Millisecond
	s, err := NewServer(NewClient(memstore.New()), m, cfg)
	if err != nil {
		t.Fatal(err)
	}
	s.base = context.Background()
	p := s.prods[0]
	job := driver.Job{Ref: driver.Ref{ID: 1, Claim: 1}, Kind: "stuck", Attempt: 1, MaxAttempts: 1, CreatedAt: time.Now()}
	p.running.Add(1)
	s.start(p, []driver.Job{job})
	s.mu.Lock()
	s.tasks[job.ID].cancel(errFenced)
	s.mu.Unlock()
	job.Claim, job.Attempt = 2, 2
	p.running.Add(1)
	s.start(p, []driver.Job{job})
	drained := make(chan struct{})
	go func() {
		s.drain()
		close(drained)
	}()
	select {
	case <-drained:
	case <-time.After(5 * time.Second):
		t.Fatal("drain waited on a displaced task")
	}
}

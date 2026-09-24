package kiln

import (
	"context"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
	"github.com/rafaelaugustos/kiln/memstore"
)

func blocking(started chan<- int64, causes chan<- error) HandlerFunc {
	return func(ctx context.Context, j *RawJob) error {
		started <- j.ID
		<-ctx.Done()
		causes <- context.Cause(ctx)
		return ctx.Err()
	}
}

func TestServerCancelRunning(t *testing.T) {
	t.Parallel()
	for _, notify := range []bool{true, false} {
		t.Run(map[bool]string{true: "notifier", false: "heartbeat"}[notify], func(t *testing.T) {
			t.Parallel()
			var store driver.Store = memstore.New()
			if !notify {
				store = bareStore{store}
			}
			c := NewClient(store)
			started, causes := make(chan int64, 1), make(chan error, 1)
			m := NewMux()
			m.HandleFunc("block", blocking(started, causes))
			runServer(t, c, m, fastConfig())
			id := mustEnqueue(t, c, testArgs{K: "block"})
			receive(t, started)
			if n, err := c.Delete(context.Background(), id); err != nil || n != 1 {
				t.Fatalf("delete: %d %v", n, err)
			}
			if cause := receive(t, causes); cause != ErrCanceled {
				t.Fatalf("cause = %v", cause)
			}
			r := waitState(t, c, id, Deleted)
			if !slices.Equal(reasons(r), []string{"canceled"}) {
				t.Fatalf("reasons = %v", reasons(r))
			}
		})
	}
}

func TestServerRequeuesLostClaim(t *testing.T) {
	t.Parallel()
	store := memstore.New()
	c := NewClient(store)
	m := NewMux()
	m.HandleFunc("lost", func(context.Context, *RawJob) error { return nil })
	s, _ := runServer(t, c, m, fastConfig())
	id := mustEnqueue(t, c, testArgs{K: "lost"}, Queue("elsewhere"))
	jobs, err := store.Claim(context.Background(), driver.ClaimQuery{Queues: []string{"elsewhere"}, Limit: 1, Server: s.ID()})
	if err != nil || len(jobs) != 1 {
		t.Fatalf("claim: %v %v", jobs, err)
	}
	r := waitState(t, c, id, Enqueued)
	if !slices.Equal(reasons(r), []string{"lost"}) || r.Attempt != 1 {
		t.Fatalf("reasons %v attempt %d", reasons(r), r.Attempt)
	}
}

func TestServerFencesStolenJob(t *testing.T) {
	t.Parallel()
	store := memstore.New()
	c := NewClient(store)
	started, causes := make(chan int64, 1), make(chan error, 1)
	m := NewMux()
	m.HandleFunc("block", blocking(started, causes))
	s, _ := runServer(t, c, m, fastConfig())
	id := mustEnqueue(t, c, testArgs{K: "block"})
	receive(t, started)
	r := getJob(t, c, id)
	res, err := store.Finish(context.Background(), "thief", []driver.Outcome{{Ref: r.Ref, State: driver.Failed, Reason: "stolen"}})
	if err != nil || res[0] != driver.Applied {
		t.Fatalf("finish: %v %v", res, err)
	}
	if cause := receive(t, causes); cause != errFenced {
		t.Fatalf("cause = %v", cause)
	}
	eventually(t, time.Second, "task release", func() bool { return s.Stats().Running == 0 })
	s.mu.Lock()
	n := len(s.tasks)
	s.mu.Unlock()
	if n != 0 {
		t.Fatalf("%d tasks still tracked", n)
	}
	r = getJob(t, c, id)
	if r.State != Failed || !slices.Equal(reasons(r), []string{"stolen"}) || s.Stats().Stale != 0 {
		t.Fatalf("state %s reasons %v stale %d", r.State, reasons(r), s.Stats().Stale)
	}
}

func TestServerPausedQueue(t *testing.T) {
	t.Parallel()
	c := NewClient(memstore.New())
	m := NewMux()
	m.HandleFunc("work", func(context.Context, *RawJob) error { return nil })
	s, _ := runServer(t, c, m, fastConfig())
	ctx := context.Background()
	if err := c.PauseQueue(ctx, DefaultQueue); err != nil {
		t.Fatal(err)
	}
	eventually(t, time.Second, "paused directive", func() bool {
		p := s.paused.Load()
		return p != nil && slices.Contains(*p, DefaultQueue)
	})
	id := mustEnqueue(t, c, testArgs{K: "work"})
	time.Sleep(100 * time.Millisecond)
	if r := getJob(t, c, id); r.State != Enqueued {
		t.Fatalf("state = %s while paused", r.State)
	}
	if err := c.ResumeQueue(ctx, DefaultQueue); err != nil {
		t.Fatal(err)
	}
	waitState(t, c, id, Succeeded)
}

type silent struct {
	driver.Store
	mute   atomic.Bool
	missed atomic.Int32
}

func (s *silent) Heartbeat(ctx context.Context, info driver.ServerInfo) (driver.Directives, error) {
	if s.mute.Load() {
		s.missed.Add(1)
		return driver.Directives{}, errDown
	}
	return s.Store.Heartbeat(ctx, info)
}

func TestServerKeepsSuccessAfterSelfFence(t *testing.T) {
	t.Parallel()
	store := &silent{Store: memstore.New()}
	c := NewClient(store)
	var runs atomic.Int32
	started, release := make(chan int64, 1), make(chan struct{})
	m := NewMux()
	m.HandleFunc("work", func(_ context.Context, j *RawJob) error {
		if runs.Add(1) == 1 {
			started <- j.ID
			<-release
		}
		return nil
	})
	cfg := fastConfig()
	cfg.DisableMaintenance = true
	s, _ := runServer(t, c, m, cfg)
	id := mustEnqueue(t, c, testArgs{K: "work"})
	receive(t, started)
	store.mute.Store(true)
	eventually(t, time.Second, "missed heartbeat", func() bool { return store.missed.Load() > 0 })
	s.lastBeat.Store(time.Now().Add(-s.fenceAfter()).UnixNano())
	s.fence()
	if !s.Stats().Fenced {
		t.Fatal("server did not fence")
	}
	store.mute.Store(false)
	eventually(t, time.Second, "heartbeat restored", func() bool { return !s.Stats().Fenced })
	close(release)
	r := waitState(t, c, id, Succeeded)
	time.Sleep(4 * cfg.HeartbeatInterval)
	if r = getJob(t, c, id); r.State != Succeeded || r.Attempt != 1 || runs.Load() != 1 {
		t.Fatalf("state %s attempt %d runs %d reasons %v", r.State, r.Attempt, runs.Load(), reasons(r))
	}
}

func TestFenceAfterFreshHeartbeat(t *testing.T) {
	t.Parallel()
	c := NewClient(memstore.New())
	started, causes := make(chan int64, 1), make(chan error, 1)
	m := NewMux()
	m.HandleFunc("block", blocking(started, causes))
	s, _ := runServer(t, c, m, fastConfig())
	mustEnqueue(t, c, testArgs{K: "block"})
	receive(t, started)
	s.fence()
	if s.Stats().Fenced {
		t.Fatal("fenced right after a successful heartbeat")
	}
	select {
	case cause := <-causes:
		t.Fatalf("job canceled: %v", cause)
	case <-time.After(3 * fastConfig().HeartbeatInterval):
	}
}

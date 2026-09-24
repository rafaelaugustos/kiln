package kiln

import (
	"context"
	"fmt"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
	"github.com/rafaelaugustos/kiln/memstore"
)

func TestServerFiresRecurring(t *testing.T) {
	t.Parallel()
	c := NewClient(memstore.New())
	m := NewMux()
	var runs atomic.Int32
	occurrences := make(chan string, 8)
	m.HandleFunc("tick", func(_ context.Context, j *RawJob) error {
		runs.Add(1)
		if j.RecurringID == "ticker" {
			occurrences <- j.Meta[metaOccurrence]
		}
		return nil
	})
	s, _ := runServer(t, c, m, fastConfig())
	if err := c.SetRecurring(context.Background(), "ticker", "@every 1s", testArgs{K: "tick"}); err != nil {
		t.Fatal(err)
	}
	eventually(t, 5*time.Second, "two recurring runs", func() bool { return runs.Load() >= 2 })
	if a, b := receive(t, occurrences), receive(t, occurrences); a == "" || a == b {
		t.Fatalf("occurrences %q %q", a, b)
	}
	if !s.Stats().Leader {
		t.Fatal("server is not leader")
	}
}

func TestServerRescuesOrphans(t *testing.T) {
	t.Parallel()
	store := memstore.New()
	started, causes := make(chan int64, 8), make(chan error, 8)
	m := NewMux()
	m.HandleFunc("work", func(ctx context.Context, j *RawJob) error {
		if j.Attempt > 1 {
			return nil
		}
		return blocking(started, causes)(ctx, j)
	}, Constant(0))

	dead := &faulty{Store: store}
	cfgB := fastConfig()
	cfgB.DisableMaintenance = true
	b, _ := runServer(t, NewClient(dead), m, cfgB)

	c := NewClient(store)
	ids := mustEnqueueMany(t, c, 3, testArgs{K: "work"}, MaxAttempts(3))
	for range ids {
		receive(t, started)
	}
	a, _ := runServer(t, c, m, fastConfig())
	dead.down.Store(true)

	for range ids {
		if cause := receive(t, causes); cause != errFenced {
			t.Fatalf("cause = %v", cause)
		}
	}
	if !b.Stats().Fenced || b.Healthy() == nil {
		t.Fatalf("dead server is not fenced: %+v", b.Stats())
	}
	for _, id := range ids {
		var r Record
		eventually(t, 10*time.Second, "rescue", func() bool {
			r = getJob(t, c, id)
			return r.State == Succeeded
		})
		if !slices.Equal(reasons(r), []string{"orphaned"}) || r.Attempt != 2 || r.History[0].Server != b.ID() {
			t.Fatalf("job %d: reasons %v attempt %d history %+v", id, reasons(r), r.Attempt, r.History)
		}
	}
	if !a.Stats().Leader {
		t.Fatal("survivor is not leader")
	}
}

type dueCounter struct {
	driver.Store
	calls atomic.Int64
}

func (d *dueCounter) Due(ctx context.Context, limit int) ([]driver.Recurring, time.Time, error) {
	d.calls.Add(1)
	return d.Store.Due(ctx, limit)
}

func TestServerSkipsBrokenRecurrings(t *testing.T) {
	t.Parallel()
	store := &dueCounter{Store: memstore.New()}
	ctx := context.Background()
	tmpl := driver.InsertParams{Kind: "tick", Queue: DefaultQueue, Args: []byte(`{}`), MaxAttempts: 1}
	past := time.Now().Add(-time.Hour)
	for i := range dueLimit {
		r := driver.Recurring{ID: fmt.Sprintf("broken-%03d", i), Spec: "* * * * *", Location: "Nowhere/Void", Template: tmpl, NextRunAt: past}
		if err := store.PutRecurring(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	c := NewClient(store)
	fired := make(chan struct{}, 8)
	m := NewMux()
	m.HandleFunc("tick", func(_ context.Context, j *RawJob) error {
		if j.RecurringID == "healthy" {
			fired <- struct{}{}
		}
		return nil
	})
	if err := c.SetRecurring(ctx, "healthy", "@every 1s", testArgs{K: "tick"}); err != nil {
		t.Fatal(err)
	}
	runServer(t, c, m, fastConfig())
	select {
	case <-fired:
	case <-time.After(5 * time.Second):
		t.Fatalf("healthy recurring never fired, Due called %d times", store.calls.Load())
	}
	if n := store.calls.Load(); n > 50 {
		t.Fatalf("Due called %d times", n)
	}
}

type stuckOnce struct {
	driver.Store
	promote, lead atomic.Bool
}

func (s *stuckOnce) Promote(ctx context.Context, limit int) (driver.Promoted, error) {
	if s.promote.CompareAndSwap(false, true) {
		<-ctx.Done()
		return driver.Promoted{}, ctx.Err()
	}
	return s.Store.Promote(ctx, limit)
}

func (s *stuckOnce) Lead(ctx context.Context, name, holder string, ttl time.Duration) (time.Duration, bool, error) {
	if s.lead.CompareAndSwap(false, true) {
		<-ctx.Done()
		return 0, false, ctx.Err()
	}
	return s.Store.Lead(ctx, name, holder, ttl)
}

func TestServerBoundsStoreCalls(t *testing.T) {
	t.Parallel()
	c := NewClient(&stuckOnce{Store: memstore.New()})
	m := NewMux()
	m.HandleFunc("later", func(context.Context, *RawJob) error { return nil })
	s, _ := runServer(t, c, m, fastConfig())
	id := mustEnqueue(t, c, testArgs{K: "later"}, Delay(20*time.Millisecond))
	waitState(t, c, id, Succeeded)
	eventually(t, 5*time.Second, "leadership", func() bool { return s.Stats().Leader })
}

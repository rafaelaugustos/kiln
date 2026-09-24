package memstore_test

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

func TestLimitAdmission(t *testing.T) {
	t.Parallel()
	s, _ := open(t)
	got := ids(t, s,
		params("a", limited("k", 2), priority(0)),
		params("a", limited("k", 2), priority(0)),
		params("a", limited("k", 2), priority(0)),
		params("a", limited("k", 2), priority(7)),
		params("a", limited("k", 2), priority(0)),
	)
	for i, want := range []driver.State{driver.Enqueued, driver.Throttled, driver.Throttled, driver.Enqueued, driver.Throttled} {
		expectState(t, s, got[i], want)
	}

	j := claimOne(t, s)
	if j.ID != got[3] {
		t.Fatalf("claimed %d, want %d", j.ID, got[3])
	}
	finish(t, s, done(j, driver.Succeeded))
	expectState(t, s, got[1], driver.Enqueued)

	if _, err := s.Delete(ctx, driver.Filter{IDs: got[1:2]}); err != nil {
		t.Fatal(err)
	}
	expectState(t, s, got[2], driver.Enqueued)

	j = claimOne(t, s)
	finish(t, s, driver.Outcome{Ref: j.Ref, State: driver.Scheduled, Delay: time.Minute})
	expectState(t, s, got[4], driver.Enqueued)

	j = claimOne(t, s)
	finish(t, s, driver.Outcome{Ref: j.Ref, State: driver.Enqueued, Refund: true})
	expectState(t, s, j.ID, driver.Enqueued)

	insert(t, s, params("a", limited("k", 3)))
	c, _ := s.Counts(ctx)
	if c.Enqueued != 3 || c.Throttled != 0 {
		t.Fatalf("raising max did not admit: %+v", c)
	}
}

func TestLimitConcurrency(t *testing.T) {
	t.Parallel()
	s, _ := open(t)
	const max = 3
	ps := make([]driver.InsertParams, 300)
	for i := range ps {
		ps[i] = params("a", limited("k", max), priority(int16(i%5)))
	}
	insert(t, s, ps...)
	var (
		running atomic.Int32
		total   atomic.Int32
		wg      sync.WaitGroup
	)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for total.Load() < int32(len(ps)) {
				js, err := s.Claim(ctx, driver.ClaimQuery{Queues: []string{"default"}, Limit: 2, Server: "s"})
				if err != nil {
					t.Error(err)
					return
				}
				if n := running.Add(int32(len(js))); n > max {
					t.Errorf("%d jobs running with limit %d", n, max)
				}
				outs := make([]driver.Outcome, len(js))
				for i, j := range js {
					outs[i] = done(j, driver.Succeeded)
				}
				running.Add(-int32(len(js)))
				if _, err := s.Finish(ctx, "s", outs); err != nil {
					t.Error(err)
				}
				total.Add(int32(len(js)))
			}
		}()
	}
	wg.Wait()
	if c, _ := s.Counts(ctx); c.Succeeded != int64(len(ps)) {
		t.Fatalf("succeeded %d of %d", c.Succeeded, len(ps))
	}
}

func TestUnique(t *testing.T) {
	t.Parallel()
	s, c := open(t)
	first := insert(t, s, params("a", unique("k", 0)))[0]
	dup := insert(t, s, params("a", unique("k", 0)), params("b"))
	if !dup[0].Duplicate || dup[0].ID != first.ID || dup[0].State != driver.Enqueued || dup[1].Duplicate {
		t.Fatalf("duplicate of live job = %+v", dup)
	}
	finish(t, s, done(claimOne(t, s), driver.Failed))
	second := insert(t, s, params("a", unique("k", 0)))[0]
	if second.Duplicate {
		t.Fatalf("failed job still holds its key")
	}
	if n, _ := s.Requeue(ctx, driver.Filter{IDs: []int64{first.ID}}); n != 0 {
		t.Fatalf("requeue took a held key")
	}
	if _, err := s.Delete(ctx, driver.Filter{IDs: []int64{second.ID}}); err != nil {
		t.Fatal(err)
	}
	if n, _ := s.Requeue(ctx, driver.Filter{IDs: []int64{first.ID}}); n != 1 {
		t.Fatalf("requeue after release = %d", n)
	}
	if again := insert(t, s, params("a", unique("k", 0)))[0]; !again.Duplicate || again.ID != first.ID {
		t.Fatalf("requeued job does not hold its key: %+v", again)
	}

	w := insert(t, s, params("w", queue("w"), unique("w", time.Hour)))[0]
	finish(t, s, done(claim(t, s, 1, "w")[0], driver.Succeeded))
	if r := insert(t, s, params("w", queue("w"), unique("w", time.Hour)))[0]; !r.Duplicate || r.ID != w.ID || r.State != driver.Succeeded {
		t.Fatalf("window released early: %+v", r)
	}
	c.add(time.Hour)
	if r := insert(t, s, params("w", queue("w"), unique("w", time.Hour)))[0]; r.Duplicate {
		t.Fatalf("window held after expiry")
	}
}

package drivertest

import (
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

var limitsTests = []test{
	{"Admission", testLimitAdmission},
	{"Order", testLimitOrder},
	{"Release", testLimitRelease},
	{"Max", testLimitMax},
	{"Promote", testLimitPromote},
	{"Requeue", testLimitRequeue},
	{"Shutdown", testLimitShutdown},
	{"DeleteWaiting", testLimitDeleteWaiting},
	{"Pruned", testLimitPruned},
	{"Archived", testLimitArchived},
	{"Rows", testLimitRows},
	{"Concurrent", testLimitConcurrent},
}

func testLimitAdmission(t *testing.T, s driver.Store) {
	ps := make([]driver.InsertParams, 5)
	for i := range ps {
		ps[i] = limited("l", "k", 2)
	}
	ids := insertedIDs(insert(t, s, ps...))
	wantState(t, s, driver.Enqueued, ids[:2]...)
	wantState(t, s, driver.Throttled, ids[2:]...)
	other := add(t, s, limited("l", "k2", 1))
	wantState(t, s, driver.Enqueued, other)

	if got, want := sorted(jobIDs(claim(t, s, 10, "l"))), []int64{ids[0], ids[1], other}; !slices.Equal(got, want) {
		t.Fatalf("claimed %v, want %v", got, want)
	}
	if js := claim(t, s, 10, "l"); len(js) != 0 {
		t.Fatalf("claimed throttled jobs %v", jobIDs(js))
	}
	if c := counts(t, s); c.Throttled != 3 || c.Processing != 3 {
		t.Fatalf("throttled %d processing %d, want 3 and 3", c.Throttled, c.Processing)
	}
}

func testLimitOrder(t *testing.T, s driver.Store) {
	cur := start(t, s, limited("o", "k", 1))
	prios := []int16{0, 0, 5, 0, 5}
	ids := make([]int64, len(prios))
	for i, p := range prios {
		lp := limited("o", "k", 1)
		lp.Priority = p
		ids[i] = add(t, s, lp)
	}
	wantState(t, s, driver.Throttled, ids...)
	for _, i := range []int{2, 4, 0, 1, 3} {
		apply(t, s, outcome(cur, driver.Succeeded))
		cur = claimOne(t, s, "o", ids[i])
		if c := counts(t, s); c.Enqueued != 0 {
			t.Fatalf("%d enqueued with limit 1 while job %d runs", c.Enqueued, cur.ID)
		}
	}
}

func testLimitRelease(t *testing.T, s driver.Store) {
	finishAs := func(out driver.Outcome) func(q string, id int64) {
		return func(q string, id int64) {
			out.Ref = claimOne(t, s, q, id).Ref
			apply(t, s, out)
		}
	}
	exits := []struct {
		name string
		exit func(q string, id int64)
	}{
		{"succeeded", finishAs(driver.Outcome{State: driver.Succeeded})},
		{"failed", finishAs(driver.Outcome{State: driver.Failed, Reason: "exhausted"})},
		{"deleted", finishAs(driver.Outcome{State: driver.Deleted, Reason: "canceled"})},
		{"retry", finishAs(driver.Outcome{State: driver.Scheduled, Delay: time.Hour, Reason: "retry"})},
		{"snooze", finishAs(driver.Outcome{State: driver.Scheduled, Delay: time.Hour, Refund: true, Reason: "snoozed"})},
		{"delete enqueued", func(q string, id int64) { deleteIDs(t, s, id) }},
		{"cancel", func(q string, id int64) {
			j := claimOne(t, s, q, id)
			deleteIDs(t, s, id)
			wantState(t, s, driver.Processing, id)
			apply(t, s, outcome(j, driver.Failed))
		}},
		{"orphan", func(q string, id int64) {
			claimAs(t, s, "gone", 1, q)
			os, err := s.Orphans(t.Context(), time.Hour, 100)
			if err != nil {
				t.Fatal(err)
			}
			for _, o := range os {
				if o.ID == id {
					apply(t, s, driver.Outcome{Ref: o.Ref, State: driver.Scheduled, Delay: time.Hour, Reason: "orphaned"})
					return
				}
			}
			t.Fatalf("job %d not reported as orphan", id)
		}},
	}
	for i, e := range exits {
		q := fmt.Sprintf("q%d", i)
		a := add(t, s, limited(q, q, 1))
		b := add(t, s, limited(q, q, 1))
		wantState(t, s, driver.Throttled, b)
		e.exit(q, a)
		if st := stateOf(t, s, b); st != driver.Enqueued {
			t.Fatalf("%s: waiting job %s, want enqueued", e.name, st)
		}
	}
}

func testLimitMax(t *testing.T, s driver.Store) {
	a := add(t, s, limited("m", "k", 1))
	b := add(t, s, limited("m", "k", 1))
	wantState(t, s, driver.Throttled, b)
	c := add(t, s, limited("m", "k", 3))
	wantState(t, s, driver.Enqueued, a, b, c)
	d := add(t, s, limited("m", "k", 1))
	wantState(t, s, driver.Throttled, d)

	js := claimN(t, s, 3, "m")
	for _, j := range js {
		wantState(t, s, driver.Throttled, d)
		apply(t, s, outcome(j, driver.Succeeded))
	}
	wantState(t, s, driver.Enqueued, d)
}

func testLimitPromote(t *testing.T, s driver.Store) {
	ps := []driver.InsertParams{limited("p", "k", 1), limited("p", "k", 1)}
	ps[0].Delay, ps[1].Delay = 50*time.Millisecond, 50*time.Millisecond
	ids := insertedIDs(insert(t, s, ps...))
	wantState(t, s, driver.Scheduled, ids...)
	eventually(t, time.Second, func() bool {
		promote(t, s, 100)
		return stateOf(t, s, ids[0]) != driver.Scheduled && stateOf(t, s, ids[1]) != driver.Scheduled
	})
	wantState(t, s, driver.Enqueued, ids[0])
	wantState(t, s, driver.Throttled, ids[1])
}

func testLimitRequeue(t *testing.T, s driver.Store) {
	a := start(t, s, limited("r", "k", 1))
	apply(t, s, outcome(a, driver.Failed))
	b := add(t, s, limited("r", "k", 1))
	if n := requeueIDs(t, s, a.ID); n != 1 {
		t.Fatalf("requeued %d, want 1", n)
	}
	sweep(t, s)
	wantState(t, s, driver.Enqueued, b)
	wantState(t, s, driver.Throttled, a.ID)
	apply(t, s, outcome(claimOne(t, s, "r", b), driver.Succeeded))
	wantState(t, s, driver.Enqueued, a.ID)
}

func testLimitShutdown(t *testing.T, s driver.Store) {
	for i, reason := range []string{"shutdown", "lost"} {
		q := fmt.Sprintf("q%d", i)
		a := start(t, s, limited(q, q, 1))
		b := add(t, s, limited(q, q, 1))
		out := outcome(a, driver.Enqueued)
		out.Refund, out.Reason = reason == "shutdown", reason
		apply(t, s, out)
		wantState(t, s, driver.Enqueued, a.ID)
		wantState(t, s, driver.Throttled, b)
		apply(t, s, outcome(claimOne(t, s, q, a.ID), driver.Succeeded))
		wantState(t, s, driver.Enqueued, b)
	}
}

func testLimitDeleteWaiting(t *testing.T, s driver.Store) {
	a := add(t, s, limited("w", "k", 1))
	b := add(t, s, limited("w", "k", 1))
	c := add(t, s, limited("w", "k", 1))
	p := limited("w", "k", 1)
	p.Delay = time.Hour
	d := add(t, s, p)
	if n := deleteIDs(t, s, b, d); n != 2 {
		t.Fatalf("deleted %d, want 2", n)
	}
	wantState(t, s, driver.Enqueued, a)
	wantState(t, s, driver.Throttled, c)
	apply(t, s, outcome(claimOne(t, s, "w", a), driver.Succeeded))
	wantState(t, s, driver.Enqueued, c)
	wantState(t, s, driver.Throttled, add(t, s, limited("w", "k", 1)))
}

func testLimitPruned(t *testing.T, s driver.Store) {
	var failed []int64
	for range 2 {
		j := start(t, s, limited("f", "f", 1))
		apply(t, s, outcome(j, driver.Failed))
		failed = append(failed, j.ID)
	}
	ps := []driver.InsertParams{limited("s", "s", 1), limited("s", "s", 1)}
	for i := range ps {
		ps[i].Delay = 50 * time.Millisecond
	}
	scheduled := insertedIDs(insert(t, s, ps...))
	parent := add(t, s, task("parents"))
	ps = []driver.InsertParams{limited("a", "a", 1), limited("a", "a", 1)}
	for i := range ps {
		ps[i].Parents = []driver.Parent{{ID: parent, On: driver.OnSucceeded}}
	}
	awaiting := insertedIDs(insert(t, s, ps...))
	for _, k := range []string{"f", "s", "a"} {
		apply(t, s, outcome(start(t, s, limited(k, k, 2)), driver.Succeeded))
	}
	time.Sleep(20 * time.Millisecond)
	pp := keep()
	pp.Succeeded = 5 * time.Millisecond
	for range 3 {
		prune(t, s, pp)
	}

	if n := requeueIDs(t, s, failed...); n != 2 {
		t.Fatalf("requeued %d, want 2", n)
	}
	apply(t, s, outcome(claimOne(t, s, "parents", parent), driver.Succeeded))
	eventually(t, time.Second, func() bool {
		promote(t, s, 100)
		return stateOf(t, s, scheduled[0]) != driver.Scheduled && stateOf(t, s, scheduled[1]) != driver.Scheduled
	})
	for _, ids := range [][]int64{failed, scheduled, awaiting} {
		wantState(t, s, driver.Enqueued, ids...)
	}
	for _, k := range []string{"f", "s", "a"} {
		wantState(t, s, driver.Throttled, add(t, s, limited(k, k, 2)))
	}
}

func testLimitArchived(t *testing.T, s driver.Store) {
	var ids []int64
	for range 2 {
		j := start(t, s, limited("d", "d", 1))
		apply(t, s, outcome(j, driver.Succeeded))
		ids = append(ids, j.ID)
	}
	apply(t, s, outcome(start(t, s, limited("d", "d", 2)), driver.Deleted))
	for range 3 {
		prune(t, s, keep())
	}
	if n := requeueIDs(t, s, ids...); n != 2 {
		t.Fatalf("requeued %d, want 2", n)
	}
	wantState(t, s, driver.Enqueued, ids...)
	wantState(t, s, driver.Throttled, add(t, s, limited("d", "d", 2)))
}

func testLimitRows(t *testing.T, s driver.Store) {
	apply(t, s, outcome(start(t, s, limited("q", "k", 3)), driver.Succeeded))
	time.Sleep(20 * time.Millisecond)
	pp := keep()
	pp.Succeeded = 5 * time.Millisecond
	n := 0
	for range 3 {
		n += prune(t, s, pp)
	}
	if n != 2 {
		t.Fatalf("pruned %d rows, want the job and its limit", n)
	}
	a := add(t, s, limited("q", "k", 1))
	b := add(t, s, limited("q", "k", 1))
	wantState(t, s, driver.Enqueued, a)
	wantState(t, s, driver.Throttled, b)
}

func testLimitConcurrent(t *testing.T, s driver.Store) {
	const n, limit, workers = 80, 3, 8
	ps := make([]driver.InsertParams, n)
	for i := range ps {
		ps[i] = limited("c", "k", limit)
		ps[i].MaxAttempts = 10
	}
	insert(t, s, ps...)

	settle := func(j driver.Job) (driver.Outcome, bool) {
		switch j.ID % 4 {
		case 0:
			return outcome(j, driver.Succeeded), true
		case 1:
			return outcome(j, driver.Failed), true
		case 2:
			return outcome(j, driver.Deleted), true
		}
		if j.Attempt == 1 {
			return driver.Outcome{Ref: j.Ref, State: driver.Scheduled, Reason: "retry"}, false
		}
		return outcome(j, driver.Succeeded), true
	}
	ctx := t.Context()
	deadline := time.Now().Add(5 * time.Second)
	var (
		running, done atomic.Int64
		wg, watch     sync.WaitGroup
	)
	stop := make(chan struct{})
	watch.Go(func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(time.Millisecond):
			}
			c, err := s.Counts(ctx)
			if err != nil {
				t.Error(err)
				return
			}
			if active := c.Enqueued + c.Processing; active > limit {
				t.Errorf("store has %d jobs enqueued or processing with limit %d", active, limit)
				return
			}
		}
	})
	for range workers {
		wg.Go(func() {
			q := driver.ClaimQuery{Queues: []string{"c"}, Limit: 2, Server: server}
			for done.Load() < n && time.Now().Before(deadline) {
				js, err := s.Claim(ctx, q)
				if err != nil {
					t.Error(err)
					return
				}
				if len(js) == 0 {
					time.Sleep(time.Millisecond)
					continue
				}
				if r := running.Add(int64(len(js))); r > limit {
					t.Errorf("%d jobs running with limit %d", r, limit)
				}
				time.Sleep(time.Millisecond)
				outs := make([]driver.Outcome, len(js))
				final := 0
				for i, j := range js {
					out, last := settle(j)
					outs[i] = out
					if last {
						final++
					}
				}
				running.Add(-int64(len(js)))
				for len(outs) > 0 && time.Now().Before(deadline) {
					rs, err := s.Finish(ctx, server, outs)
					if err != nil || len(rs) != len(outs) {
						t.Errorf("finish: %v %v", rs, err)
						return
					}
					var busy []driver.Outcome
					for i, r := range rs {
						switch r {
						case driver.Busy:
							busy = append(busy, outs[i])
						case driver.Applied:
						default:
							t.Errorf("finish job %d: %d", outs[i].ID, r)
						}
					}
					outs = busy
				}
				done.Add(int64(final))
			}
		})
	}
	wg.Wait()
	close(stop)
	watch.Wait()
	if t.Failed() {
		return
	}
	if done.Load() != n {
		t.Fatalf("finished %d of %d jobs before the deadline", done.Load(), n)
	}
	c := counts(t, s)
	if c.Throttled != 0 || c.Enqueued != 0 || c.Processing != 0 || c.Scheduled != 0 {
		t.Fatalf("counts %+v, want every job finished", c)
	}
}

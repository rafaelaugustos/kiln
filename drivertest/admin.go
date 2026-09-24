package drivertest

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

var adminTests = []test{
	{"Filter", testAdminFilter},
	{"FilterFields", testAdminFilterFields},
	{"Delete", testDelete},
	{"DeleteProcessing", testDeleteProcessing},
	{"DeleteByState", testDeleteByState},
	{"DeleteBulk", testDeleteBulk},
	{"Requeue", testRequeue},
	{"RequeueSkips", testRequeueSkips},
	{"RequeueAttempts", testRequeueAttempts},
	{"RequeueByState", testRequeueByState},
}

func testAdminFilter(t *testing.T, s driver.Store) {
	add(t, s, task("q"))
	apply(t, s, outcome(start(t, s, task("f")), driver.Failed))
	for _, f := range []driver.Filter{{}, {Queue: "q"}, {Kind: "task"}, {BatchID: 1}, {RecurringID: "r"}} {
		_, err := s.Delete(t.Context(), f)
		wantErr(t, err, driver.ErrInvalid)
		_, err = s.Requeue(t.Context(), f)
		wantErr(t, err, driver.ErrInvalid)
	}
	if c := counts(t, s); c.Enqueued != 1 || c.Failed != 1 {
		t.Fatalf("counts %+v after rejected filters, want 1 enqueued and 1 failed", c)
	}
	if n := deleteIDs(t, s, 1<<40); n != 0 {
		t.Fatalf("deleted %d unknown jobs", n)
	}
	if n := requeueIDs(t, s, 1<<40); n != 0 {
		t.Fatalf("requeued %d unknown jobs", n)
	}
}

func testAdminFilterFields(t *testing.T, s driver.Store) {
	ctx := t.Context()
	bid := openBatch(t, s)
	job := func(batch int64, recurring string) driver.InsertParams {
		p := task("q")
		p.BatchID, p.RecurringID = batch, recurring
		return p
	}
	ids := insertedIDs(insert(t, s, job(bid, ""), job(0, "r"), job(bid, "r"), job(0, "")))
	steps := []struct {
		op   func(context.Context, driver.Filter) (int, error)
		f    driver.Filter
		want []int64
	}{
		{s.Delete, driver.Filter{State: driver.Enqueued, BatchID: bid, RecurringID: "r"}, ids[2:3]},
		{s.Delete, driver.Filter{State: driver.Enqueued, RecurringID: "r"}, ids[1:2]},
		{s.Delete, driver.Filter{State: driver.Enqueued, BatchID: bid}, ids[:1]},
		{s.Requeue, driver.Filter{State: driver.Deleted, RecurringID: "r"}, ids[1:3]},
		{s.Requeue, driver.Filter{State: driver.Deleted, BatchID: bid}, ids[:1]},
	}
	for i, st := range steps {
		before := list(t, s, driver.JobQuery{State: st.f.State, Limit: 500})
		n, err := st.op(ctx, st.f)
		if err != nil {
			t.Fatalf("step %d: %v", i, err)
		}
		after := list(t, s, driver.JobQuery{State: st.f.State, Limit: 500})
		gone := slices.DeleteFunc(before, func(id int64) bool { return slices.Contains(after, id) })
		if got := sorted(gone); n != len(st.want) || !slices.Equal(got, st.want) {
			t.Fatalf("step %d %+v: affected %d, moved %v, want %v", i, st.f, n, got, st.want)
		}
	}
	wantState(t, s, driver.Enqueued, ids...)
}

func testDelete(t *testing.T, s driver.Store) {
	finished := func(q string, st driver.State) int64 {
		j := start(t, s, task(q))
		apply(t, s, outcome(j, st))
		return j.ID
	}
	parent := add(t, s, task("p"))
	enqueued := add(t, s, task("q"))
	p := task("q")
	p.Delay = time.Hour
	scheduled := add(t, s, p)
	awaiting := add(t, s, after("q", driver.OnSucceeded, parent))
	add(t, s, limited("other", "k", 1))
	throttled := add(t, s, limited("q", "k", 1))
	failed := finished("f", driver.Failed)
	succeeded := finished("s", driver.Succeeded)
	deleted := finished("d", driver.Deleted)
	live := []int64{enqueued, scheduled, awaiting, throttled, failed}
	history := len(record(t, s, deleted).History)

	n0 := now(t, s)
	n := deleteIDs(t, s, append(live, succeeded, deleted)...)
	n1 := now(t, s)
	if n != len(live) {
		t.Fatalf("deleted %d, want %d", n, len(live))
	}
	for _, id := range live {
		r := record(t, s, id)
		if r.State != driver.Deleted {
			t.Fatalf("job %d state %s, want deleted", id, r.State)
		}
		within(t, "finalized at", r.FinalizedAt, n0, n1)
		if e := lastEntry(t, r); e.Reason != "deleted" {
			t.Fatalf("job %d last reason %q, want deleted", id, e.Reason)
		}
	}
	if r := record(t, s, succeeded); r.State != driver.Succeeded || len(r.History) != 0 {
		t.Fatalf("succeeded job touched: %s %+v", r.State, r.History)
	}
	if r := record(t, s, deleted); r.State != driver.Deleted || len(r.History) != history {
		t.Fatalf("deleted job touched: %+v", r.History)
	}
	if n := deleteIDs(t, s, live...); n != 0 {
		t.Fatalf("second delete affected %d", n)
	}
	wantState(t, s, driver.Enqueued, parent)
}

func testDeleteProcessing(t *testing.T, s driver.Store) {
	j := start(t, s, task("q"))
	if n := deleteIDs(t, s, j.ID); n != 1 {
		t.Fatalf("deleted %d, want 1", n)
	}
	r := record(t, s, j.ID)
	if r.State != driver.Processing || !r.CancelRequested || r.Claim != j.Claim {
		t.Fatalf("state %s cancel %v claim %d, want processing with cancel requested", r.State, r.CancelRequested, r.Claim)
	}
	if n := deleteIDs(t, s, j.ID); n != 0 {
		t.Fatalf("second delete of a canceling job affected %d, want 0", n)
	}
	if n, err := s.Delete(t.Context(), driver.Filter{State: driver.Processing}); err != nil || n != 0 {
		t.Fatalf("delete of processing jobs affected %d, %v, want 0", n, err)
	}
	apply(t, s, outcome(j, driver.Succeeded))
	wantState(t, s, driver.Succeeded, j.ID)
}

func testDeleteByState(t *testing.T, s driver.Store) {
	ctx := t.Context()
	scheduled := func(q, kind string) driver.InsertParams {
		p := task(q)
		p.Kind, p.Delay = kind, time.Hour
		return p
	}
	insert(t, s, scheduled("a", "x"), scheduled("a", "x"), scheduled("a", "y"), scheduled("b", "x"), scheduled("b", "y"))
	enqueued := insertedIDs(insert(t, s, tasks(2, "a")...))

	del := func(f driver.Filter, want int) {
		t.Helper()
		n, err := s.Delete(ctx, f)
		if err != nil {
			t.Fatal(err)
		}
		if n != want {
			t.Fatalf("delete %+v affected %d, want %d", f, n, want)
		}
	}
	del(driver.Filter{State: driver.Scheduled, Queue: "b", Kind: "y"}, 1)
	del(driver.Filter{State: driver.Scheduled, Queue: "b"}, 1)
	del(driver.Filter{State: driver.Scheduled, Kind: "y"}, 1)
	del(driver.Filter{State: driver.Enqueued, Kind: "other"}, 0)
	del(driver.Filter{State: driver.Scheduled}, 2)
	if c := counts(t, s); c.Scheduled != 0 || c.Enqueued != 2 {
		t.Fatalf("counts %+v", c)
	}
	wantState(t, s, driver.Enqueued, enqueued...)
	del(driver.Filter{IDs: enqueued, State: driver.Scheduled}, 0)
	wantState(t, s, driver.Enqueued, enqueued...)
}

func testDeleteBulk(t *testing.T, s driver.Store) {
	const n = 2500
	for range n / 500 {
		insert(t, s, tasks(500, "q")...)
	}
	got, err := s.Delete(t.Context(), driver.Filter{State: driver.Enqueued})
	if err != nil {
		t.Fatal(err)
	}
	if got != n {
		t.Fatalf("deleted %d, want %d", got, n)
	}
	if c := counts(t, s); c.Enqueued != 0 {
		t.Fatalf("enqueued %d after bulk delete", c.Enqueued)
	}
}

func testRequeue(t *testing.T, s driver.Store) {
	finished := func(q string, st driver.State) int64 {
		j := start(t, s, task(q))
		apply(t, s, outcome(j, st))
		return j.ID
	}
	failed := finished("q", driver.Failed)
	succeeded := finished("q", driver.Succeeded)
	deleted := finished("q", driver.Deleted)
	p := task("q")
	p.Delay = time.Hour
	scheduled := add(t, s, p)

	ids := []int64{failed, succeeded, deleted, scheduled}
	if n := requeueIDs(t, s, ids...); n != len(ids) {
		t.Fatalf("requeued %d, want %d", n, len(ids))
	}
	attempts := map[int64]int{failed: 1, succeeded: 1, deleted: 1, scheduled: 0}
	for _, id := range ids {
		r := record(t, s, id)
		if r.State != driver.Enqueued || r.Attempt != attempts[id] {
			t.Fatalf("job %d: state %s attempt %d, want enqueued with attempt %d", id, r.State, r.Attempt, attempts[id])
		}
		if e := lastEntry(t, r); e.Reason != "requeued" {
			t.Fatalf("job %d last reason %q, want requeued", id, e.Reason)
		}
	}
	js := claim(t, s, 10, "q")
	if len(js) != len(ids) {
		t.Fatalf("claimed %d, want %d", len(js), len(ids))
	}
	for _, j := range js {
		if j.Attempt != attempts[j.ID]+1 {
			t.Fatalf("job %d attempt %d, want %d", j.ID, j.Attempt, attempts[j.ID]+1)
		}
	}
	if c := counts(t, s); c.Succeeded != 1 || c.Deleted != 1 || c.Processing != 4 {
		t.Fatalf("counts %+v", c)
	}
}

func testRequeueSkips(t *testing.T, s driver.Store) {
	parent := add(t, s, task("p"))
	awaiting := add(t, s, after("q", driver.OnSucceeded, parent))
	running := start(t, s, task("r"))
	add(t, s, limited("l", "k", 1))
	throttled := add(t, s, limited("l", "k", 1))
	skipped := []int64{parent, awaiting, running.ID, throttled}
	if n := requeueIDs(t, s, append(skipped, 1<<40)...); n != 0 {
		t.Fatalf("requeued %d, want 0", n)
	}
	for _, st := range []driver.State{driver.Awaiting, driver.Throttled, driver.Enqueued, driver.Processing} {
		n, err := s.Requeue(t.Context(), driver.Filter{State: st})
		if err != nil || n != 0 {
			t.Fatalf("requeue %s jobs: %d %v, want 0", st, n, err)
		}
	}
	wantState(t, s, driver.Enqueued, parent)
	wantState(t, s, driver.Awaiting, awaiting)
	wantState(t, s, driver.Processing, running.ID)
	wantState(t, s, driver.Throttled, throttled)
	for _, id := range skipped {
		if h := record(t, s, id).History; len(h) != 0 {
			t.Fatalf("job %d history %+v after skipped requeue", id, h)
		}
	}
	apply(t, s, outcome(running, driver.Succeeded))
}

func testRequeueAttempts(t *testing.T, s driver.Store) {
	p := task("q")
	p.MaxAttempts = 1
	j := start(t, s, p)
	apply(t, s, outcome(j, driver.Failed))
	requeueIDs(t, s, j.ID)
	if r := record(t, s, j.ID); r.MaxAttempts != 2 || r.Attempt != 1 {
		t.Fatalf("max attempts %d attempt %d, want 2 and 1", r.MaxAttempts, r.Attempt)
	}
	j = claimOne(t, s, "q", j.ID)
	if j.Attempt != 2 || j.MaxAttempts != 2 {
		t.Fatalf("attempt %d of %d, want 2 of 2", j.Attempt, j.MaxAttempts)
	}

	p = task("r")
	p.MaxAttempts = 5
	k := start(t, s, p)
	apply(t, s, outcome(k, driver.Failed))
	requeueIDs(t, s, k.ID)
	if r := record(t, s, k.ID); r.MaxAttempts != 5 {
		t.Fatalf("max attempts %d, want 5", r.MaxAttempts)
	}

	c := start(t, s, task("c"))
	deleteIDs(t, s, c.ID)
	apply(t, s, outcome(c, driver.Failed))
	wantState(t, s, driver.Deleted, c.ID)
	requeueIDs(t, s, c.ID)
	if r := record(t, s, c.ID); r.State != driver.Enqueued || r.CancelRequested {
		t.Fatalf("state %s cancel %v, want enqueued without cancel", r.State, r.CancelRequested)
	}
	c = claimOne(t, s, "c", c.ID)
	apply(t, s, outcome(c, driver.Failed))
	wantState(t, s, driver.Failed, c.ID)
}

func testRequeueByState(t *testing.T, s driver.Store) {
	for _, q := range []string{"a", "a", "b"} {
		apply(t, s, outcome(start(t, s, task(q)), driver.Failed))
	}
	n, err := s.Requeue(t.Context(), driver.Filter{State: driver.Failed, Queue: "a"})
	if err != nil || n != 2 {
		t.Fatalf("requeued %d, %v, want 2", n, err)
	}
	if c := counts(t, s); c.Failed != 1 || c.Enqueued != 2 {
		t.Fatalf("counts %+v", c)
	}
	n, err = s.Requeue(t.Context(), driver.Filter{State: driver.Failed})
	if err != nil || n != 1 {
		t.Fatalf("requeued %d, %v, want 1", n, err)
	}
	if c := counts(t, s); c.Failed != 0 || c.Enqueued != 3 {
		t.Fatalf("counts %+v", c)
	}
}

package drivertest

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

var batchTests = []test{
	{"Open", testBatchOpen},
	{"SealAfter", testBatchSealAfter},
	{"SealBefore", testBatchSealBefore},
	{"SealEmpty", testBatchSealEmpty},
	{"FailedMember", testBatchFailedMember},
	{"Attach", testBatchAttach},
	{"Counts", testBatchCounts},
	{"NotFound", testBatchNotFound},
	{"List", testBatchList},
	{"Nested", testBatchNested},
	{"NestedChain", testBatchNestedChain},
	{"NestedOpen", testBatchNestedOpen},
	{"NestedFailed", testBatchNestedFailed},
	{"NestedEmpty", testBatchNestedEmpty},
	{"NestedList", testBatchNestedList},
	{"NestedConcurrent", testBatchNestedConcurrent},
	{"NestedPrune", testBatchNestedPrune},
}

func openBatch(t *testing.T, w driver.Writer) int64 {
	t.Helper()
	id, err := w.OpenBatch(t.Context(), driver.NewBatch{Description: "import"})
	if err != nil {
		t.Fatalf("open batch: %v", err)
	}
	return id
}

func nest(t *testing.T, w driver.Writer, parent int64) int64 {
	t.Helper()
	id, err := w.OpenBatch(t.Context(), driver.NewBatch{Description: "part", Parent: parent})
	if err != nil {
		t.Fatalf("open batch in %d: %v", parent, err)
	}
	return id
}

func seal(t *testing.T, w driver.Writer, id int64) {
	t.Helper()
	if err := w.SealBatch(t.Context(), id); err != nil {
		t.Fatalf("seal batch %d: %v", id, err)
	}
}

func batch(t *testing.T, s driver.Store, id int64) driver.Batch {
	t.Helper()
	b, err := s.Batch(t.Context(), id)
	if err != nil {
		t.Fatalf("batch %d: %v", id, err)
	}
	return b
}

func members(queue string, batch int64, n int) []driver.InsertParams {
	ps := tasks(n, queue)
	for i := range ps {
		ps[i].BatchID = batch
	}
	return ps
}

func afterBatch(queue string, batch int64) driver.InsertParams {
	p := task(queue)
	p.AfterBatch = batch
	return p
}

func wantFinished(t *testing.T, s driver.Store, id int64, finished bool) driver.Batch {
	t.Helper()
	b := batch(t, s, id)
	if b.FinishedAt.IsZero() == finished {
		t.Fatalf("batch %d finished at %v, want finished %v", id, b.FinishedAt, finished)
	}
	return b
}

func testBatchOpen(t *testing.T, s driver.Store) {
	meta := map[string]string{"source": "s3", "file": "a.csv"}
	n0 := now(t, s)
	id, err := s.OpenBatch(t.Context(), driver.NewBatch{Description: "import", Meta: meta})
	if err != nil {
		t.Fatal(err)
	}
	n1 := now(t, s)
	if id <= 0 {
		t.Fatalf("batch id %d, want > 0", id)
	}
	b := batch(t, s, id)
	switch {
	case b.ID != id || b.Description != "import" || !maps.Equal(b.Meta, meta):
		t.Fatalf("batch %+v", b)
	case b.Total != 0 || b.Sealed || !b.FinishedAt.IsZero():
		t.Fatalf("new batch total %d sealed %v finished %v", b.Total, b.Sealed, b.FinishedAt)
	}
	within(t, "created at", b.CreatedAt, n0, n1)
	if other := openBatch(t, s); other == id {
		t.Fatalf("second batch reused id %d", id)
	}

	ids := insertedIDs(insert(t, s, members("m", id, 3)...))
	if b := batch(t, s, id); b.Total != 3 || b.Counts[driver.Enqueued] != 3 {
		t.Fatalf("total %d counts %v, want 3 enqueued", b.Total, b.Counts)
	}
	for _, mid := range ids {
		if r := record(t, s, mid); r.BatchID != id {
			t.Fatalf("job %d batch %d, want %d", mid, r.BatchID, id)
		}
	}
}

func testBatchSealAfter(t *testing.T, s driver.Store) {
	id := openBatch(t, s)
	insert(t, s, members("m", id, 2)...)
	next := add(t, s, afterBatch("next", id))
	if r := record(t, s, next); r.State != driver.Awaiting || r.AfterBatch != id || r.PendingDeps != 1 {
		t.Fatalf("dependent: state %s after batch %d pending %d", r.State, r.AfterBatch, r.PendingDeps)
	}
	js := claimN(t, s, 2, "m")
	apply(t, s, outcome(js[0], driver.Succeeded), outcome(js[1], driver.Succeeded))
	wantFinished(t, s, id, false)
	sweep(t, s)
	wantFinished(t, s, id, false)
	wantState(t, s, driver.Awaiting, next)

	n0 := now(t, s)
	seal(t, s, id)
	n1 := now(t, s)
	b := wantFinished(t, s, id, true)
	if !b.Sealed || b.Total != 2 || b.Counts[driver.Succeeded] != 2 {
		t.Fatalf("batch %+v", b)
	}
	within(t, "finished at", b.FinishedAt, n0, n1)
	wantState(t, s, driver.Enqueued, next)
}

func testBatchSealBefore(t *testing.T, s driver.Store) {
	id := openBatch(t, s)
	insert(t, s, members("m", id, 2)...)
	next := add(t, s, afterBatch("next", id))
	seal(t, s, id)
	if b := wantFinished(t, s, id, false); !b.Sealed {
		t.Fatal("batch not sealed")
	}
	js := claimN(t, s, 2, "m")
	apply(t, s, outcome(js[0], driver.Succeeded))
	wantFinished(t, s, id, false)
	wantState(t, s, driver.Awaiting, next)
	apply(t, s, outcome(js[1], driver.Deleted))
	finished := wantFinished(t, s, id, true).FinishedAt
	wantState(t, s, driver.Enqueued, next)

	seal(t, s, id)
	if b := batch(t, s, id); !b.FinishedAt.Equal(finished) {
		t.Fatalf("finished at moved from %v to %v on second seal", finished, b.FinishedAt)
	}
}

func testBatchSealEmpty(t *testing.T, s driver.Store) {
	id := openBatch(t, s)
	early := add(t, s, afterBatch("next", id))
	seal(t, s, id)
	wantFinished(t, s, id, true)
	wantState(t, s, driver.Enqueued, early)
	if in := insert(t, s, afterBatch("next", id))[0]; in.State != driver.Enqueued {
		t.Fatalf("dependent of finished batch inserted as %s, want enqueued", in.State)
	}
	seal(t, s, id)
}

func testBatchFailedMember(t *testing.T, s driver.Store) {
	id := openBatch(t, s)
	m := add(t, s, members("m", id, 1)[0])
	next := add(t, s, afterBatch("next", id))
	seal(t, s, id)
	apply(t, s, outcome(claimOne(t, s, "m", m), driver.Failed))
	sweep(t, s)
	wantFinished(t, s, id, false)
	wantState(t, s, driver.Awaiting, next)
	if n := requeueIDs(t, s, m); n != 1 {
		t.Fatalf("requeued %d, want 1", n)
	}
	apply(t, s, outcome(claimOne(t, s, "m", m), driver.Succeeded))
	wantFinished(t, s, id, true)
	wantState(t, s, driver.Enqueued, next)
}

func testBatchAttach(t *testing.T, s driver.Store) {
	id := openBatch(t, s)
	first := add(t, s, members("m", id, 1)[0])
	seal(t, s, id)
	second := add(t, s, members("m", id, 1)[0])
	if b := batch(t, s, id); b.Total != 2 {
		t.Fatalf("total %d, want 2", b.Total)
	}
	apply(t, s, outcome(claimOne(t, s, "m", first), driver.Failed))
	third := add(t, s, members("m", id, 1)[0])
	apply(t, s, outcome(claimOne(t, s, "m", second), driver.Succeeded))
	apply(t, s, outcome(claimOne(t, s, "m", third), driver.Succeeded))
	wantFinished(t, s, id, false)
	deleteIDs(t, s, first)
	wantFinished(t, s, id, true)

	before := counts(t, s)
	_, err := s.Insert(t.Context(), append([]driver.InsertParams{task("other")}, members("m", id, 1)...))
	wantErr(t, err, driver.ErrClosed)
	if c := counts(t, s); c != before {
		t.Fatalf("counts %+v after rejected insert, want %+v", c, before)
	}
	if b := batch(t, s, id); b.Total != 3 {
		t.Fatalf("total %d, want 3", b.Total)
	}

	_, err = s.Insert(t.Context(), members("m", id+1000, 1))
	wantErr(t, err, driver.ErrNotFound)
}

func testBatchCounts(t *testing.T, s driver.Store) {
	id := openBatch(t, s)
	insert(t, s, members("m", id, 5)...)
	js := claimN(t, s, 4, "m")
	apply(t, s, outcome(js[0], driver.Succeeded), outcome(js[1], driver.Failed), outcome(js[2], driver.Deleted))
	b := batch(t, s, id)
	want := map[driver.State]int64{driver.Succeeded: 1, driver.Failed: 1, driver.Deleted: 1, driver.Processing: 1, driver.Enqueued: 1}
	for _, st := range driver.States {
		if b.Counts[st] != want[st] {
			t.Fatalf("counts %v, want %v", b.Counts, want)
		}
	}
	if b.Total != 5 {
		t.Fatalf("total %d, want 5", b.Total)
	}
}

func testBatchNotFound(t *testing.T, s driver.Store) {
	ctx := t.Context()
	_, err := s.Batch(ctx, 1<<40)
	wantErr(t, err, driver.ErrNotFound)
	wantErr(t, s.SealBatch(ctx, 1<<40), driver.ErrNotFound)
	_, err = s.Insert(ctx, []driver.InsertParams{afterBatch("q", 1<<40)})
	wantErr(t, err, driver.ErrNotFound)
	wantEmpty(t, s)

	parent := add(t, s, task("p"))
	member := members("q", 1<<40, 1)[0]
	unique := member
	unique.UniqueKey = key("unknown batch")
	child := member
	child.Parents = []driver.Parent{{ID: parent, On: driver.OnSucceeded}}
	late := afterBatch("q", 1<<40)
	late.Parents = child.Parents
	for name, p := range map[string]driver.InsertParams{"member": member, "unique member": unique, "child member": child, "child after batch": late} {
		_, err := s.Insert(ctx, []driver.InsertParams{task("q"), p})
		if !errors.Is(err, driver.ErrNotFound) {
			t.Fatalf("%s of unknown batch: err %v, want not found", name, err)
		}
	}
	if c := counts(t, s); c.Enqueued != 1 || c.Awaiting != 0 {
		t.Fatalf("counts %+v after rejected inserts, want only the parent", c)
	}
}

func testBatchList(t *testing.T, s driver.Store) {
	var ids []int64
	for range 5 {
		ids = append(ids, openBatch(t, s))
	}
	slices.Reverse(ids)
	var got []int64
	q := driver.BatchQuery{Limit: 2}
	for range 10 {
		p, err := s.Batches(t.Context(), q)
		if err != nil {
			t.Fatal(err)
		}
		if len(p.Batches) > q.Limit {
			t.Fatalf("page of %d batches, limit %d", len(p.Batches), q.Limit)
		}
		for _, b := range p.Batches {
			got = append(got, b.ID)
		}
		if p.Next == "" {
			break
		}
		q.Cursor = p.Next
	}
	if !slices.Equal(got, ids) {
		t.Fatalf("batches %v, want %v", got, ids)
	}
}

func testBatchNested(t *testing.T, s driver.Store) {
	outer := openBatch(t, s)
	inner := nest(t, s, outer)
	if b := batch(t, s, inner); b.Parent != outer || b.Nested != 0 || b.Description != "part" {
		t.Fatalf("nested batch %+v", b)
	}
	if b := batch(t, s, outer); b.Parent != 0 || b.Nested != 1 || b.NestedFinished != 0 || b.Total != 0 {
		t.Fatalf("outer batch %+v", b)
	}
	m := add(t, s, members("m", inner, 1)[0])
	next := add(t, s, afterBatch("next", outer))
	seal(t, s, outer)
	seal(t, s, inner)
	sweep(t, s)
	wantFinished(t, s, inner, false)
	wantFinished(t, s, outer, false)
	wantState(t, s, driver.Awaiting, next)

	apply(t, s, outcome(claimOne(t, s, "m", m), driver.Succeeded))
	if b := wantFinished(t, s, inner, true); b.Total != 1 || b.Counts[driver.Succeeded] != 1 {
		t.Fatalf("nested batch %+v", b)
	}
	b := wantFinished(t, s, outer, true)
	if b.Total != 0 || len(b.Counts) != 0 || b.Nested != 1 || b.NestedFinished != 1 {
		t.Fatalf("outer batch %+v", b)
	}
	wantState(t, s, driver.Enqueued, next)
}

func testBatchNestedChain(t *testing.T, s driver.Store) {
	top := openBatch(t, s)
	mid := nest(t, s, top)
	low := nest(t, s, mid)
	m := add(t, s, members("m", low, 1)[0])
	var next []int64
	for _, id := range []int64{top, mid, low} {
		next = append(next, add(t, s, afterBatch("next", id)))
		seal(t, s, id)
	}
	wantFinished(t, s, top, false)
	apply(t, s, outcome(claimOne(t, s, "m", m), driver.Succeeded))
	for _, id := range []int64{low, mid, top} {
		wantFinished(t, s, id, true)
	}
	wantState(t, s, driver.Enqueued, next...)
}

func testBatchNestedOpen(t *testing.T, s driver.Store) {
	ctx := t.Context()
	_, err := s.OpenBatch(ctx, driver.NewBatch{Parent: 1 << 40})
	wantErr(t, err, driver.ErrNotFound)
	done := openBatch(t, s)
	seal(t, s, done)
	_, err = s.OpenBatch(ctx, driver.NewBatch{Parent: done})
	wantErr(t, err, driver.ErrClosed)

	outer := openBatch(t, s)
	inner := nest(t, s, outer)
	seal(t, s, outer)
	m := add(t, s, members("m", outer, 1)[0])
	late := nest(t, s, outer)
	if b := batch(t, s, outer); b.Total != 1 || b.Nested != 2 {
		t.Fatalf("outer batch %+v", b)
	}
	seal(t, s, inner)
	seal(t, s, late)
	wantFinished(t, s, late, true)
	wantFinished(t, s, outer, false)
	apply(t, s, outcome(claimOne(t, s, "m", m), driver.Succeeded))
	wantFinished(t, s, outer, true)
	_, err = s.OpenBatch(ctx, driver.NewBatch{Parent: outer})
	wantErr(t, err, driver.ErrClosed)

	p, err := s.Batches(ctx, driver.BatchQuery{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Batches) != 4 {
		t.Fatalf("%d batches after refused opens, want 4", len(p.Batches))
	}
}

func testBatchNestedFailed(t *testing.T, s driver.Store) {
	outer := openBatch(t, s)
	inner := nest(t, s, outer)
	m := add(t, s, members("m", inner, 1)[0])
	seal(t, s, inner)
	seal(t, s, outer)
	apply(t, s, outcome(claimOne(t, s, "m", m), driver.Failed))
	sweep(t, s)
	wantFinished(t, s, inner, false)
	wantFinished(t, s, outer, false)
	deleteIDs(t, s, m)
	wantFinished(t, s, inner, true)
	wantFinished(t, s, outer, true)
}

func testBatchNestedEmpty(t *testing.T, s driver.Store) {
	outer := openBatch(t, s)
	inner := nest(t, s, outer)
	next := add(t, s, afterBatch("next", outer))
	seal(t, s, outer)
	wantFinished(t, s, outer, false)
	wantState(t, s, driver.Awaiting, next)
	seal(t, s, inner)
	wantFinished(t, s, inner, true)
	wantFinished(t, s, outer, true)
	wantState(t, s, driver.Enqueued, next)

	first := openBatch(t, s)
	seal(t, s, nest(t, s, first))
	wantFinished(t, s, first, false)
	seal(t, s, first)
	wantFinished(t, s, first, true)
}

func testBatchNestedList(t *testing.T, s driver.Store) {
	outer := openBatch(t, s)
	other := openBatch(t, s)
	var ids []int64
	for i := range 5 {
		id := nest(t, s, outer)
		if i < 2 {
			seal(t, s, id)
		}
		ids = append(ids, id)
	}
	stray := nest(t, s, other)
	if b := batch(t, s, outer); b.Nested != 5 || b.NestedFinished != 2 || b.Parent != 0 {
		t.Fatalf("outer batch %+v", b)
	}
	slices.Reverse(ids)
	var got []int64
	q := driver.BatchQuery{Parent: outer, Limit: 2}
	for range 10 {
		p, err := s.Batches(t.Context(), q)
		if err != nil {
			t.Fatal(err)
		}
		if len(p.Batches) > q.Limit {
			t.Fatalf("page of %d batches, limit %d", len(p.Batches), q.Limit)
		}
		for _, b := range p.Batches {
			if b.Parent != outer {
				t.Fatalf("batch %d of parent %d listed under %d", b.ID, b.Parent, outer)
			}
			got = append(got, b.ID)
		}
		if p.Next == "" {
			break
		}
		q.Cursor = p.Next
	}
	if !slices.Equal(got, ids) {
		t.Fatalf("nested batches %v, want %v", got, ids)
	}
	p, err := s.Batches(t.Context(), driver.BatchQuery{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Batches) != 8 || p.Batches[0].ID != stray || p.Batches[0].Parent != other {
		t.Fatalf("all batches %+v, want 8 starting with %d", p.Batches, stray)
	}
}

func testBatchNestedConcurrent(t *testing.T, s driver.Store) {
	const width = 16
	outer := openBatch(t, s)
	for range width {
		inner := nest(t, s, outer)
		add(t, s, members("m", inner, 1)[0])
		seal(t, s, inner)
	}
	seal(t, s, outer)
	next := add(t, s, afterBatch("next", outer))
	js := claimN(t, s, width, "m")
	ctx := t.Context()
	deadline := time.Now().Add(15 * time.Second)
	begin := make(chan struct{})
	var wg sync.WaitGroup
	for i, j := range js {
		wg.Go(func() {
			<-begin
			srv := fmt.Sprintf("s%d", i+2)
			for time.Now().Before(deadline) {
				rs, err := s.Finish(ctx, srv, []driver.Outcome{outcome(j, driver.Succeeded)})
				if err != nil || len(rs) != 1 {
					t.Errorf("finish job %d: %v %v", j.ID, rs, err)
					return
				}
				if rs[0] != driver.Busy {
					if rs[0] != driver.Applied {
						t.Errorf("finish job %d: %d, want applied", j.ID, rs[0])
					}
					return
				}
				time.Sleep(time.Millisecond)
			}
			t.Errorf("job %d still busy after 15s", j.ID)
		})
	}
	close(begin)
	wg.Wait()
	if t.Failed() {
		return
	}
	if b := wantFinished(t, s, outer, true); b.Nested != width || b.NestedFinished != width {
		t.Fatalf("outer batch %+v", b)
	}
	wantState(t, s, driver.Enqueued, next)
}

func testBatchNestedPrune(t *testing.T, s driver.Store) {
	ctx := t.Context()
	outer := openBatch(t, s)
	inner := nest(t, s, outer)
	m := add(t, s, members("m", inner, 1)[0])
	seal(t, s, inner)
	seal(t, s, outer)
	apply(t, s, outcome(claimOne(t, s, "m", m), driver.Succeeded))
	wantFinished(t, s, outer, true)
	prune(t, s, keep())
	batch(t, s, inner)
	batch(t, s, outer)

	time.Sleep(20 * time.Millisecond)
	pp := keep()
	pp.Succeeded = 5 * time.Millisecond
	for range 5 {
		prune(t, s, pp)
		_, errInner := s.Batch(ctx, inner)
		_, errOuter := s.Batch(ctx, outer)
		switch {
		case errOuter == nil:
		case errInner == nil:
			t.Fatalf("batch %d pruned before the batch %d nested in it", outer, inner)
		default:
			wantErr(t, errInner, driver.ErrNotFound)
			wantErr(t, errOuter, driver.ErrNotFound)
			return
		}
	}
	t.Fatalf("batches %d and %d left after 5 prunes", outer, inner)
}

package drivertest

import (
	"errors"
	"maps"
	"slices"
	"testing"

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
}

func openBatch(t *testing.T, w driver.Writer) int64 {
	t.Helper()
	id, err := w.OpenBatch(t.Context(), driver.NewBatch{Description: "import"})
	if err != nil {
		t.Fatalf("open batch: %v", err)
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

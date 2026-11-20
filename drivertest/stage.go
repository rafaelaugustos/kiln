package drivertest

import (
	"slices"
	"testing"

	"github.com/rafaelaugustos/kiln/driver"
)

var stageTests = []test{
	{"Apply", testStageApply},
	{"ApplyOnce", testStageApplyOnce},
	{"OwnWrites", testStageOwnWrites},
	{"Dropped", testStageDropped},
	{"LateDuplicate", testStageLateDuplicate},
	{"ParentGone", testStageParentGone},
	{"BatchClosed", testStageBatchClosed},
	{"Limit", testStageLimit},
	{"Invalid", testStageInvalid},
}

func stager(t *testing.T, s driver.Store) driver.Stager {
	t.Helper()
	st, ok := s.(driver.Stager)
	if !ok {
		t.Skip("store does not implement driver.Stager")
	}
	return st
}

func stage(t *testing.T, s driver.Store) driver.Staged {
	t.Helper()
	w, err := stager(t, s).Stage(t.Context())
	if err != nil {
		t.Fatalf("stage: %v", err)
	}
	return w
}

func encode(t *testing.T, w driver.Staged) (string, []byte) {
	t.Helper()
	id, data, err := w.Encode()
	if err != nil || id == "" {
		t.Fatalf("encode: id %q, %v", id, err)
	}
	return id, data
}

func applyStaged(t *testing.T, s driver.Store, w driver.Staged) {
	t.Helper()
	id, data := encode(t, w)
	if err := stager(t, s).Apply(t.Context(), id, data); err != nil {
		t.Fatalf("apply: %v", err)
	}
}

func wantMissing(t *testing.T, s driver.Store, ids ...int64) {
	t.Helper()
	for _, id := range ids {
		if _, err := s.Job(t.Context(), id); err == nil {
			t.Fatalf("job %d exists, want none", id)
		} else {
			wantErr(t, err, driver.ErrNotFound)
		}
	}
}

func testStageApply(t *testing.T, s driver.Store) {
	w := stage(t, s)
	ids := insertedIDs(insert(t, w, tasks(2, "st")...))
	bid := openBatch(t, w)
	member := insertedIDs(insert(t, w, members("st", bid, 1)...))[0]
	next := add(t, w, afterBatch("st", bid))
	seal(t, w, bid)
	if !slices.IsSorted(ids) || ids[0] <= 0 {
		t.Fatalf("staged ids %v", ids)
	}
	wantMissing(t, s, ids[0], ids[1], member, next)
	if _, err := s.Batch(t.Context(), bid); err == nil {
		t.Fatalf("batch %d exists before apply", bid)
	}
	wantEmpty(t, s)

	applyStaged(t, s, w)
	wantState(t, s, driver.Enqueued, ids[0], ids[1], member)
	wantState(t, s, driver.Awaiting, next)
	if b := wantFinished(t, s, bid, false); !b.Sealed || b.Total != 1 {
		t.Fatalf("batch %+v", b)
	}
	if got := sorted(jobIDs(claim(t, s, 10, "st"))); !slices.Equal(got, []int64{ids[0], ids[1], member}) {
		t.Fatalf("claimed %v", got)
	}
}

func testStageApplyOnce(t *testing.T, s driver.Store) {
	w := stage(t, s)
	ids := insertedIDs(insert(t, w, tasks(3, "once")...))
	id, data := encode(t, w)
	for range 3 {
		if err := stager(t, s).Apply(t.Context(), id, data); err != nil {
			t.Fatalf("apply: %v", err)
		}
	}
	if c := counts(t, s); c.Enqueued != 3 {
		t.Fatalf("enqueued %d after applying the same writes three times, want 3", c.Enqueued)
	}
	if got := sorted(jobIDs(claim(t, s, 10, "once"))); !slices.Equal(got, ids) {
		t.Fatalf("claimed %v, want %v", got, ids)
	}
}

func testStageOwnWrites(t *testing.T, s driver.Store) {
	w := stage(t, s)
	parent := add(t, w, task("p"))
	in := insert(t, w, after("c", driver.OnSucceeded, parent))[0]
	if in.State != driver.Awaiting {
		t.Fatalf("child staged as %s, want awaiting", in.State)
	}
	holder := add(t, w, uniq("u", "own"))
	if dup := insert(t, w, uniq("u", "own"))[0]; !dup.Duplicate || dup.ID != holder {
		t.Fatalf("second unique insert %+v, want duplicate of %d", dup, holder)
	}
	applyStaged(t, s, w)
	if r := record(t, s, in.ID); r.State != driver.Awaiting || r.PendingDeps != 1 || !slices.Equal(r.Parents, []int64{parent}) {
		t.Fatalf("child state %s pending %d parents %v", r.State, r.PendingDeps, r.Parents)
	}
	wantDuplicate(t, s, uniq("u", "own"), holder)
	apply(t, s, outcome(claimOne(t, s, "p", parent), driver.Succeeded))
	wantState(t, s, driver.Enqueued, in.ID)
}

func testStageDropped(t *testing.T, s driver.Store) {
	w := stage(t, s)
	p := uniq("drop", "k")
	id := add(t, w, p)
	bid := openBatch(t, w)
	encode(t, w)
	wantMissing(t, s, id)
	if _, err := s.Batch(t.Context(), bid); err == nil {
		t.Fatalf("batch %d exists, want none", bid)
	}
	wantEmpty(t, s)
	wantFresh(t, s, p)
}

func testStageLateDuplicate(t *testing.T, s driver.Store) {
	w := stage(t, s)
	p := uniq("late", "k")
	ins := insert(t, w, p, driver.InsertParams{
		Kind: "task", Queue: "late", Args: []byte(`{}`), MaxAttempts: 1,
		Parents: []driver.Parent{{Index: 0, On: driver.OnSucceeded}},
	})
	if ins[0].Duplicate {
		t.Fatalf("staged %+v, want a new job", ins[0])
	}
	holder := wantFresh(t, s, p)
	applyStaged(t, s, w)
	wantMissing(t, s, ins[0].ID)
	if r := record(t, s, ins[1].ID); r.State != driver.Awaiting || !slices.Equal(r.Parents, []int64{holder}) {
		t.Fatalf("child state %s parents %v, want awaiting on %d", r.State, r.Parents, holder)
	}
	wantDuplicate(t, s, p, holder)
}

func testStageParentGone(t *testing.T, s driver.Store) {
	parent := add(t, s, task("gone"))
	w := stage(t, s)
	other := add(t, w, task("gone"))
	child := add(t, w, after("gone", driver.OnSucceeded, parent))
	if n := deleteIDs(t, s, parent); n != 1 {
		t.Fatalf("deleted %d", n)
	}
	prune(t, s, driver.PruneParams{Retention: driver.Retention{Succeeded: -1, Failed: -1}})
	wantMissing(t, s, parent)
	id, data := encode(t, w)
	for range 2 {
		wantErr(t, stager(t, s).Apply(t.Context(), id, data), driver.ErrNotFound)
	}
	wantMissing(t, s, other, child)
	if c := counts(t, s); c.Enqueued+c.Awaiting != 0 {
		t.Fatalf("counts %+v after a failed apply, want no live job", c)
	}
}

func testStageBatchClosed(t *testing.T, s driver.Store) {
	bid := openBatch(t, s)
	w := stage(t, s)
	member := insertedIDs(insert(t, w, members("closed", bid, 1)...))[0]
	seal(t, s, bid)
	wantFinished(t, s, bid, true)
	id, data := encode(t, w)
	wantErr(t, stager(t, s).Apply(t.Context(), id, data), driver.ErrClosed)
	wantMissing(t, s, member)
}

func testStageLimit(t *testing.T, s driver.Store) {
	w := stage(t, s)
	ids := insertedIDs(insert(t, w, limited("st", "k", 1), limited("st", "k", 1)))
	applyStaged(t, s, w)
	wantState(t, s, driver.Enqueued, ids[0])
	wantState(t, s, driver.Throttled, ids[1])
	wantConsistent(t, s)
	apply(t, s, outcome(claimOne(t, s, "st", ids[0]), driver.Succeeded))
	wantState(t, s, driver.Enqueued, ids[1])
}

func testStageInvalid(t *testing.T, s driver.Store) {
	wantErr(t, stager(t, s).Apply(t.Context(), "nonsense", []byte("not staged writes")), driver.ErrInvalid)
	wantEmpty(t, s)
}

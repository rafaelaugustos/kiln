package drivertest

import (
	"errors"
	"slices"
	"testing"

	"github.com/rafaelaugustos/kiln/driver"
)

var txTests = []test{
	{"Commit", testTxCommit},
	{"Rollback", testTxRollback},
	{"InsertError", testTxInsertError},
	{"OwnWrites", testTxOwnWrites},
	{"Batch", testTxBatch},
	{"NestedBatch", testTxNestedBatch},
	{"Limit", testTxLimit},
	{"SealLimit", testTxSealLimit},
}

func transactor(t *testing.T, s driver.Store) driver.Transactor {
	t.Helper()
	tx, ok := s.(driver.Transactor)
	if !ok {
		t.Skip("store does not implement driver.Transactor")
	}
	return tx
}

func inTx(t *testing.T, s driver.Store, fn func(w driver.Writer) error) {
	t.Helper()
	if err := transactor(t, s).InTx(t.Context(), fn); err != nil {
		t.Fatalf("in tx: %v", err)
	}
}

func testTxCommit(t *testing.T, s driver.Store) {
	var ids []int64
	inTx(t, s, func(w driver.Writer) error {
		ids = insertedIDs(insert(t, w, tasks(3, "tx")...))
		return nil
	})
	if got := sorted(jobIDs(claim(t, s, 10, "tx"))); !slices.Equal(got, ids) {
		t.Fatalf("claimed %v, want %v", got, ids)
	}
}

func testTxRollback(t *testing.T, s driver.Store) {
	ctx := t.Context()
	boom := errors.New("boom")
	p := uniq("tx", "rollback")
	var id, bid int64
	err := transactor(t, s).InTx(ctx, func(w driver.Writer) error {
		id = add(t, w, p)
		bid = openBatch(t, w)
		return boom
	})
	wantErr(t, err, boom)
	_, err = s.Job(ctx, id)
	wantErr(t, err, driver.ErrNotFound)
	_, err = s.Batch(ctx, bid)
	wantErr(t, err, driver.ErrNotFound)
	wantEmpty(t, s)
	wantFresh(t, s, p)
}

func testTxInsertError(t *testing.T, s driver.Store) {
	ctx := t.Context()
	err := transactor(t, s).InTx(ctx, func(w driver.Writer) error {
		insert(t, w, tasks(2, "tx")...)
		_, err := w.Insert(ctx, []driver.InsertParams{after("tx", driver.OnSucceeded, 1<<40)})
		return err
	})
	wantErr(t, err, driver.ErrNotFound)
	wantEmpty(t, s)
}

func testTxOwnWrites(t *testing.T, s driver.Store) {
	var parent, child, holder int64
	inTx(t, s, func(w driver.Writer) error {
		parent = add(t, w, task("p"))
		in := insert(t, w, after("c", driver.OnSucceeded, parent))[0]
		if child = in.ID; in.State != driver.Awaiting {
			t.Errorf("child inserted as %s, want awaiting", in.State)
		}
		holder = add(t, w, uniq("u", "k"))
		if in := insert(t, w, uniq("u", "k"))[0]; !in.Duplicate || in.ID != holder {
			t.Errorf("second unique insert in tx %+v, want duplicate of %d", in, holder)
		}
		return nil
	})
	if r := record(t, s, child); r.State != driver.Awaiting || r.PendingDeps != 1 || !slices.Equal(r.Parents, []int64{parent}) {
		t.Fatalf("child state %s pending %d parents %v", r.State, r.PendingDeps, r.Parents)
	}
	wantDuplicate(t, s, uniq("u", "k"), holder)
	apply(t, s, outcome(claimOne(t, s, "p", parent), driver.Succeeded))
	wantState(t, s, driver.Enqueued, child)
}

func testTxBatch(t *testing.T, s driver.Store) {
	var bid, next int64
	inTx(t, s, func(w driver.Writer) error {
		bid = openBatch(t, w)
		insert(t, w, members("m", bid, 2)...)
		next = add(t, w, afterBatch("next", bid))
		return w.SealBatch(t.Context(), bid)
	})
	if b := wantFinished(t, s, bid, false); !b.Sealed || b.Total != 2 {
		t.Fatalf("batch %+v", b)
	}
	wantState(t, s, driver.Awaiting, next)
	js := claimN(t, s, 2, "m")
	apply(t, s, outcome(js[0], driver.Succeeded), outcome(js[1], driver.Succeeded))
	wantFinished(t, s, bid, true)
	wantState(t, s, driver.Enqueued, next)
}

func testTxNestedBatch(t *testing.T, s driver.Store) {
	outer := openBatch(t, s)
	var inner, top, low int64
	inTx(t, s, func(w driver.Writer) error {
		inner = nest(t, w, outer)
		insert(t, w, members("m", inner, 1)...)
		top = openBatch(t, w)
		low = nest(t, w, top)
		seal(t, w, low)
		seal(t, w, top)
		return w.SealBatch(t.Context(), inner)
	})
	if b := wantFinished(t, s, inner, false); b.Parent != outer || !b.Sealed || b.Total != 1 {
		t.Fatalf("nested batch %+v", b)
	}
	if b := wantFinished(t, s, top, true); b.Nested != 1 || b.NestedFinished != 1 {
		t.Fatalf("batch %d %+v", top, b)
	}
	seal(t, s, outer)
	if b := wantFinished(t, s, outer, false); b.Nested != 1 {
		t.Fatalf("outer batch %+v", b)
	}
	apply(t, s, outcome(claimN(t, s, 1, "m")[0], driver.Succeeded))
	wantFinished(t, s, inner, true)
	wantFinished(t, s, outer, true)

	err := transactor(t, s).InTx(t.Context(), func(w driver.Writer) error {
		_, err := w.OpenBatch(t.Context(), driver.NewBatch{Parent: outer})
		return err
	})
	wantErr(t, err, driver.ErrClosed)
}

func testTxLimit(t *testing.T, s driver.Store) {
	var ids []int64
	inTx(t, s, func(w driver.Writer) error {
		ids = insertedIDs(insert(t, w, limited("tx", "k", 1), limited("tx", "k", 1)))
		return nil
	})
	for range 3 {
		sweep(t, s)
	}
	wantState(t, s, driver.Enqueued, ids[0])
	wantState(t, s, driver.Throttled, ids[1])
	apply(t, s, outcome(claimOne(t, s, "tx", ids[0]), driver.Succeeded))
	wantState(t, s, driver.Enqueued, ids[1])
}

func testTxSealLimit(t *testing.T, s driver.Store) {
	held := func(key string) (int64, int64) {
		bid := openBatch(t, s)
		p := afterBatch("seal", bid)
		p.LimitKey, p.LimitMax = key, 1
		id := add(t, s, p)
		wantState(t, s, driver.Awaiting, id)
		return bid, id
	}
	bid, id := held("own")
	seal(t, s, bid)
	wantState(t, s, driver.Enqueued, id)

	bid, id = held("tx")
	inTx(t, s, func(w driver.Writer) error { return w.SealBatch(t.Context(), bid) })
	wantState(t, s, driver.Enqueued, id)
}

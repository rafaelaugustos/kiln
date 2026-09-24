package memstore_test

import (
	"errors"
	"testing"

	"github.com/rafaelaugustos/kiln/driver"
)

func TestBatchLifecycle(t *testing.T) {
	t.Parallel()
	s, _ := open(t)
	empty, err := s.OpenBatch(ctx, driver.NewBatch{Description: "empty"})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SealBatch(ctx, empty); err != nil {
		t.Fatal(err)
	}
	if b, _ := s.Batch(ctx, empty); !b.Sealed || b.FinishedAt.IsZero() {
		t.Fatalf("empty batch after seal: %+v", b)
	}
	if _, err := s.Insert(ctx, []driver.InsertParams{params("a", inBatch(empty))}); !errors.Is(err, driver.ErrClosed) {
		t.Fatalf("insert into finished batch: %v", err)
	}
	if err := s.SealBatch(ctx, 999); !errors.Is(err, driver.ErrNotFound) {
		t.Fatalf("seal unknown: %v", err)
	}

	id, err := s.OpenBatch(ctx, driver.NewBatch{Description: "work", Meta: map[string]string{"k": "v"}})
	if err != nil {
		t.Fatal(err)
	}
	members := ids(t, s, params("a", inBatch(id)), params("a", inBatch(id)))
	then := ids(t, s, params("then", queue("then"), afterBatch(id)))[0]
	if err := s.SealBatch(ctx, id); err != nil {
		t.Fatal(err)
	}
	if err := s.SealBatch(ctx, id); err != nil {
		t.Fatalf("seal twice: %v", err)
	}
	insert(t, s, params("late", queue("late"), inBatch(id)))

	finish(t, s, done(claimOne(t, s), driver.Succeeded))
	finish(t, s, done(claimOne(t, s), driver.Failed))
	finish(t, s, done(claim(t, s, 1, "late")[0], driver.Succeeded))
	b, err := s.Batch(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if !b.FinishedAt.IsZero() || b.Total != 3 || b.Counts[driver.Succeeded] != 2 || b.Counts[driver.Failed] != 1 || b.Meta["k"] != "v" {
		t.Fatalf("batch with failed member: %+v", b)
	}
	expectState(t, s, then, driver.Awaiting)

	if n, err := s.Requeue(ctx, driver.Filter{State: driver.Failed, BatchID: id}); err != nil || n != 1 {
		t.Fatalf("requeue = %d, %v", n, err)
	}
	j := claimOne(t, s)
	if j.ID != members[1] {
		t.Fatalf("claimed %d, want %d", j.ID, members[1])
	}
	finish(t, s, done(j, driver.Succeeded))
	if b, _ := s.Batch(ctx, id); b.FinishedAt.IsZero() || b.Counts[driver.Succeeded] != 3 {
		t.Fatalf("batch not finished: %+v", b)
	}
	expectState(t, s, then, driver.Enqueued)
	if _, err := s.Insert(ctx, []driver.InsertParams{params("a", inBatch(id))}); !errors.Is(err, driver.ErrClosed) {
		t.Fatalf("insert into finished batch: %v", err)
	}
	if got := ids(t, s, params("x", afterBatch(id)))[0]; record(t, s, got).State != driver.Enqueued {
		t.Fatalf("after finished batch not enqueued")
	}

	page, err := s.Batches(ctx, driver.BatchQuery{Limit: 1})
	if err != nil || len(page.Batches) != 1 || page.Batches[0].ID != id || page.Next == "" {
		t.Fatalf("batches page = %+v, %v", page, err)
	}
	page, err = s.Batches(ctx, driver.BatchQuery{Limit: 1, Cursor: page.Next})
	if err != nil || len(page.Batches) != 1 || page.Batches[0].ID != empty || page.Next != "" {
		t.Fatalf("second batches page = %+v, %v", page, err)
	}
}

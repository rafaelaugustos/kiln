package pgstore

import (
	"context"
	"errors"
	"testing"

	"github.com/rafaelaugustos/kiln/driver"
)

func inBatch(id int64) func(*driver.InsertParams) {
	return func(p *driver.InsertParams) { p.BatchID = id }
}

func TestBatchLifecycle(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	b, err := s.OpenBatch(ctx, driver.NewBatch{Description: "import", Meta: map[string]string{"src": "csv"}})
	if err != nil {
		t.Fatal(err)
	}
	members := insert(t, s, job("m", inBatch(b)), job("m", inBatch(b)))
	cont := insert(t, s, job("after", func(p *driver.InsertParams) { p.AfterBatch = b }))[0]
	if cont.State != driver.Awaiting {
		t.Fatalf("continuation %+v", cont)
	}
	if err := s.SealBatch(ctx, b); err != nil {
		t.Fatal(err)
	}
	run(t, s, members[0].ID, driver.Succeeded)
	run(t, s, members[1].ID, driver.Failed)
	got, err := s.Batch(ctx, b)
	if err != nil {
		t.Fatal(err)
	}
	if !got.FinishedAt.IsZero() || got.Total != 2 || got.Counts[driver.Succeeded] != 1 || got.Counts[driver.Failed] != 1 || got.Meta["src"] != "csv" {
		t.Fatalf("batch with failed member %+v", got)
	}
	if more := insert(t, s, job("m", inBatch(b)))[0]; more.State != driver.Enqueued {
		t.Fatalf("attach to open sealed batch %+v", more)
	} else {
		run(t, s, more.ID, driver.Succeeded)
	}
	if n, err := s.Requeue(ctx, driver.Filter{IDs: []int64{members[1].ID}}); err != nil || n != 1 {
		t.Fatalf("requeue %d %v", n, err)
	}
	run(t, s, members[1].ID, driver.Succeeded)
	got, err = s.Batch(ctx, b)
	if err != nil {
		t.Fatal(err)
	}
	if got.FinishedAt.IsZero() || got.Total != 3 {
		t.Fatalf("batch not finished %+v", got)
	}
	if r := record(t, s, cont.ID); r.State != driver.Enqueued {
		t.Fatalf("continuation %s", r.State)
	}
	if _, err := s.Insert(ctx, []driver.InsertParams{job("m", inBatch(b))}); !errors.Is(err, driver.ErrClosed) {
		t.Fatalf("attach to finished batch: %v", err)
	}
	if _, err := s.Insert(ctx, []driver.InsertParams{job("m", inBatch(b), unique("u", 0))}); !errors.Is(err, driver.ErrClosed) {
		t.Fatalf("attach unique to finished batch: %v", err)
	}
	if _, err := s.Insert(ctx, []driver.InsertParams{job("m", inBatch(1<<40))}); !errors.Is(err, driver.ErrNotFound) {
		t.Fatalf("attach to missing batch: %v", err)
	}
	if dup := insert(t, s, job("m", unique("u", 0)))[0]; dup.Duplicate {
		t.Fatalf("closed-batch insert leaked a unique key")
	}
}

func TestBatchSeal(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	empty, err := s.OpenBatch(ctx, driver.NewBatch{})
	if err != nil {
		t.Fatal(err)
	}
	cont := insert(t, s, job("after", func(p *driver.InsertParams) { p.AfterBatch = empty }))[0]
	for range 2 {
		if err := s.SealBatch(ctx, empty); err != nil {
			t.Fatal(err)
		}
	}
	if b, _ := s.Batch(ctx, empty); b.FinishedAt.IsZero() || !b.Sealed {
		t.Fatalf("empty batch %+v", b)
	}
	if r := record(t, s, cont.ID); r.State != driver.Enqueued {
		t.Fatalf("continuation %s", r.State)
	}
	late, _ := s.OpenBatch(ctx, driver.NewBatch{})
	m := insert(t, s, job("m", inBatch(late)))[0]
	run(t, s, m.ID, driver.Succeeded)
	if b, _ := s.Batch(ctx, late); !b.FinishedAt.IsZero() {
		t.Fatalf("unsealed batch finished")
	}
	if err := s.SealBatch(ctx, late); err != nil {
		t.Fatal(err)
	}
	if b, _ := s.Batch(ctx, late); b.FinishedAt.IsZero() {
		t.Fatalf("seal after members done did not finish")
	}
	if err := s.SealBatch(ctx, 1<<40); !errors.Is(err, driver.ErrNotFound) {
		t.Fatalf("seal missing: %v", err)
	}
	after := insert(t, s, job("x", func(p *driver.InsertParams) { p.AfterBatch = late }))[0]
	if after.State != driver.Enqueued {
		t.Fatalf("after finished batch %+v", after)
	}
	page, err := s.Batches(ctx, driver.BatchQuery{Limit: 1})
	if err != nil || len(page.Batches) != 1 || page.Batches[0].ID != late || page.Next == "" {
		t.Fatalf("batches page %+v %v", page, err)
	}
	page, err = s.Batches(ctx, driver.BatchQuery{Limit: 1, Cursor: page.Next})
	if err != nil || len(page.Batches) != 1 || page.Batches[0].ID != empty || page.Next != "" {
		t.Fatalf("batches page 2 %+v %v", page, err)
	}
}

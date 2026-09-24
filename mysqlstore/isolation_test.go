package mysqlstore

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

func beginRR(t *testing.T, s *Store) *sql.Tx {
	t.Helper()
	tx, err := s.db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tx.Rollback() })
	var n int
	if err := tx.QueryRow("SELECT COUNT(*) FROM kiln_jobs").Scan(&n); err != nil {
		t.Fatal(err)
	}
	return tx
}

func TestTxSealRepeatableRead(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	b, err := s.OpenBatch(ctx, driver.NewBatch{})
	if err != nil {
		t.Fatal(err)
	}
	cont := insert(t, s, job("after", func(p *driver.InsertParams) { p.AfterBatch = b }))[0]
	tx := beginRR(t, s)
	member := insert(t, s, job("m", inBatch(b)))[0]
	if err := s.Tx(tx).SealBatch(ctx, b); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if got, err := s.Batch(ctx, b); err != nil || !got.FinishedAt.IsZero() {
		t.Fatalf("batch %+v %v finished while member %d is %s", got, err, member.ID, record(t, s, member.ID).State)
	}
	if r := record(t, s, cont.ID); r.State != driver.Awaiting {
		t.Fatalf("continuation %s before its batch finished", r.State)
	}
	j := claim(t, s, 1)[0]
	finish(t, s, driver.Outcome{Ref: j.Ref, State: driver.Succeeded})
	if got, err := s.Batch(ctx, b); err != nil || got.FinishedAt.IsZero() {
		t.Fatalf("batch after its last member %+v %v", got, err)
	}
	if r := record(t, s, cont.ID); r.State != driver.Enqueued {
		t.Fatalf("continuation %s after the batch finished", r.State)
	}

	empty, err := s.OpenBatch(ctx, driver.NewBatch{})
	if err != nil {
		t.Fatal(err)
	}
	tx = beginRR(t, s)
	if err := s.Tx(tx).SealBatch(ctx, empty); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if got, err := s.Batch(ctx, empty); err != nil || got.FinishedAt.IsZero() {
		t.Fatalf("empty batch sealed in a transaction %+v %v", got, err)
	}
}

func TestTxDuplicateRepeatableRead(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	held := insert(t, s, job("a", unique("held", 0)))[0]
	tx := beginRR(t, s)
	late := insert(t, s, job("a", unique("late", 0)))[0]
	res, err := s.Tx(tx).Insert(ctx, []driver.InsertParams{job("a", unique("held", 0)), job("a", unique("late", 0))})
	if err != nil {
		t.Fatal(err)
	}
	if !res[0].Duplicate || res[0].ID != held.ID || !res[1].Duplicate || res[1].ID != late.ID {
		t.Fatalf("duplicates %+v, want %d and %d", res, held.ID, late.ID)
	}
	js := claim(t, s, 2)
	if len(js) != 2 {
		t.Fatalf("claimed %d holders while a transaction checks their keys, want 2", len(js))
	}
	finishSoon(t, s, driver.Outcome{Ref: js[0].Ref, State: driver.Succeeded}, driver.Outcome{Ref: js[1].Ref, State: driver.Succeeded})
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if again := insert(t, s, job("a", unique("held", 0)))[0]; again.Duplicate {
		t.Fatalf("key still held after release: %+v", again)
	}
}

func TestTxNewKeysRepeatableRead(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	first := beginRR(t, s)
	if _, err := s.Tx(first).Insert(ctx, []driver.InsertParams{job("a", unique("one", 0))}); err != nil {
		t.Fatal(err)
	}
	second := beginRR(t, s)
	soon, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	res, err := s.Tx(second).Insert(soon, []driver.InsertParams{job("a", unique("two", 0))})
	if err != nil {
		t.Fatalf("insert of other keys behind an open transaction: %v", err)
	}
	if res[0].Duplicate {
		t.Fatalf("fresh key reported as duplicate: %+v", res[0])
	}
	if err := second.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := first.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestTxParentRepeatableRead(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	parent := insert(t, s, job("p"))[0].ID
	tx := beginRR(t, s)
	j := claim(t, s, 1)[0]
	finish(t, s, driver.Outcome{Ref: j.Ref, State: driver.Succeeded})
	child, err := s.Tx(tx).Insert(ctx, []driver.InsertParams{job("c", after(driver.OnSucceeded, parent))})
	if err != nil {
		t.Fatalf("child of a parent archived after the snapshot: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if r := record(t, s, child[0].ID); r.State != driver.Enqueued {
		t.Fatalf("child %s, want enqueued", r.State)
	}
}

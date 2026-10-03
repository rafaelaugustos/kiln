package mssqlstore

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

func unique(key string) func(*driver.InsertParams) {
	return func(p *driver.InsertParams) {
		h := sha256.Sum256([]byte(key))
		p.UniqueKey = h[:16]
	}
}

func begin(t *testing.T, s *Store) *sql.Tx {
	t.Helper()
	tx, err := s.db.BeginTx(context.Background(), readCommitted)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tx.Rollback() })
	return tx
}

func TestTxWriterNil(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	w := s.Tx(nil)
	if _, err := w.Insert(ctx, []driver.InsertParams{job("a")}); !errors.Is(err, driver.ErrNilTx) {
		t.Fatalf("insert: %v", err)
	}
	if _, err := w.OpenBatch(ctx, driver.NewBatch{}); !errors.Is(err, driver.ErrNilTx) {
		t.Fatalf("open batch: %v", err)
	}
	if err := w.SealBatch(ctx, 1); !errors.Is(err, driver.ErrNilTx) {
		t.Fatalf("seal batch: %v", err)
	}
}

func TestTxWriterCommit(t *testing.T) {
	t.Parallel()
	b := newBus()
	s := open(t, Bus(b))
	ctx := context.Background()
	tx := begin(t, s)
	w := s.Tx(tx)
	res, err := w.Insert(ctx, []driver.InsertParams{job("a"), job("a", limited("tx", 1)), job("a", limited("tx", 1))})
	if err != nil {
		t.Fatal(err)
	}
	if res[1].State != driver.Throttled || res[2].State != driver.Throttled {
		t.Fatalf("limited jobs inserted as %+v, want throttled until admitted", res)
	}
	if got := claim(t, s, 10); len(got) != 0 {
		t.Fatalf("claimed %v before the commit", got)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if r := record(t, s, res[1].ID); r.State != driver.Throttled {
		t.Fatalf("limited job %s after commit, want throttled until Notify", r.State)
	}
	if err := w.Notify(ctx); err != nil {
		t.Fatal(err)
	}
	if got := claim(t, s, 10); len(got) != 2 {
		t.Fatalf("claimed %d after commit and Notify, want 2", len(got))
	}
	if r := record(t, s, res[2].ID); r.State != driver.Throttled {
		t.Fatalf("second limited job %s, want throttled", r.State)
	}

	var ids []int64
	err = s.InTx(ctx, func(w driver.Writer) error {
		res, err := w.Insert(ctx, []driver.InsertParams{job("b", limited("intx", 2)), job("b", limited("intx", 2))})
		for _, r := range res {
			ids = append(ids, r.ID)
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		if r := record(t, s, id); r.State != driver.Enqueued {
			t.Fatalf("InTx job %d is %s, want admitted right after commit", id, r.State)
		}
	}

	down := errors.New("bus down")
	b.mu.Lock()
	b.down = down
	b.mu.Unlock()
	tx = begin(t, s)
	w = s.Tx(tx)
	if _, err := w.Insert(ctx, []driver.InsertParams{job("c")}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := w.Notify(ctx); !errors.Is(err, down) {
		t.Fatalf("notify: %v, want %v", err, down)
	}
}

func TestTxWriterRollback(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	parent := insert(t, s, job("p"))[0].ID
	tx := begin(t, s)
	w := s.Tx(tx)
	bid, err := w.OpenBatch(ctx, driver.NewBatch{Description: "gone"})
	if err != nil {
		t.Fatal(err)
	}
	res, err := w.Insert(ctx, []driver.InsertParams{
		job("a", unique("rolled back")),
		job("a", limited("tx", 1), func(p *driver.InsertParams) { p.BatchID = bid }),
		job("c", func(p *driver.InsertParams) { p.Parents = []driver.Parent{{ID: parent, On: driver.OnSucceeded}} }),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Insert(ctx, []driver.InsertParams{job("bad", func(p *driver.InsertParams) { p.Parents = []driver.Parent{{ID: 1 << 40, On: driver.OnSucceeded}} })}); !errors.Is(err, driver.ErrNotFound) {
		t.Fatalf("insert with a missing parent: %v", err)
	}
	if err := w.SealBatch(ctx, bid); err != nil {
		t.Fatalf("seal after a failed write: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	for _, r := range res {
		if _, err := s.Job(ctx, r.ID); !errors.Is(err, driver.ErrNotFound) {
			t.Fatalf("rolled back job %d: %v", r.ID, err)
		}
	}
	if _, err := s.Batch(ctx, bid); !errors.Is(err, driver.ErrNotFound) {
		t.Fatalf("rolled back batch: %v", err)
	}
	if r := record(t, s, parent); len(r.Children) != 0 {
		t.Fatalf("rolled back child still linked: %v", r.Children)
	}
	if again := insert(t, s, job("a", unique("rolled back")))[0]; again.Duplicate {
		t.Fatalf("key still held after rollback: %+v", again)
	}
}

func TestTxWriterLeavesFinishUnblocked(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	held := insert(t, s, job("a", unique("held"), limited("k", 1)))[0]
	j := claim(t, s, 1)[0]
	tx := begin(t, s)
	w := s.Tx(tx)
	for _, p := range []driver.InsertParams{job("a", unique("held")), job("b", limited("k", 1))} {
		if _, err := w.Insert(ctx, []driver.InsertParams{p}); err != nil {
			t.Fatal(err)
		}
	}
	soon, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	rs, err := s.Finish(soon, "srv", []driver.Outcome{{Ref: j.Ref, State: driver.Succeeded}})
	if err != nil || rs[0] != driver.Applied {
		t.Fatalf("finish beside an open transaction: %v %v", rs, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if res := insert(t, s, job("a", unique("held")))[0]; res.Duplicate && res.ID == held.ID {
		t.Fatalf("key still held by finished job %d", held.ID)
	}
}

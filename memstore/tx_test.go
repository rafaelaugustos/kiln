package memstore_test

import (
	"errors"
	"testing"

	"github.com/rafaelaugustos/kiln/driver"
)

func TestTxIsolation(t *testing.T) {
	t.Parallel()
	s, _ := open(t)
	tx := s.Begin()
	res := insert(t, tx, params("a", unique("k", 0)))
	if _, err := s.Job(ctx, res[0].ID); !errors.Is(err, driver.ErrNotFound) {
		t.Fatalf("uncommitted job visible: %v", err)
	}
	if js := claim(t, s, 10); len(js) != 0 {
		t.Fatalf("uncommitted job claimed")
	}
	if again := insert(t, tx, params("a", unique("k", 0)))[0]; !again.Duplicate || again.ID != res[0].ID || again.State != driver.Enqueued {
		t.Fatalf("tx does not see its own key: %+v", again)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err == nil {
		t.Fatalf("second commit succeeded")
	}
	if _, err := tx.Insert(ctx, []driver.InsertParams{params("a")}); err == nil {
		t.Fatalf("insert after commit succeeded")
	}
	expectState(t, s, res[0].ID, driver.Enqueued)
	if dup := insert(t, s, params("a", unique("k", 0)))[0]; !dup.Duplicate || dup.ID != res[0].ID {
		t.Fatalf("committed key not held: %+v", dup)
	}
}

func TestTxRollback(t *testing.T) {
	t.Parallel()
	s, _ := open(t)
	tx := s.Begin()
	res := insert(t, tx, params("a", unique("k", 0)))
	if dup := insert(t, s, params("a", unique("k", 0)))[0]; !dup.Duplicate {
		t.Fatalf("reserved key not held")
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Job(ctx, res[0].ID); !errors.Is(err, driver.ErrNotFound) {
		t.Fatalf("rolled back job visible: %v", err)
	}
	if r := insert(t, s, params("a", unique("k", 0)))[0]; r.Duplicate {
		t.Fatalf("rollback kept the key")
	}

	boom := errors.New("boom")
	err := s.InTx(ctx, func(w driver.Writer) error {
		if _, err := w.Insert(ctx, []driver.InsertParams{params("b", queue("b"))}); err != nil {
			return err
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("InTx = %v", err)
	}
	if js := claim(t, s, 10, "b"); len(js) != 0 {
		t.Fatalf("failed InTx committed %d jobs", len(js))
	}
}

func TestTxBatch(t *testing.T) {
	t.Parallel()
	s, _ := open(t)
	var id, then int64
	err := s.InTx(ctx, func(w driver.Writer) error {
		var err error
		if id, err = w.OpenBatch(ctx, driver.NewBatch{Description: "tx"}); err != nil {
			return err
		}
		res, err := w.Insert(ctx, []driver.InsertParams{
			params("a", inBatch(id)),
			params("a", inBatch(id)),
			params("then", queue("then"), afterBatch(id)),
		})
		if err != nil {
			return err
		}
		then = res[2].ID
		return w.SealBatch(ctx, id)
	})
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.Batch(ctx, id)
	if err != nil || !b.Sealed || !b.FinishedAt.IsZero() || b.Total != 2 {
		t.Fatalf("batch after commit = %+v, %v", b, err)
	}
	expectState(t, s, then, driver.Awaiting)
	for _, j := range claim(t, s, 2) {
		finish(t, s, done(j, driver.Succeeded))
	}
	expectState(t, s, then, driver.Enqueued)

	err = s.InTx(ctx, func(w driver.Writer) error {
		_, err := w.Insert(ctx, []driver.InsertParams{params("late", inBatch(id))})
		return err
	})
	if !errors.Is(err, driver.ErrClosed) {
		t.Fatalf("insert into finished batch in tx: %v", err)
	}
}

func TestTxDoesNotAdmit(t *testing.T) {
	t.Parallel()
	s, _ := open(t)
	tx := s.Begin()
	res := insert(t, tx, params("a", limited("k", 1)))
	if res[0].State != driver.Throttled {
		t.Fatalf("tx insert state = %s", res[0].State)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	expectState(t, s, res[0].ID, driver.Throttled)
	if n, err := s.Sweep(ctx, 100); err != nil || n == 0 {
		t.Fatalf("sweep = %d, %v", n, err)
	}
	expectState(t, s, res[0].ID, driver.Enqueued)
}

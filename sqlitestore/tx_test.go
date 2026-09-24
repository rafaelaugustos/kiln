package sqlitestore

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

func limited(key string, n int) func(*driver.InsertParams) {
	return func(p *driver.InsertParams) { p.LimitKey, p.LimitMax = key, n }
}

func after(on driver.Mask, ids ...int64) func(*driver.InsertParams) {
	return func(p *driver.InsertParams) {
		for _, id := range ids {
			p.Parents = append(p.Parents, driver.Parent{ID: id, On: on})
		}
	}
}

func queue(name string) func(*driver.InsertParams) {
	return func(p *driver.InsertParams) { p.Queue = name }
}

func userTx(t *testing.T, s *Store) *sql.Tx {
	t.Helper()
	tx, err := s.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tx.Rollback() })
	return tx
}

func count(t *testing.T, s *Store, query string) int {
	t.Helper()
	var n int
	if err := s.db.QueryRow(query).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
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

func TestTxWriterRollback(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	parent := insert(t, s, job("p"))[0].ID
	tx := userTx(t, s)
	w := s.Tx(tx)
	b, err := w.OpenBatch(ctx, driver.NewBatch{Description: "gone"})
	if err != nil {
		t.Fatal(err)
	}
	res, err := w.Insert(ctx, []driver.InsertParams{
		job("a", unique("rolled back")),
		job("a", limited("tx", 1), func(p *driver.InsertParams) { p.BatchID = b }),
		job("c", after(driver.OnSucceeded, parent)),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	for _, r := range res {
		if _, err := s.Job(ctx, r.ID); !errors.Is(err, driver.ErrNotFound) {
			t.Fatalf("rolled back job %d: %v", r.ID, err)
		}
	}
	if _, err := s.Batch(ctx, b); !errors.Is(err, driver.ErrNotFound) {
		t.Fatalf("rolled back batch: %v", err)
	}
	if r := record(t, s, parent); len(r.Children) != 0 {
		t.Fatalf("rolled back child still linked: %v", r.Children)
	}
	if again := insert(t, s, job("a", unique("rolled back")))[0]; again.Duplicate {
		t.Fatalf("key still held after rollback: %+v", again)
	}
	if n := count(t, s, "SELECT COUNT(*) FROM kiln_limits"); n != 0 {
		t.Fatalf("%d limits outlived the rollback", n)
	}
}

func TestTxWriterCommit(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	e := listen(t, s)
	tx := userTx(t, s)
	w := s.Tx(tx)
	res, err := w.Insert(ctx, []driver.InsertParams{job("a", queue("tx")), job("a", limited("tx", 1)), job("a", limited("tx", 1))})
	if err != nil {
		t.Fatal(err)
	}
	if res[0].State != driver.Enqueued || res[1].State != driver.Enqueued || res[2].State != driver.Throttled {
		t.Fatalf("inserted %+v, want the first limited job admitted inside the transaction", res)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if e.has(driver.Event{Kind: driver.JobsReady}) {
		t.Fatalf("events %+v published before Notify", e.all())
	}
	if err := w.Notify(ctx); err != nil {
		t.Fatal(err)
	}
	e.wait(t, driver.Event{Kind: driver.JobsReady, Queue: "tx"})
	e.wait(t, driver.Event{Kind: driver.JobsReady, Queue: "default"})
	if got := claim(t, s, 10, "tx", "default"); len(got) != 2 {
		t.Fatalf("claimed %d after commit, want 2", len(got))
	}
	if r := record(t, s, res[2].ID); r.State != driver.Throttled {
		t.Fatalf("second limited job %s, want throttled", r.State)
	}
}

func TestTxWriterKeepsCallerTx(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	if _, err := s.db.Exec("CREATE TABLE orders (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	b, err := s.OpenBatch(ctx, driver.NewBatch{})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SealBatch(ctx, b); err != nil {
		t.Fatal(err)
	}
	tx := userTx(t, s)
	w := s.Tx(tx)
	if _, err := tx.Exec("INSERT INTO orders (id) VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	first, err := w.Insert(ctx, []driver.InsertParams{job("a", unique("kept"))})
	if err != nil {
		t.Fatal(err)
	}
	_, err = w.Insert(ctx, []driver.InsertParams{job("b", unique("undone")), job("b", func(p *driver.InsertParams) { p.BatchID = b })})
	if !errors.Is(err, driver.ErrClosed) {
		t.Fatalf("insert into a finished batch: %v", err)
	}
	_, err = w.Insert(ctx, []driver.InsertParams{job("c", after(driver.OnSucceeded, 1<<40))})
	if !errors.Is(err, driver.ErrNotFound) {
		t.Fatalf("insert after a missing parent: %v", err)
	}
	second, err := w.Insert(ctx, []driver.InsertParams{job("d")})
	if err != nil {
		t.Fatalf("insert after failed inserts: %v", err)
	}
	if _, err := tx.Exec("INSERT INTO orders (id) VALUES (2)"); err != nil {
		t.Fatalf("caller transaction unusable: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if n := count(t, s, "SELECT COUNT(*) FROM orders"); n != 2 {
		t.Fatalf("%d orders, want 2", n)
	}
	if n := count(t, s, "SELECT COUNT(*) FROM kiln_jobs"); n != 2 {
		t.Fatalf("%d jobs, want 2", n)
	}
	record(t, s, first[0].ID)
	record(t, s, second[0].ID)
	if again := insert(t, s, job("b", unique("undone")))[0]; again.Duplicate {
		t.Fatalf("key of a failed insert is held: %+v", again)
	}
	if second[0].ID <= first[0].ID {
		t.Fatalf("ids %d then %d", first[0].ID, second[0].ID)
	}
}

func TestInTxDiscardsFailedInsert(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	e := listen(t, s)
	var kept int64
	err := s.InTx(ctx, func(w driver.Writer) error {
		res, err := w.Insert(ctx, []driver.InsertParams{job("a", queue("intx"))})
		if err != nil {
			return err
		}
		kept = res[0].ID
		_, err = w.Insert(ctx, []driver.InsertParams{job("b", unique("partial")), job("b", after(driver.OnSucceeded, 1<<40))})
		if !errors.Is(err, driver.ErrNotFound) {
			t.Errorf("insert with a missing parent: %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	e.wait(t, driver.Event{Kind: driver.JobsReady, Queue: "intx"})
	if n := count(t, s, "SELECT COUNT(*) FROM kiln_jobs"); n != 1 {
		t.Fatalf("%d jobs, want only %d", n, kept)
	}
	if again := insert(t, s, job("b", unique("partial")))[0]; again.Duplicate {
		t.Fatalf("key of a failed insert is held: %+v", again)
	}
}

func TestTxWriterAfterRollback(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	tx := userTx(t, s)
	w := s.Tx(tx)
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Insert(ctx, []driver.InsertParams{job("a")}); !errors.Is(err, sql.ErrTxDone) {
		t.Fatalf("insert into a finished transaction: %v", err)
	}
}

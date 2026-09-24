package mysqlstore

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

func unique(key string, window time.Duration) func(*driver.InsertParams) {
	return func(p *driver.InsertParams) {
		h := sha256.Sum256([]byte(key))
		p.UniqueKey, p.UniqueFor = h[:16], window
	}
}

func limited(key string, n int) func(*driver.InsertParams) {
	return func(p *driver.InsertParams) { p.LimitKey, p.LimitMax = key, n }
}

func inBatch(id int64) func(*driver.InsertParams) {
	return func(p *driver.InsertParams) { p.BatchID = id }
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

func finishSoon(t *testing.T, s *Store, outs ...driver.Outcome) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	rs, err := s.Finish(ctx, "srv", outs)
	if err != nil {
		t.Fatalf("finish behind an open transaction: %v", err)
	}
	for i, r := range rs {
		if r != driver.Applied {
			t.Fatalf("outcome %d: %v, want applied", i, r)
		}
	}
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
	tx := begin(t, s)
	w := s.Tx(tx)
	b, err := w.OpenBatch(ctx, driver.NewBatch{Description: "gone"})
	if err != nil {
		t.Fatal(err)
	}
	res, err := w.Insert(ctx, []driver.InsertParams{
		job("a", unique("rolled back", 0)),
		job("a", limited("tx", 1), inBatch(b)),
		job("c", after(driver.OnSucceeded, parent)),
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := claim(t, s, 10); len(got) != 0 {
		t.Fatalf("claimed %v while the transaction is open and holds the parent", got)
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
	if got := claim(t, s, 10); len(got) != 1 || got[0].ID != parent {
		t.Fatalf("claimed %v after rollback, want the parent", got)
	}
	if again := insert(t, s, job("a", unique("rolled back", 0)))[0]; again.Duplicate {
		t.Fatalf("key still held after rollback: %+v", again)
	}
}

func TestTxWriterCommit(t *testing.T) {
	t.Parallel()
	s := open(t)
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
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sweep(ctx, 100); err != nil {
		t.Fatal(err)
	}
	if got := claim(t, s, 10); len(got) != 2 {
		t.Fatalf("claimed %d after commit and sweep, want 2", len(got))
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
}

func TestTxDuplicateLeavesUniqueUnlocked(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	held := insert(t, s, job("a", unique("held", 0)))[0]
	j := claim(t, s, 1)[0]
	parent := insert(t, s, job("p"))[0].ID
	tx := begin(t, s)
	w := s.Tx(tx)
	for _, p := range []driver.InsertParams{job("a", unique("held", 0)), job("a", unique("held", 0), after(driver.OnSucceeded, parent))} {
		res, err := w.Insert(ctx, []driver.InsertParams{p})
		if err != nil || !res[0].Duplicate || res[0].ID != held.ID {
			t.Fatalf("duplicate %+v %v", res, err)
		}
	}
	finishSoon(t, s, driver.Outcome{Ref: j.Ref, State: driver.Succeeded})
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if res := insert(t, s, job("a", unique("held", 0)))[0]; res.Duplicate {
		t.Fatalf("key still held after release: %+v", res)
	}
}

func TestTxInsertLeavesLimitsUnlocked(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	insert(t, s, job("a", limited("a", 1)), job("a", limited("b", 1)))
	jobs := claim(t, s, 2)
	if len(jobs) != 2 {
		t.Fatalf("claimed %d, want 2", len(jobs))
	}
	tx := begin(t, s)
	if _, err := s.Tx(tx).Insert(ctx, []driver.InsertParams{job("a", limited("a", 2)), job("a", limited("b", 4))}); err != nil {
		t.Fatal(err)
	}
	finishSoon(t, s, driver.Outcome{Ref: jobs[0].Ref, State: driver.Succeeded}, driver.Outcome{Ref: jobs[1].Ref, State: driver.Succeeded})
	soon, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if _, err := s.Insert(soon, []driver.InsertParams{job("a", limited("b", 4)), job("a", limited("a", 2))}); err != nil {
		t.Fatalf("insert behind an open transaction: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sweep(ctx, 100); err != nil {
		t.Fatal(err)
	}
	if got := claim(t, s, 10); len(got) != 4 {
		t.Fatalf("claimed %d after commit and sweep, want 4", len(got))
	}
}

func TestTxAttachLeavesFinishUnblocked(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	b, err := s.OpenBatch(ctx, driver.NewBatch{})
	if err != nil {
		t.Fatal(err)
	}
	cont := insert(t, s, job("m", inBatch(b)), job("after", func(p *driver.InsertParams) { p.AfterBatch = b }))[1]
	if err := s.SealBatch(ctx, b); err != nil {
		t.Fatal(err)
	}
	j := claim(t, s, 1)[0]
	tx := begin(t, s)
	if _, err := s.Tx(tx).Insert(ctx, []driver.InsertParams{job("m", inBatch(b))}); err != nil {
		t.Fatal(err)
	}
	finishSoon(t, s, driver.Outcome{Ref: j.Ref, State: driver.Succeeded})
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sweep(ctx, 100); err != nil {
		t.Fatal(err)
	}
	if got, err := s.Batch(ctx, b); err != nil || got.FinishedAt.IsZero() {
		t.Fatalf("batch after sweep %+v %v", got, err)
	}
	if r := record(t, s, cont.ID); r.State != driver.Enqueued {
		t.Fatalf("continuation %s", r.State)
	}
}

func TestChildInsertHoldsParent(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	parent := insert(t, s, job("p"))[0].ID
	j := claim(t, s, 1)[0]
	tx := begin(t, s)
	child, err := s.Tx(tx).Insert(ctx, []driver.InsertParams{job("c", after(driver.OnSucceeded, parent))})
	if err != nil {
		t.Fatal(err)
	}
	if rs := finish(t, s, driver.Outcome{Ref: j.Ref, State: driver.Succeeded}); rs[0] != driver.Busy {
		t.Fatalf("finish of a parent while a child registers: %v, want busy", rs[0])
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if rs := finish(t, s, driver.Outcome{Ref: j.Ref, State: driver.Succeeded}); rs[0] != driver.Applied {
		t.Fatalf("finish after commit: %v", rs[0])
	}
	if r := record(t, s, child[0].ID); r.State != driver.Enqueued {
		t.Fatalf("child %s, want enqueued", r.State)
	}
}

func keyOf(name string) []byte {
	h := sha256.Sum256([]byte(name))
	return h[:16]
}

func blockOn(t *testing.T, s *Store, held, wanted string) <-chan error {
	t.Helper()
	ctx := context.Background()
	other, err := s.db.BeginTx(ctx, readCommitted)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := other.ExecContext(ctx, render("INSERT INTO kiln_uniques (unique_key, job_id) VALUES (?, 0)", keyOf(held))); err != nil {
		t.Fatal(err)
	}
	heavy := []byte("INSERT INTO kiln_queues (name, paused, updated_at) VALUES ")
	for i := range 200 {
		if i > 0 {
			heavy = append(heavy, ',')
		}
		heavy = appendSQL(heavy, "(?, FALSE, UTC_TIMESTAMP(6))", fmt.Sprintf("q%d", i))
	}
	if _, err := other.ExecContext(ctx, string(heavy)); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		var id int64
		err := other.QueryRowContext(ctx, render("SELECT job_id FROM kiln_uniques WHERE unique_key = ? FOR UPDATE", keyOf(wanted))).Scan(&id)
		if errors.Is(err, sql.ErrNoRows) {
			err = nil
		}
		other.Rollback()
		done <- err
	}()
	time.Sleep(300 * time.Millisecond)
	return done
}

func TestTxDeadlockAbortsWriter(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	tx := begin(t, s)
	w := s.Tx(tx)
	if _, err := w.Insert(ctx, []driver.InsertParams{job("a", unique("a", 0))}); err != nil {
		t.Fatal(err)
	}
	done := blockOn(t, s, "b", "a")
	_, err := w.Insert(ctx, []driver.InsertParams{job("b", unique("b", 0))})
	if me := mysqlError(err); me == nil || me.Number != errDeadlock {
		t.Fatalf("insert into a deadlock: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, err := w.Insert(ctx, []driver.InsertParams{job("late")}); err == nil {
		t.Fatal("insert after the transaction was rolled back succeeded")
	}
	if _, err := w.OpenBatch(ctx, driver.NewBatch{}); err == nil {
		t.Fatal("open batch after the transaction was rolled back succeeded")
	}
	tx.Rollback()
	if n := count(t, s, "SELECT COUNT(*) FROM kiln_jobs"); n != 0 {
		t.Fatalf("%d jobs outlived the rolled back transaction", n)
	}
	if n := count(t, s, "SELECT COUNT(*) FROM kiln_batches"); n != 0 {
		t.Fatalf("%d batches outlived the rolled back transaction", n)
	}
}

func TestInTxRetriesDeadlock(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	var (
		attempts int
		done     <-chan error
		ids      []int64
	)
	err := s.InTx(ctx, func(w driver.Writer) error {
		attempts++
		ids = ids[:0]
		for _, name := range []string{"a", "b"} {
			if attempts == 1 && name == "b" {
				done = blockOn(t, s, "b", "a")
			}
			res, err := w.Insert(ctx, []driver.InsertParams{job(name, unique(name, 0))})
			if err != nil {
				return err
			}
			ids = append(ids, res[0].ID)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("InTx after %d attempts: %v", attempts, err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if attempts != 2 {
		t.Fatalf("%d attempts, want a retry after the deadlock", attempts)
	}
	for _, id := range ids {
		if r := record(t, s, id); r.State != driver.Enqueued {
			t.Fatalf("job %d %s", id, r.State)
		}
	}
}

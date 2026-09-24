package pgstore

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/rafaelaugustos/kiln/driver"
)

func blocked(t *testing.T, s *Store, tx pgx.Tx) {
	t.Helper()
	ctx := context.Background()
	var pid int32
	if err := tx.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pid); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		var waiting bool
		err := s.pool.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE $1::int = ANY(pg_blocking_pids(pid)))", pid).Scan(&waiting)
		if err != nil {
			t.Fatal(err)
		}
		if waiting {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("nothing waits on the open transaction")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func begin(t *testing.T, s *Store) pgx.Tx {
	t.Helper()
	ctx := context.Background()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tx.Rollback(ctx) })
	if _, err := tx.Exec(ctx, "SET LOCAL lock_timeout = '3s'"); err != nil {
		t.Fatal(err)
	}
	return tx
}

func TestFinishLocksUniquesInKeyOrder(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	insert(t, s, job("a", unique("b-key", 0)), job("a", unique("a-key", 0)))
	jobs := claim(t, s, 2)
	if len(jobs) != 2 {
		t.Fatalf("claimed %d, want 2", len(jobs))
	}
	tx := begin(t, s)
	lock := "SELECT 1 FROM " + s.schema + ".uniques WHERE key = $1 FOR UPDATE"
	if _, err := tx.Exec(ctx, lock, []byte("a-key")); err != nil {
		t.Fatal(err)
	}
	var rs []driver.Result
	done := make(chan error, 1)
	go func() {
		var err error
		rs, err = s.Finish(ctx, "srv", []driver.Outcome{
			{Ref: jobs[0].Ref, State: driver.Succeeded},
			{Ref: jobs[1].Ref, State: driver.Succeeded},
		})
		done <- err
	}()
	blocked(t, s, tx)
	if _, err := tx.Exec(ctx, lock, []byte("b-key")); err != nil {
		t.Fatalf("lock behind a waiting finish: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatalf("finish: %v", err)
	}
	if rs[0] != driver.Applied || rs[1] != driver.Applied {
		t.Fatalf("results %v, want both applied", rs)
	}
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
	w := s.Tx(tx)
	if _, err := w.Insert(ctx, []driver.InsertParams{job("a", limited("a", 2)), job("a", limited("b", 4))}); err != nil {
		t.Fatal(err)
	}
	finishSoon(t, s, driver.Outcome{Ref: jobs[0].Ref, State: driver.Succeeded}, driver.Outcome{Ref: jobs[1].Ref, State: driver.Succeeded})
	soon, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if _, err := s.Insert(soon, []driver.InsertParams{job("a", limited("b", 3)), job("a", limited("a", 3))}); err != nil {
		t.Fatalf("insert behind an open transaction: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := w.Notify(ctx); err != nil {
		t.Fatal(err)
	}
	rows, err := s.pool.Query(ctx, "SELECT key, max FROM "+s.schema+".limits ORDER BY key")
	if err != nil {
		t.Fatal(err)
	}
	type row struct {
		Key string
		Max int
	}
	got, err := pgx.CollectRows(rows, pgx.RowToStructByPos[row])
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != (row{"a", 2}) || got[1] != (row{"b", 4}) {
		t.Fatalf("limits %+v, want the committed transaction's maxes", got)
	}
	if got := claim(t, s, 10); len(got) != 4 {
		t.Fatalf("claimed %d after notify, want 4", len(got))
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
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if res := insert(t, s, job("a", unique("held", 0)))[0]; res.Duplicate {
		t.Fatalf("key still held after release: %+v", res)
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
	if err := tx.Rollback(ctx); err != nil {
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

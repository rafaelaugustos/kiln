package mysqlstore

import (
	"context"
	"database/sql"
	"testing"

	"github.com/rafaelaugustos/kiln/driver"
)

func TestSweepRestoresMissingLimit(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	res := insert(t, s, job("a", limited("gone", 1)), job("a", limited("gone", 1)))
	if res[0].State != driver.Enqueued || res[1].State != driver.Throttled {
		t.Fatalf("inserted %+v", res)
	}
	if _, err := s.db.Exec("DELETE FROM kiln_limits"); err != nil {
		t.Fatal(err)
	}
	j := claim(t, s, 1)[0]
	finish(t, s, driver.Outcome{Ref: j.Ref, State: driver.Succeeded})
	if _, err := s.Sweep(ctx, 100); err != nil {
		t.Fatal(err)
	}
	if r := record(t, s, res[1].ID); r.State != driver.Enqueued {
		t.Fatalf("throttled job %s after sweep, want enqueued", r.State)
	}
	insert(t, s, job("a", limited("gone", 3)), job("a", limited("gone", 3)))
	if got := claim(t, s, 10); len(got) != 3 {
		t.Fatalf("claimed %d after the limit was declared again, want 3", len(got))
	}
}

func TestInsertRestoresVanishedLimit(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	in := &inserter{s: s, own: true, jobs: []driver.InsertParams{job("a", limited("vanished", 2))}}
	in.plan()
	if err := in.allocate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.declare(ctx, in.limits, in.maxes, false); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Prune(ctx, keepAll); err != nil {
		t.Fatal(err)
	}
	if n := count(t, s, "SELECT COUNT(*) FROM kiln_limits"); n != 0 {
		t.Fatalf("prune kept %d unused limits", n)
	}
	if err := s.txn(ctx, func(tx *sql.Tx) error { return in.write(ctx, tx) }); err != nil {
		t.Fatal(err)
	}
	if n := count(t, s, "SELECT max FROM kiln_limits WHERE limit_key = 'vanished'"); n != 2 {
		t.Fatalf("limit max %d, want 2", n)
	}
	if in.res[0].State != driver.Enqueued || len(in.late) != 0 {
		t.Fatalf("inserted %+v, late %v", in.res, in.late)
	}
}

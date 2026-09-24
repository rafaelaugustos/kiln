package mysqlstore

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

func TestMigrate(t *testing.T) {
	t.Parallel()
	db := database(t)
	ctx := context.Background()
	if _, err := New(ctx, db, NoMigrate()); err == nil || !strings.Contains(err.Error(), "run mysqlstore.Migrate") {
		t.Fatalf("no-migrate on an empty database: %v", err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for range 4 {
		wg.Go(func() {
			errs <- Migrate(ctx, db)
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := Migrate(ctx, db); err != nil {
		t.Fatalf("second migrate: %v", err)
	}
	s, err := New(ctx, db, NoMigrate())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Insert(ctx, []driver.InsertParams{job("a")}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO kiln_migrations (version, applied_at) VALUES (9999, UTC_TIMESTAMP(6))"); err != nil {
		t.Fatal(err)
	}
	if _, err := New(ctx, db); err == nil || !strings.Contains(err.Error(), "9999") {
		t.Fatalf("newer schema accepted: %v", err)
	}
	for _, bad := range []string{"Bad-", "1x_", "a;b", strings.Repeat("p", 33)} {
		if _, err := New(ctx, db, Prefix(bad)); !errors.Is(err, driver.ErrInvalid) {
			t.Fatalf("prefix %q: %v", bad, err)
		}
	}
}

func TestUpgradeFromV020(t *testing.T) {
	t.Parallel()
	db := database(t)
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var name string
	if err := conn.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&name); err != nil {
		t.Fatal(err)
	}
	err = upgrade(ctx, conn, "kiln_", name)
	conn.Close()
	if err != nil {
		t.Fatal(err)
	}
	v020 := []string{
		`INSERT INTO kiln_limits (limit_key, max, declared_at) VALUES ('legacy', 1, NULL) AS n
ON DUPLICATE KEY UPDATE max = n.max, declared_at = COALESCE(n.declared_at, kiln_limits.declared_at)`,
		`INSERT INTO kiln_jobs (id, state, queue, kind, priority, max_attempts, timeout_ms, deps_pending, run_at, created_at,
	batch_id, after_batch, parents, recurring_id, unique_key, limit_key, args, meta, tags)
VALUES (1, 'throttled', 'default', 'old', 0, 3, 0, 0, UTC_TIMESTAMP(6), UTC_TIMESTAMP(6), NULL, NULL, NULL, NULL, NULL, 'legacy', '{}', NULL, NULL),
	(2, 'throttled', 'default', 'old', 0, 3, 0, 0, UTC_TIMESTAMP(6), UTC_TIMESTAMP(6), NULL, NULL, NULL, NULL, NULL, 'legacy', '{}', NULL, NULL)`,
		`UPDATE kiln_jobs SET state = 'processing', attempt = 1, claim = 1, attempted_at = UTC_TIMESTAMP(6), server = 'srv' WHERE id = 1`,
		`UPDATE kiln_limits SET active = 1 WHERE limit_key = 'legacy'`,
		`UPDATE kiln_sequences SET last_id = LAST_INSERT_ID(last_id + 2) WHERE name = 'jobs'`,
	}
	for _, q := range v020 {
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatalf("%v\n%s", err, q)
		}
	}
	if _, err := New(ctx, db, NoMigrate()); err == nil || !strings.Contains(err.Error(), changes[0].name) ||
		!strings.Contains(err.Error(), "run mysqlstore.Migrate") {
		t.Fatalf("no-migrate on a v0.2.0 database: %v", err)
	}
	s, err := New(ctx, db)
	if err != nil {
		t.Fatalf("upgrade of a v0.2.0 database: %v", err)
	}
	t.Cleanup(s.Close)
	if n := count(t, s, "SELECT COUNT(*) FROM kiln_migrations WHERE version = 1"); n != 1 || latest() != 1 {
		t.Fatalf("%d rows for version 1, latest %d: the versioned schema must stay where v0.1 and v0.2 accept it", n, latest())
	}
	if n := count(t, s, "SELECT COUNT(*) FROM kiln_migrations"); n != 1 {
		t.Fatalf("%d versioned migrations, want only the one v0.2.0 applied", n)
	}
	if n := count(t, s, "SELECT COUNT(*) FROM kiln_schema_changes WHERE name = '001_rate_limits'"); n != 1 {
		t.Fatalf("change recorded %d times", n)
	}
	legacy := "SELECT COUNT(*) FROM kiln_limits WHERE limit_key = 'legacy' AND max = 1 AND active = 1 AND rate = 0 AND per_us = 0 AND burst = 0 AND tat IS NULL"
	if n := count(t, s, legacy); n != 1 {
		t.Fatal("the v0.2.0 limit row did not get the rate defaults")
	}
	if n := count(t, s, "SELECT COUNT(*) FROM kiln_jobs WHERE NOT granted"); n != 2 {
		t.Fatalf("%d v0.2.0 jobs without a grant, want 2", n)
	}

	finish(t, s, driver.Outcome{Ref: driver.Ref{ID: 1, Claim: 1}, State: driver.Succeeded})
	if r := record(t, s, 2); r.State != driver.Enqueued {
		t.Fatalf("v0.2.0 job waiting on a mutex is %s after its holder finished, want enqueued", r.State)
	}
	ids := insertedIDs(insert(t, s, job("a", rated("api", 20, time.Second, 1)), job("a", rated("api", 20, time.Second, 1)),
		job("a", rated("api", 20, time.Second, 1))))
	a, b := record(t, s, ids[1]), record(t, s, ids[2])
	if st := record(t, s, ids[0]).State; st != driver.Enqueued || a.State != driver.Scheduled || b.State != driver.Scheduled {
		t.Fatalf("rated jobs %s, %s, %s on the upgraded schema", st, a.State, b.State)
	}
	if gap := b.RunAt.Sub(a.RunAt); gap != 50*time.Millisecond {
		t.Fatalf("reserved slots %v apart, want 50ms", gap)
	}

	old := []string{
		`INSERT INTO kiln_limits (limit_key, max, declared_at) VALUES ('api', 4, NULL) AS n
ON DUPLICATE KEY UPDATE max = n.max, declared_at = COALESCE(n.declared_at, kiln_limits.declared_at)`,
		`INSERT INTO kiln_jobs (id, state, queue, kind, priority, max_attempts, timeout_ms, deps_pending, run_at, created_at,
	batch_id, after_batch, parents, recurring_id, unique_key, limit_key, args, meta, tags)
VALUES (1000, 'enqueued', 'default', 'old', 0, 3, 0, 0, UTC_TIMESTAMP(6), UTC_TIMESTAMP(6), NULL, NULL, NULL, NULL, NULL, NULL, '{}', NULL, NULL)`,
	}
	for _, q := range old {
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatalf("a v0.2.0 statement fails on the upgraded schema: %v\n%s", err, q)
		}
	}
	kept := render("SELECT COUNT(*) FROM kiln_limits WHERE limit_key = 'api' AND max = 4 AND rate = 20 AND per_us = 1000000 AND tat = ?",
		b.RunAt.Add(50*time.Millisecond))
	if n := count(t, s, kept); n != 1 {
		t.Fatal("a v0.2.0 declare lost the rate or the reservations of a key")
	}

	if _, err := db.ExecContext(ctx, "DELETE FROM kiln_schema_changes"); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(ctx, db); err != nil {
		t.Fatalf("migrate over a change that was applied but not recorded: %v", err)
	}
	if _, err := New(ctx, db, NoMigrate()); err != nil {
		t.Fatal(err)
	}
}

func TestPrefixes(t *testing.T) {
	t.Parallel()
	db := database(t)
	ctx := context.Background()
	a, err := New(ctx, db, Prefix("a_"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := New(ctx, db, Prefix(""))
	if err != nil {
		t.Fatal(err)
	}
	insert(t, a, job("x"), job("x"))
	insert(t, b, job("y"))
	ca, err := a.Counts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cb, err := b.Counts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if ca.Enqueued != 2 || cb.Enqueued != 1 {
		t.Fatalf("counts %+v and %+v leak across prefixes", ca, cb)
	}
	if js := claim(t, b, 10); len(js) != 1 || js[0].Kind != "y" {
		t.Fatalf("claimed %+v", js)
	}
}

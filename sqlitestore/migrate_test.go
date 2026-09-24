package sqlitestore

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

func TestNewNeedsBusyTimeout(t *testing.T) {
	t.Parallel()
	db, err := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "bare.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = New(context.Background(), db)
	if !errors.Is(err, driver.ErrInvalid) || !strings.Contains(err.Error(), "busy_timeout") {
		t.Fatalf("new without busy_timeout: %v", err)
	}
}

func TestNewSetsWAL(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "wal.db")
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := New(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	other := connect(t, path)
	var mode string
	if err := other.QueryRow("PRAGMA journal_mode").Scan(&mode); err != nil || mode != "wal" {
		t.Fatalf("journal mode %q %v, want wal on every connection", mode, err)
	}
}

func TestNewRejectsMemory(t *testing.T) {
	t.Parallel()
	db, err := sql.Open("sqlite", "file::memory:?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := New(context.Background(), db); !errors.Is(err, driver.ErrInvalid) || !strings.Contains(err.Error(), "WAL") {
		t.Fatalf("new on an in-memory database: %v", err)
	}
}

func TestMigrate(t *testing.T) {
	t.Parallel()
	db := database(t)
	ctx := context.Background()
	if _, err := New(ctx, db, NoMigrate()); err == nil || !strings.Contains(err.Error(), "run sqlitestore.Migrate") {
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
	insert(t, s, job("a"))
	if _, err := db.Exec("INSERT INTO kiln_migrations (version, applied_at) VALUES (9999, 0)"); err != nil {
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

func v020(t *testing.T, db *sql.DB) {
	t.Helper()
	r := strings.NewReplacer("{p}", "kiln_", "{now}", clock)
	stmts := []string{"CREATE TABLE kiln_migrations (version INTEGER PRIMARY KEY, applied_at INTEGER NOT NULL)"}
	for _, m := range migrations {
		if m.version <= 1 {
			for _, stmt := range m.stmts {
				stmts = append(stmts, r.Replace(stmt))
			}
		}
	}
	stmts = append(stmts,
		"INSERT INTO kiln_migrations (version, applied_at) VALUES (1, 0)",
		"INSERT INTO kiln_limits (limit_key, max, active) VALUES ('mutex', 1, 1)",
		`INSERT INTO kiln_jobs (id, state, queue, kind, max_attempts, run_at, created_at, limit_key, args)
		VALUES (1, 'enqueued', 'default', 'old', 3, 0, 0, 'mutex', '{}'), (2, 'throttled', 'default', 'old', 3, 0, 0, 'mutex', '{}')`,
		"UPDATE kiln_sequences SET last_id = 2 WHERE name = 'jobs'",
	)
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
}

func TestUpgradeFromV020(t *testing.T) {
	t.Parallel()
	db := database(t)
	ctx := context.Background()
	v020(t, db)
	if _, err := New(ctx, db, NoMigrate()); err == nil || !strings.Contains(err.Error(), "001_rate_limits: run sqlitestore.Migrate") {
		t.Fatalf("no-migrate on a v0.2.0 file: %v", err)
	}
	s, err := New(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	if err := Migrate(ctx, db); err != nil {
		t.Fatalf("second migrate: %v", err)
	}
	if _, err := New(ctx, db, NoMigrate()); err != nil {
		t.Fatalf("no-migrate after the upgrade: %v", err)
	}
	if v := count(t, s, "SELECT max(version) FROM kiln_migrations"); v != 1 {
		t.Fatalf("migrations at version %d, v0.2.0 refuses to start above 1", v)
	}
	if n := count(t, s, "SELECT count(*) FROM kiln_schema_changes WHERE name = '001_rate_limits'"); n != 1 {
		t.Fatalf("change recorded %d times", n)
	}
	if n := count(t, s, "SELECT count(*) FROM kiln_limits WHERE max = 1 AND rate = 0 AND tat IS NULL"); n != 1 {
		t.Fatal("v0.2.0 limit row did not keep its rule")
	}

	js := claim(t, s, 10)
	if len(js) != 1 || js[0].ID != 1 {
		t.Fatalf("claimed %v, want the enqueued v0.2.0 job only", js)
	}
	finish(t, s, driver.Outcome{Ref: js[0].Ref, State: driver.Succeeded})
	if st := record(t, s, 2).State; st != driver.Enqueued {
		t.Fatalf("v0.2.0 throttled job is %s after the holder finished, want enqueued", st)
	}

	paced := job("new", rated("paced", 1, time.Minute, 1))
	res := insert(t, s, paced, paced)
	if res[0].ID != 3 || res[0].State != driver.Enqueued || res[1].State != driver.Scheduled {
		t.Fatalf("inserted %+v, want ids after the v0.2.0 jobs and one reserved slot", res)
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
	if a.w != b.w {
		t.Fatal("two stores on one *sql.DB do not share the write lock")
	}
}

func TestNewRejectsSingleConnection(t *testing.T) {
	t.Parallel()
	db := database(t)
	db.SetMaxOpenConns(1)
	if _, err := New(context.Background(), db); !errors.Is(err, driver.ErrInvalid) {
		t.Fatalf("new on a one-connection pool: %v", err)
	}
}

func TestWriterReconnects(t *testing.T) {
	t.Parallel()
	s := open(t)
	insert(t, s, job("a"))
	s.w.conn.Close()
	insert(t, s, job("b"))
	if js := claim(t, s, 10); len(js) != 2 {
		t.Fatalf("claimed %d jobs, want 2", len(js))
	}
}

func TestCloseReleasesWriter(t *testing.T) {
	t.Parallel()
	db := database(t)
	ctx := context.Background()
	a, err := New(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	b, err := New(ctx, db, Prefix("b_"))
	if err != nil {
		t.Fatal(err)
	}
	insert(t, a, job("a"))
	a.Close()
	insert(t, b, job("b"))
	if n := db.Stats().InUse; n != 1 {
		t.Fatalf("%d connections in use with one open store, want the writer", n)
	}
	b.Close()
	if n := db.Stats().InUse; n != 0 {
		t.Fatalf("%d connections in use after closing every store", n)
	}
}

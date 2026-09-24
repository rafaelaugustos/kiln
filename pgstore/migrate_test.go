package pgstore

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rafaelaugustos/kiln/driver"
)

const v020 = 1

func TestMigrate(t *testing.T) {
	t.Parallel()
	pool := connect(t)
	defer pool.Close()
	ctx := context.Background()
	schema := fmt.Sprintf("km_%x", rand.Uint64())
	defer pool.Exec(ctx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE")

	if _, err := New(ctx, pool, Schema(schema), NoMigrate()); err == nil || !strings.Contains(err.Error(), "run pgstore.Migrate") {
		t.Fatalf("no-migrate on empty schema: %v", err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for range 4 {
		wg.Go(func() {
			errs <- Migrate(ctx, pool, Schema(schema))
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := Migrate(ctx, pool, Schema(schema)); err != nil {
		t.Fatalf("second migrate: %v", err)
	}
	var applied int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+schema+".schema_changes").Scan(&applied); err != nil {
		t.Fatal(err)
	}
	if applied != len(changes) {
		t.Fatalf("%d schema changes recorded, want %d", applied, len(changes))
	}
	s, err := New(ctx, pool, Schema(schema), NoMigrate())
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	if _, err := pool.Exec(ctx, "INSERT INTO "+schema+".migrations (version) VALUES (9999)"); err != nil {
		t.Fatal(err)
	}
	if _, err := New(ctx, pool, Schema(schema)); err == nil || !strings.Contains(err.Error(), "9999") {
		t.Fatalf("newer schema accepted: %v", err)
	}
	if _, err := New(ctx, pool, Schema("Bad-Name")); err == nil {
		t.Fatal("invalid schema accepted")
	}
}

func TestNewWithoutCreatePrivilege(t *testing.T) {
	t.Parallel()
	pool := connect(t)
	defer pool.Close()
	ctx := context.Background()
	schema := fmt.Sprintf("kp_%x", rand.Uint64())
	role := schema + "_app"
	if err := Migrate(ctx, pool, Schema(schema)); err != nil {
		t.Fatal(err)
	}
	defer pool.Exec(ctx, fmt.Sprintf("DROP SCHEMA %s CASCADE; DROP OWNED BY %s; DROP ROLE %[2]s", schema, role))
	grants := fmt.Sprintf(`CREATE ROLE %[2]s LOGIN PASSWORD 'app';
GRANT USAGE ON SCHEMA %[1]s TO %[2]s;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA %[1]s TO %[2]s;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA %[1]s TO %[2]s`, schema, role)
	if _, err := pool.Exec(ctx, grants); err != nil {
		t.Fatal(err)
	}
	cfg, err := pgxpool.ParseConfig(dbURL())
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.User, cfg.ConnConfig.Password = role, "app"
	app, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()
	s, err := New(ctx, app, Schema(schema))
	if err != nil {
		t.Fatalf("current schema with a least-privilege role: %v", err)
	}
	defer s.Close()
	if _, err := s.Insert(ctx, []driver.InsertParams{job("a")}); err != nil {
		t.Fatal(err)
	}
}

func TestUpgradeFromV020(t *testing.T) {
	t.Parallel()
	pool := connect(t)
	defer pool.Close()
	ctx := context.Background()
	schema := fmt.Sprintf("ku_%x", rand.Uint64())
	defer pool.Exec(ctx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE")
	exec := func(sql string) {
		t.Helper()
		if _, err := pool.Exec(ctx, strings.ReplaceAll(sql, "{s}", schema)); err != nil {
			t.Fatal(err)
		}
	}
	for _, m := range migrations {
		if m.version > v020 {
			break
		}
		exec(`CREATE SCHEMA IF NOT EXISTS {s};
CREATE TABLE IF NOT EXISTS {s}.migrations (version int PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now());
` + m.sql + fmt.Sprintf(";\nINSERT INTO {s}.migrations (version) VALUES (%d)", m.version))
	}
	exec(`INSERT INTO {s}.limits (key, max, active) VALUES ('legacy', 1, 1);
INSERT INTO {s}.jobs (state, queue, kind, max_attempts, run_at, limit_key, args, attempt, claim, server, attempted_at) VALUES
	('processing', 'default', 'old', 3, now(), 'legacy', '{}', 1, 1, 'v020', now()),
	('throttled', 'default', 'old', 3, now(), 'legacy', '{}', 0, 0, NULL, NULL)`)
	files := func() []uint32 {
		t.Helper()
		var ids []uint32
		q := "SELECT ARRAY[pg_relation_filenode($1 || '.jobs'), pg_relation_filenode($1 || '.limits')]::int8[]"
		if err := pool.QueryRow(ctx, q, schema).Scan(&ids); err != nil {
			t.Fatal(err)
		}
		return ids
	}
	before := files()

	_, err := New(ctx, pool, Schema(schema), NoMigrate())
	if err == nil || !strings.Contains(err.Error(), "change "+changes[0].name) || !strings.Contains(err.Error(), "run pgstore.Migrate") {
		t.Fatalf("no-migrate on a v0.2.0 schema: %v", err)
	}
	s, err := New(ctx, pool, Schema(schema))
	if err != nil {
		t.Fatalf("upgrade a v0.2.0 schema: %v", err)
	}
	defer s.Close()
	if after := files(); !slices.Equal(after, before) {
		t.Fatalf("the change rewrote jobs or limits: files %v, then %v", before, after)
	}
	var (
		version, missing int
		applied          []string
	)
	err = pool.QueryRow(ctx, strings.ReplaceAll(`SELECT (SELECT max(version) FROM {s}.migrations),
	(SELECT array_agg(name ORDER BY name) FROM {s}.schema_changes),
	(SELECT count(*) FROM pg_attribute WHERE attrelid IN ('{s}.jobs'::regclass, '{s}.limits'::regclass) AND atthasmissing)`, "{s}", schema)).
		Scan(&version, &applied, &missing)
	if err != nil {
		t.Fatal(err)
	}
	if version != v020 || len(applied) != len(changes) || missing != 4 {
		t.Fatalf("migrations at %d, changes %v, %d columns with a stored default, want %d, all changes and 4", version, applied, missing, v020)
	}

	wantLimit(t, s, "legacy", 1, 1)
	if rs := finish(t, s, driver.Outcome{Ref: driver.Ref{ID: 1, Claim: 1}, State: driver.Succeeded}); rs[0] != driver.Applied {
		t.Fatalf("finish the job a v0.2 server claimed: %v", rs)
	}
	if js := claim(t, s, 10); len(js) != 1 || js[0].ID != 2 {
		t.Fatalf("claimed %v after the legacy mutex released, want job 2", js)
	}
	wantLimit(t, s, "legacy", 1, 1)
	limitAdmission(t, s)

	again, err := New(ctx, pool, Schema(schema), NoMigrate())
	if err != nil {
		t.Fatalf("no-migrate after the upgrade: %v", err)
	}
	again.Close()
}

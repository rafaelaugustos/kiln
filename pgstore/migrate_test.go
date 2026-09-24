package pgstore

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rafaelaugustos/kiln/driver"
)

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

package mssqlstore

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/rafaelaugustos/kiln/driver"
)

func TestMigrate(t *testing.T) {
	t.Parallel()
	db, prefix := fresh(t)
	ctx := context.Background()
	if _, err := New(ctx, db, Prefix(prefix), NoMigrate()); err == nil || !strings.Contains(err.Error(), "run mssqlstore.Migrate") {
		t.Fatalf("no-migrate on an empty schema: %v", err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for range 4 {
		wg.Go(func() {
			errs <- Migrate(ctx, db, Prefix(prefix))
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := Migrate(ctx, db, Prefix(prefix)); err != nil {
		t.Fatalf("second migrate: %v", err)
	}
	var applied int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+prefix+"migrations").Scan(&applied); err != nil || applied != latest() {
		t.Fatalf("%d migrations recorded, want %d: %v", applied, latest(), err)
	}
	s, err := New(ctx, db, Prefix(prefix), NoMigrate())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Insert(ctx, []driver.InsertParams{job("a")}); err != nil {
		t.Fatal(err)
	}
	newer := "INSERT INTO " + prefix + "migrations (version, applied_at) VALUES (9999, SYSUTCDATETIME())"
	if _, err := db.ExecContext(ctx, newer); err != nil {
		t.Fatal(err)
	}
	for _, opts := range [][]Option{{Prefix(prefix)}, {Prefix(prefix), NoMigrate()}} {
		if _, err := New(ctx, db, opts...); err == nil || !strings.Contains(err.Error(), "9999") {
			t.Fatalf("newer schema accepted: %v", err)
		}
	}
	for _, bad := range []string{"Bad-", "1x_", "a;b", strings.Repeat("p", 33)} {
		if _, err := New(ctx, db, Prefix(bad)); !errors.Is(err, driver.ErrInvalid) {
			t.Fatalf("prefix %q: %v", bad, err)
		}
	}
	single, err := sql.Open("sqlserver", dsn())
	if err != nil {
		t.Fatal(err)
	}
	defer single.Close()
	single.SetMaxOpenConns(1)
	if _, err := New(ctx, single, Prefix(prefix)); !errors.Is(err, driver.ErrInvalid) {
		t.Fatalf("store on a single connection: %v", err)
	}
}

func TestPrefixes(t *testing.T) {
	t.Parallel()
	a, b := open(t), open(t)
	insert(t, a, job("x"), job("x"))
	insert(t, b, job("y"))
	ctx := context.Background()
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

package mysqlstore

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

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
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- Migrate(ctx, db)
		}()
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

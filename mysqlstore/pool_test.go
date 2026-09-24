package mysqlstore

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

func TestTxSmallPool(t *testing.T) {
	t.Parallel()
	s := open(t)
	s.db.SetMaxOpenConns(2)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	errs := make(chan error, 4)
	for i := range 3 {
		go func() {
			errs <- s.InTx(ctx, func(w driver.Writer) error {
				time.Sleep(100 * time.Millisecond)
				_, err := w.Insert(ctx, []driver.InsertParams{job("a", limited(fmt.Sprint("k", i), 1))})
				return err
			})
		}()
	}
	go func() {
		time.Sleep(50 * time.Millisecond)
		_, err := s.Insert(ctx, []driver.InsertParams{job("b")})
		errs <- err
	}()
	for range 4 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	if n := count(t, s, "SELECT COUNT(*) FROM kiln_jobs"); n != 4 {
		t.Fatalf("%d jobs, want 4", n)
	}
}

func TestSideConnectionRecovers(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	var id int64
	if err := s.side.conn.QueryRowContext(ctx, "SELECT CONNECTION_ID()").Scan(&id); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(render("KILL ?", id)); err != nil {
		t.Fatal(err)
	}
	insert(t, s, job("a"))
	if _, err := s.side.conn.ExecContext(ctx, "SET SESSION wait_timeout = 1"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2500 * time.Millisecond)
	insert(t, s, job("a"))
	tx := begin(t, s)
	if _, err := s.Tx(tx).Insert(ctx, []driver.InsertParams{job("a", limited("k", 1))}); err != nil {
		t.Fatal(err)
	}
}

func TestSingleConnectionPool(t *testing.T) {
	t.Parallel()
	db := database(t)
	db.SetMaxOpenConns(1)
	if _, err := New(context.Background(), db); !errors.Is(err, driver.ErrInvalid) {
		t.Fatalf("new on a one-connection pool: %v", err)
	}
}

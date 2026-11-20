package pgstore_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/jackc/pgx/v5/stdlib"
	"github.com/rafaelaugustos/kiln/driver"
	"github.com/rafaelaugustos/kiln/drivertest"
	"github.com/rafaelaugustos/kiln/pgstore"
)

func TestOutbox(t *testing.T) {
	drivertest.RunOutbox(t, func(t *testing.T, target driver.Stager) drivertest.OutboxHarness {
		pool := pgstore.Connect(t)
		schema := fmt.Sprintf("ko_%x", rand.Uint64())
		t.Cleanup(func() {
			if _, err := pool.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+schema+" CASCADE"); err != nil {
				t.Errorf("drop schema: %v", err)
			}
			pool.Close()
		})
		ob, err := pgstore.NewOutbox(context.Background(), pool, target, pgstore.Schema(schema))
		if err != nil {
			t.Fatal(err)
		}
		return drivertest.OutboxHarness{
			Outbox: ob,
			Begin: func(t *testing.T) (driver.TxWriter, func(), func()) {
				ctx := context.Background()
				tx, err := pool.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { tx.Rollback(ctx) })
				commit := func() {
					if err := tx.Commit(ctx); err != nil {
						t.Fatalf("commit: %v", err)
					}
				}
				rollback := func() {
					if err := tx.Rollback(ctx); err != nil {
						t.Fatalf("rollback: %v", err)
					}
				}
				return ob.Tx(tx), commit, rollback
			},
		}
	})
}

func TestOutboxSQLTx(t *testing.T) {
	drivertest.RunOutbox(t, func(t *testing.T, target driver.Stager) drivertest.OutboxHarness {
		pool := pgstore.Connect(t)
		db := stdlib.OpenDBFromPool(pool)
		schema := fmt.Sprintf("ko_%x", rand.Uint64())
		t.Cleanup(func() {
			db.Close()
			if _, err := pool.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+schema+" CASCADE"); err != nil {
				t.Errorf("drop schema: %v", err)
			}
			pool.Close()
		})
		ob, err := pgstore.NewOutbox(context.Background(), pool, target, pgstore.Schema(schema))
		if err != nil {
			t.Fatal(err)
		}
		return drivertest.OutboxHarness{
			Outbox: ob,
			Begin: func(t *testing.T) (driver.TxWriter, func(), func()) {
				tx, err := db.BeginTx(context.Background(), nil)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { tx.Rollback() })
				commit := func() {
					if err := tx.Commit(); err != nil {
						t.Fatalf("commit: %v", err)
					}
				}
				rollback := func() {
					if err := tx.Rollback(); err != nil {
						t.Fatalf("rollback: %v", err)
					}
				}
				return ob.SQLTx(tx), commit, rollback
			},
		}
	})
}

func TestOutboxNilTx(t *testing.T) {
	pool := pgstore.Connect(t)
	defer pool.Close()
	ob, err := pgstore.NewOutbox(context.Background(), pool, drivertest.NewRecorder(), pgstore.NoMigrate())
	if err != nil {
		t.Fatal(err)
	}
	for _, w := range []driver.TxWriter{ob.Tx(nil), ob.SQLTx(nil)} {
		if _, err := w.Insert(context.Background(), []driver.InsertParams{{Kind: "k", Queue: "q", Args: []byte(`{}`), MaxAttempts: 1}}); err != driver.ErrNilTx {
			t.Fatalf("insert with a nil tx: %v", err)
		}
	}
}

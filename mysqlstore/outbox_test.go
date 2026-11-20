package mysqlstore_test

import (
	"context"
	"testing"

	"github.com/rafaelaugustos/kiln/driver"
	"github.com/rafaelaugustos/kiln/drivertest"
	"github.com/rafaelaugustos/kiln/mysqlstore"
)

func TestOutbox(t *testing.T) {
	drivertest.RunOutbox(t, func(t *testing.T, target driver.Stager) drivertest.OutboxHarness {
		db := mysqlstore.Database(t)
		ob, err := mysqlstore.NewOutbox(context.Background(), db, target)
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
				return ob.Tx(tx), commit, rollback
			},
		}
	})
}

func TestOutboxNilTx(t *testing.T) {
	ob, err := mysqlstore.NewOutbox(context.Background(), mysqlstore.Database(t), drivertest.NewRecorder())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ob.Tx(nil).Insert(context.Background(), []driver.InsertParams{{Kind: "k", Queue: "q", Args: []byte(`{}`), MaxAttempts: 1}}); err != driver.ErrNilTx {
		t.Fatalf("insert with a nil tx: %v", err)
	}
}

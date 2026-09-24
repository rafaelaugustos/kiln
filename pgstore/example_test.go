package pgstore_test

import (
	"context"
	"log"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rafaelaugustos/kiln"
	"github.com/rafaelaugustos/kiln/pgstore"
)

type SendReceipt struct {
	OrderID int64
	Email   string
}

func (SendReceipt) Kind() string { return "send_receipt" }

func ExampleNew() {
	ctx := context.Background()

	pool, err := pgxpool.New(ctx, "postgres://kiln:kiln@localhost:5432/kiln?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	store, err := pgstore.New(ctx, pool)
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()

	client := kiln.NewClient(store)
	if _, err := client.Enqueue(ctx, SendReceipt{OrderID: 1, Email: "ada@example.com"}); err != nil {
		log.Fatal(err)
	}
}

func ExampleStore_Tx() {
	ctx := context.Background()

	pool, err := pgxpool.New(ctx, "postgres://kiln:kiln@localhost:5432/kiln?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	store, err := pgstore.New(ctx, pool)
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()

	client := kiln.NewClient(store)

	var w *pgstore.TxWriter
	var orderID int64
	err = pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		w = store.Tx(tx)
		if err := tx.QueryRow(ctx, "insert into orders(email) values($1) returning id", "ada@example.com").Scan(&orderID); err != nil {
			return err
		}
		_, err := client.EnqueueTx(ctx, w, SendReceipt{OrderID: orderID, Email: "ada@example.com"})
		return err
	})
	if err != nil {
		log.Fatal(err)
	}
	if err := w.Notify(ctx); err != nil {
		log.Fatal(err)
	}
}

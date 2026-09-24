package main

import (
	"context"
	"fmt"
	"log"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rafaelaugustos/kiln"
	"github.com/rafaelaugustos/kiln/examples/internal/setup"
	"github.com/rafaelaugustos/kiln/pgstore"
)

type SendReceipt struct {
	OrderID int64
	Email   string
}

func (SendReceipt) Kind() string { return "send_receipt" }

func main() {
	ctx := context.Background()

	pool, err := pgxpool.New(ctx, setup.DBURL())
	if err != nil {
		log.Fatalf("pool: %v", err)
	}
	defer pool.Close()

	store, err := pgstore.New(ctx, pool)
	if err != nil {
		log.Fatalf("store: %v", err)
	}
	defer store.Close()

	client := kiln.NewClient(store)

	orderID, err := placeOrder(ctx, pool, store, client, "ada@example.com")
	if err != nil {
		log.Fatalf("place order: %v", err)
	}
	fmt.Printf("order %d placed, receipt job enqueued in the same transaction\n", orderID)
}

func placeOrder(ctx context.Context, pool *pgxpool.Pool, store *pgstore.Store, client *kiln.Client, email string) (int64, error) {
	var orderID int64
	var w *pgstore.TxWriter
	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		w = store.Tx(tx)
		if _, err := tx.Exec(ctx, "create temp table if not exists orders(id bigserial primary key, email text not null)"); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, "insert into orders(email) values($1) returning id", email).Scan(&orderID); err != nil {
			return err
		}
		_, err := client.EnqueueTx(ctx, w, SendReceipt{OrderID: orderID, Email: email})
		return err
	})
	if err != nil {
		return 0, err
	}
	if err := w.Notify(ctx); err != nil {
		log.Printf("notify: %v", err)
	}
	return orderID, nil
}

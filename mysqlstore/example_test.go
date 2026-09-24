package mysqlstore_test

import (
	"context"
	"database/sql"
	"log"

	"github.com/rafaelaugustos/kiln"
	"github.com/rafaelaugustos/kiln/mysqlstore"
)

type SendReceipt struct {
	OrderID int64
	Email   string
}

func (SendReceipt) Kind() string { return "send_receipt" }

func ExampleNew() {
	ctx := context.Background()

	db, err := sql.Open("mysql", "kiln:kiln@tcp(localhost:3306)/kiln")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	store, err := mysqlstore.New(ctx, db)
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

	db, err := sql.Open("mysql", "kiln:kiln@tcp(localhost:3306)/kiln")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	store, err := mysqlstore.New(ctx, db)
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()

	client := kiln.NewClient(store)

	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		log.Fatal(err)
	}
	defer tx.Rollback()

	res, err := tx.ExecContext(ctx, "insert into orders(email) values(?)", "ada@example.com")
	if err != nil {
		log.Fatal(err)
	}
	orderID, err := res.LastInsertId()
	if err != nil {
		log.Fatal(err)
	}

	w := store.Tx(tx)
	if _, err := client.EnqueueTx(ctx, w, SendReceipt{OrderID: orderID, Email: "ada@example.com"}); err != nil {
		log.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}
}

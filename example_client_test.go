package kiln_test

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/rafaelaugustos/kiln"
	"github.com/rafaelaugustos/kiln/memstore"
)

type ChargeCard struct {
	OrderID int64
}

func (ChargeCard) Kind() string { return "charge_card" }

type SendReceipt struct {
	OrderID int64
}

func (SendReceipt) Kind() string { return "send_receipt" }

type FetchData struct {
	URL string
}

func (FetchData) Kind() string { return "fetch_data" }

type MergeData struct{}

func (MergeData) Kind() string { return "merge_data" }

type ImportFile struct {
	Name string
}

func (ImportFile) Kind() string { return "import_file" }

type SendSummary struct{}

func (SendSummary) Kind() string { return "send_summary" }

type GenerateReport struct{}

func (GenerateReport) Kind() string { return "generate_report" }

func ExampleClient_Enqueue() {
	client := kiln.NewClient(memstore.New())
	ctx := context.Background()

	id := must(client.Enqueue(ctx, ChargeCard{OrderID: 42},
		kiln.Queue("payments"),
		kiln.Priority(10),
		kiln.Delay(time.Hour),
		kiln.MaxAttempts(3),
	))

	record := must(client.Get(ctx, id))
	fmt.Println(record.State, record.Queue)
	// Output:
	// scheduled payments
}

func ExampleClient_EnqueueTx() {
	store := memstore.New()
	client := kiln.NewClient(store)
	ctx := context.Background()

	tx := store.Begin()
	id := must(client.EnqueueTx(ctx, tx, SendReceipt{OrderID: 42}))
	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}

	record := must(client.Get(ctx, id))
	fmt.Println(record.State)
	// Output:
	// enqueued
}

func ExampleClient_EnqueueMany() {
	client := kiln.NewClient(memstore.New())
	ctx := context.Background()

	var flow kiln.Flow
	a := flow.Add(FetchData{URL: "https://example.com/a.csv"})
	b := flow.Add(FetchData{URL: "https://example.com/b.csv"})
	flow.Add(MergeData{}, kiln.Needs{a, b})

	inserted := must(client.EnqueueMany(ctx, flow...))
	for _, ins := range inserted {
		fmt.Println(ins.State)
	}
	// Output:
	// enqueued
	// enqueued
	// awaiting
}

func ExampleClient_StartBatch() {
	client := kiln.NewClient(memstore.New())
	ctx := context.Background()

	batch := &kiln.Batch{Description: "nightly-import"}
	batch.Add(ImportFile{Name: "a.csv"})
	batch.Add(ImportFile{Name: "b.csv"})
	batch.Then(SendSummary{})

	id := must(client.StartBatch(ctx, batch))
	info := must(client.Store().Batch(ctx, id))
	fmt.Println(info.Total, info.Sealed)
	// Output:
	// 2 true
}

func ExampleClient_SetRecurring() {
	client := kiln.NewClient(memstore.New())
	ctx := context.Background()

	err := client.SetRecurring(ctx, "daily-report", "0 9 * * *", GenerateReport{},
		kiln.TZ("America/Sao_Paulo"), kiln.MisfireSkip)
	if err != nil {
		log.Fatal(err)
	}

	r := must(client.Store().Recurring(ctx, "daily-report"))
	fmt.Println(r.Spec, r.NextRunAt.After(time.Now()))
	// Output:
	// 0 9 * * * true
}

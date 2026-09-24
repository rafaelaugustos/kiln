package kiln_test

import (
	"context"
	"fmt"
	"time"

	"github.com/rafaelaugustos/kiln"
	"github.com/rafaelaugustos/kiln/memstore"
)

type CallWebhook struct {
	URL string
}

func (CallWebhook) Kind() string { return "call_webhook" }

type ProcessData struct{}

func (ProcessData) Kind() string { return "process_data" }

func ExampleUnique() {
	client := kiln.NewClient(memstore.New())
	ctx := context.Background()

	unique := kiln.Unique{Key: "order-42", For: time.Hour}
	first := must(client.Enqueue(ctx, ChargeCard{OrderID: 42}, unique))
	second := must(client.Enqueue(ctx, ChargeCard{OrderID: 42}, unique))

	fmt.Println(first == second)
	// Output:
	// true
}

func ExampleLimit() {
	client := kiln.NewClient(memstore.New())
	ctx := context.Background()

	limit := kiln.Limit{Key: "webhooks", Max: 2}
	for range 3 {
		id := must(client.Enqueue(ctx, CallWebhook{URL: "https://hooks.example.com/notify"}, limit))
		record := must(client.Get(ctx, id))
		fmt.Println(record.State)
	}
	// Output:
	// enqueued
	// enqueued
	// throttled
}

func ExampleAfter() {
	client := kiln.NewClient(memstore.New())
	ctx := context.Background()

	fetchID := must(client.Enqueue(ctx, FetchData{URL: "https://example.com/data.csv"}))
	processID := must(client.Enqueue(ctx, ProcessData{}, kiln.After{fetchID}))

	record := must(client.Get(ctx, processID))
	fmt.Println(record.State)
	// Output:
	// awaiting
}

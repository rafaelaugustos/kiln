package main

import (
	"context"
	"fmt"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/rafaelaugustos/kiln"
	"github.com/rafaelaugustos/kiln/examples/internal/setup"
)

type CallWebhook struct{ URL string }

func (CallWebhook) Kind() string { return "call_webhook" }

type ChargeCard struct{ OrderID int64 }

func (ChargeCard) Kind() string { return "charge_card" }

func handleWebhook(ctx context.Context, j *kiln.Job[CallWebhook]) error {
	fmt.Printf("calling webhook %s\n", j.Args.URL)
	return nil
}

func handleCharge(ctx context.Context, j *kiln.Job[ChargeCard]) error {
	fmt.Printf("charging card for order %d\n", j.Args.OrderID)
	return nil
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	store, err := setup.Store(ctx)
	if err != nil {
		log.Fatalf("store: %v", err)
	}
	defer store.Close()

	client := kiln.NewClient(store)

	mux := kiln.NewMux()
	kiln.Handle(mux, handleWebhook)
	kiln.Handle(mux, handleCharge)

	server, err := kiln.NewServer(client, mux, kiln.ServerConfig{
		Queues: map[string]int{kiln.DefaultQueue: 5},
	})
	if err != nil {
		log.Fatalf("server: %v", err)
	}

	for range 20 {
		_, err := client.Enqueue(ctx, CallWebhook{URL: "https://hooks.example.com/notify"},
			kiln.Limit{Key: "webhooks", Max: 5})
		if err != nil {
			log.Fatalf("enqueue: %v", err)
		}
	}

	orderID := int64(42)
	unique := kiln.Unique{Key: fmt.Sprint(orderID), For: time.Hour}
	if _, err := client.Enqueue(ctx, ChargeCard{OrderID: orderID}, unique); err != nil {
		log.Fatalf("enqueue: %v", err)
	}
	if _, err := client.Enqueue(ctx, ChargeCard{OrderID: orderID}, unique); err != nil {
		log.Fatalf("enqueue: %v", err)
	}

	log.Println("throttling: server running, ctrl-c to stop")
	if err := server.Run(ctx); err != nil {
		log.Fatalf("run: %v", err)
	}
}

package main

import (
	"context"
	"fmt"
	"log"
	"os/signal"
	"syscall"

	"github.com/rafaelaugustos/kiln"
	"github.com/rafaelaugustos/kiln/examples/internal/setup"
)

type FetchData struct{ URL string }

func (FetchData) Kind() string { return "fetch_data" }

type ProcessData struct{}

func (ProcessData) Kind() string { return "process_data" }

type ArchiveData struct{}

func (ArchiveData) Kind() string { return "archive_data" }

type SendSummary struct{}

func (SendSummary) Kind() string { return "send_summary" }

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
	kiln.Handle(mux, handleFetch)
	kiln.Handle(mux, handleProcess)
	kiln.Handle(mux, handleArchive)
	kiln.Handle(mux, handleSummary)

	server, err := kiln.NewServer(client, mux, kiln.ServerConfig{
		Queues: map[string]int{kiln.DefaultQueue: 5},
	})
	if err != nil {
		log.Fatalf("server: %v", err)
	}

	if err := runWorkflows(ctx, client); err != nil {
		log.Fatalf("enqueue: %v", err)
	}

	log.Println("workflow: server running, ctrl-c to stop")
	if err := server.Run(ctx); err != nil {
		log.Fatalf("run: %v", err)
	}
}

func runWorkflows(ctx context.Context, client *kiln.Client) error {
	fetchID, err := client.Enqueue(ctx, FetchData{URL: "https://example.com/data.csv"})
	if err != nil {
		return fmt.Errorf("continuation: %w", err)
	}
	if _, err := client.Enqueue(ctx, ProcessData{}, kiln.After{fetchID}); err != nil {
		return fmt.Errorf("continuation: %w", err)
	}

	var flow kiln.Flow
	fetch := flow.Add(FetchData{URL: "https://example.com/other.csv"})
	process := flow.Add(ProcessData{}, kiln.Needs{fetch})
	flow.Add(ArchiveData{}, kiln.Needs{process})
	if _, err := client.EnqueueMany(ctx, flow...); err != nil {
		return fmt.Errorf("flow: %w", err)
	}

	batch := &kiln.Batch{Description: "nightly-import"}
	batch.Add(ProcessData{})
	batch.Add(ProcessData{})
	batch.Then(SendSummary{})
	if _, err := client.StartBatch(ctx, batch); err != nil {
		return fmt.Errorf("batch: %w", err)
	}
	return nil
}

func handleFetch(ctx context.Context, j *kiln.Job[FetchData]) error {
	fmt.Printf("fetching %s\n", j.Args.URL)
	return nil
}

func handleProcess(ctx context.Context, j *kiln.Job[ProcessData]) error {
	fmt.Println("processing data")
	return nil
}

func handleArchive(ctx context.Context, j *kiln.Job[ArchiveData]) error {
	fmt.Println("archiving data")
	return nil
}

func handleSummary(ctx context.Context, j *kiln.Job[SendSummary]) error {
	fmt.Println("sending summary")
	return nil
}

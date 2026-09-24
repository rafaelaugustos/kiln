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

type GenerateReport struct{}

func (GenerateReport) Kind() string { return "generate_report" }

func handleReport(ctx context.Context, j *kiln.Job[GenerateReport]) error {
	fmt.Println("generating daily report")
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
	kiln.Handle(mux, handleReport)

	server, err := kiln.NewServer(client, mux, kiln.ServerConfig{
		Queues: map[string]int{kiln.DefaultQueue: 5},
	})
	if err != nil {
		log.Fatalf("server: %v", err)
	}

	err = client.SetRecurring(ctx, "daily-report", "0 9 * * *", GenerateReport{},
		kiln.TZ("America/Sao_Paulo"), kiln.Misfire(kiln.MisfireSkip))
	if err != nil {
		log.Fatalf("set recurring: %v", err)
	}

	log.Println("recurring: server running, ctrl-c to stop")
	if err := server.Run(ctx); err != nil {
		log.Fatalf("run: %v", err)
	}
}

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

type WelcomeEmail struct {
	To string
}

func (WelcomeEmail) Kind() string { return "welcome_email" }

func sendWelcomeEmail(ctx context.Context, j *kiln.Job[WelcomeEmail]) error {
	fmt.Printf("sending welcome email to %s (attempt %d)\n", j.Args.To, j.Attempt)
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
	kiln.Handle(mux, sendWelcomeEmail)

	server, err := kiln.NewServer(client, mux, kiln.ServerConfig{
		Queues: map[string]int{kiln.DefaultQueue: 5},
	})
	if err != nil {
		log.Fatalf("server: %v", err)
	}

	if _, err := client.Enqueue(ctx, WelcomeEmail{To: "ada@example.com"}); err != nil {
		log.Fatalf("enqueue: %v", err)
	}

	log.Println("basic: server running, ctrl-c to stop")
	if err := server.Run(ctx); err != nil {
		log.Fatalf("run: %v", err)
	}
}

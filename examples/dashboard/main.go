package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os/signal"
	"syscall"

	"github.com/rafaelaugustos/kiln"
	"github.com/rafaelaugustos/kiln/dashboard"
	"github.com/rafaelaugustos/kiln/examples/internal/setup"
)

type Ping struct{}

func (Ping) Kind() string { return "ping" }

func handlePing(ctx context.Context, j *kiln.Job[Ping]) error { return nil }

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
	kiln.Handle(mux, handlePing)

	server, err := kiln.NewServer(client, mux, kiln.ServerConfig{
		Queues: map[string]int{kiln.DefaultQueue: 5},
	})
	if err != nil {
		log.Fatalf("server: %v", err)
	}

	dash := dashboard.New(client, dashboard.Options{
		Prefix: "/kiln",
		Authorize: func(r *http.Request) dashboard.Access {
			if r.Header.Get("Authorization") == "Bearer devtoken" {
				return dashboard.ReadWrite
			}
			return dashboard.Denied
		},
	})

	httpMux := http.NewServeMux()
	httpMux.Handle("/kiln/", dash)
	httpServer := &http.Server{Addr: ":8080", Handler: httpMux}

	go func() {
		<-ctx.Done()
		httpServer.Close()
	}()
	go func() {
		log.Println("dashboard: listening on :8080/kiln/")
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("http: %v", err)
		}
	}()

	if err := server.Run(ctx); err != nil {
		log.Fatalf("run: %v", err)
	}
}

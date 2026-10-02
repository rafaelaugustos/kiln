package kilnotel_test

import (
	"context"
	"log"
	"os"
	"os/signal"

	"github.com/rafaelaugustos/kiln"
	"github.com/rafaelaugustos/kiln/kilnotel"
	"github.com/rafaelaugustos/kiln/memstore"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

type SendReceipt struct {
	OrderID int64
}

func (SendReceipt) Kind() string { return "send_receipt" }

func sendReceipt(ctx context.Context, j *kiln.Job[SendReceipt]) error {
	log.Printf("sending receipt for order %d", j.Args.OrderID)
	return nil
}

func ExampleEnqueueMiddleware() {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	client := kiln.NewClient(memstore.New(), kilnotel.EnqueueMiddleware())

	ctx, span := otel.Tracer("shop").Start(context.Background(), "checkout")
	defer span.End()
	if _, err := client.Enqueue(ctx, SendReceipt{OrderID: 42}); err != nil {
		log.Fatal(err)
	}
}

func ExampleMiddleware() {
	store := memstore.New()
	client := kiln.NewClient(store, kilnotel.EnqueueMiddleware())

	mux := kiln.NewMux()
	mux.Use(kilnotel.Middleware())
	kiln.Handle(mux, sendReceipt)

	server, err := kiln.NewServer(client, mux, kiln.ServerConfig{})
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := server.Run(ctx); err != nil {
		log.Fatal(err)
	}
}

func ExampleObserve() {
	store := memstore.New()
	client := kiln.NewClient(store)

	mux := kiln.NewMux()
	kiln.Handle(mux, sendReceipt)

	server, err := kiln.NewServer(client, mux, kiln.ServerConfig{})
	if err != nil {
		log.Fatal(err)
	}
	unregister, err := kilnotel.Observe(server, store)
	if err != nil {
		log.Fatal(err)
	}
	defer unregister()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := server.Run(ctx); err != nil {
		log.Fatal(err)
	}
}

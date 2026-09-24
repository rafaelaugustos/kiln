package kiln_test

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/rafaelaugustos/kiln"
	"github.com/rafaelaugustos/kiln/memstore"
)

type SendEmail struct {
	To string
}

func (SendEmail) Kind() string { return "send_email" }

func sendEmail(ctx context.Context, j *kiln.Job[SendEmail]) error {
	log.Printf("sending email to %s", j.Args.To)
	return nil
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func waitSucceeded(ctx context.Context, c *kiln.Client, id int64) kiln.Record {
	deadline := time.Now().Add(2 * time.Second)
	for {
		r := must(c.Get(ctx, id))
		if r.State == kiln.Succeeded {
			return r
		}
		if time.Now().After(deadline) {
			panic("kiln: timed out waiting for job to succeed")
		}
		time.Sleep(time.Millisecond)
	}
}

func Example() {
	client := kiln.NewClient(memstore.New())

	mux := kiln.NewMux()
	kiln.Handle(mux, sendEmail)

	server := must(kiln.NewServer(client, mux, kiln.ServerConfig{
		Queues:       map[string]int{kiln.DefaultQueue: 5},
		PollInterval: 5 * time.Millisecond,
	}))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Run(ctx) }()

	id := must(client.Enqueue(ctx, SendEmail{To: "ada@example.com"}))
	record := waitSucceeded(ctx, client, id)

	cancel()
	<-done

	fmt.Println(record.Kind, record.State)
	// Output:
	// send_email succeeded
}

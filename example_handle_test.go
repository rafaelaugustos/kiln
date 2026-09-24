package kiln_test

import (
	"context"
	"fmt"
	"time"

	"github.com/rafaelaugustos/kiln"
	"github.com/rafaelaugustos/kiln/kilntest"
	"github.com/rafaelaugustos/kiln/memstore"
)

type ResizeImage struct {
	Path string
}

func (ResizeImage) Kind() string { return "resize_image" }

type NotifyOwner struct {
	Path string
}

func (NotifyOwner) Kind() string { return "notify_owner" }

func ExampleHandle() {
	mux := kiln.NewMux()
	kiln.Handle(mux, func(ctx context.Context, j *kiln.Job[ResizeImage]) error {
		if j.Args.Path == "" {
			return kiln.Permanent(fmt.Errorf("resize: no path"))
		}
		return kiln.Snooze(time.Minute)
	})

	bad := kilntest.Work(context.Background(), mux, ResizeImage{})
	fmt.Println(bad.State)

	later := kilntest.Work(context.Background(), mux, ResizeImage{Path: "photo.png"})
	fmt.Println(later.State, later.Delay)

	// Output:
	// failed
	// scheduled 1m0s
}

func ExampleMux_Use() {
	var trace []string
	mux := kiln.NewMux()
	mux.Use(func(next kiln.HandlerFunc) kiln.HandlerFunc {
		return func(ctx context.Context, j *kiln.RawJob) error {
			trace = append(trace, "start")
			err := next(ctx, j)
			trace = append(trace, "done")
			return err
		}
	})
	kiln.Handle(mux, func(ctx context.Context, j *kiln.Job[ResizeImage]) error {
		trace = append(trace, "handle")
		return nil
	})

	kilntest.Work(context.Background(), mux, ResizeImage{Path: "photo.png"})
	fmt.Println(trace)
	// Output:
	// [start handle done]
}

func ExampleJob_SetParam() {
	mux := kiln.NewMux()
	kiln.Handle(mux, func(ctx context.Context, j *kiln.Job[ResizeImage]) error {
		if err := j.SetParam(ctx, "width", 800); err != nil {
			return err
		}
		var width int
		if _, err := j.Param("width", &width); err != nil {
			return err
		}
		fmt.Println("width is now", width)
		return nil
	})

	kilntest.Work(context.Background(), mux, ResizeImage{Path: "photo.png"})
	// Output:
	// width is now 800
}

func ExampleJob_SetOutput() {
	mux := kiln.NewMux()
	kiln.Handle(mux, func(ctx context.Context, j *kiln.Job[ResizeImage]) error {
		return j.SetOutput(map[string]int{"width": 800, "height": 600})
	})

	result := kilntest.Work(context.Background(), mux, ResizeImage{Path: "photo.png"})
	fmt.Println(string(result.Output))
	// Output:
	// {"height":600,"width":800}
}

func ExampleClientFrom() {
	client := kiln.NewClient(memstore.New())

	mux := kiln.NewMux()
	kiln.Handle(mux, func(ctx context.Context, j *kiln.Job[NotifyOwner]) error {
		return nil
	})
	kiln.Handle(mux, func(ctx context.Context, j *kiln.Job[ResizeImage]) error {
		c, ok := kiln.ClientFrom(ctx)
		if !ok {
			return fmt.Errorf("resize: no client in context")
		}
		_, err := c.Enqueue(ctx, NotifyOwner{Path: j.Args.Path})
		return err
	})

	server := must(kiln.NewServer(client, mux, kiln.ServerConfig{
		Queues:       map[string]int{kiln.DefaultQueue: 5},
		PollInterval: 5 * time.Millisecond,
	}))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Run(ctx) }()

	must(client.Enqueue(ctx, ResizeImage{Path: "photo.png"}))

	deadline := time.Now().Add(2 * time.Second)
	var followUp kiln.Record
	for {
		page := must(client.List(ctx, kiln.JobQuery{State: kiln.Succeeded, Kind: "notify_owner"}))
		if len(page.Records) > 0 {
			followUp = page.Records[0]
			break
		}
		if time.Now().After(deadline) {
			panic("kiln: timed out waiting for follow-up job")
		}
		time.Sleep(time.Millisecond)
	}

	cancel()
	<-done

	fmt.Println(followUp.Kind, followUp.State)
	// Output:
	// notify_owner succeeded
}

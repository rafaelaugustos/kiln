package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rafaelaugustos/kiln"
	"github.com/rafaelaugustos/kiln/pgstore"
)

type kilnSystem struct {
	schema string
	pool   *pgxpool.Pool
	store  *pgstore.Store
	client *kiln.Client
	specs  []kiln.Spec
	server *kiln.Server
	cancel context.CancelFunc
	done   chan error
}

func (k *kilnSystem) open(ctx context.Context, url, schema string) error {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return err
	}
	store, err := pgstore.New(ctx, pool, pgstore.Schema(schema))
	if err != nil {
		pool.Close()
		return err
	}
	k.schema, k.pool, k.store = schema, pool, store
	k.client = kiln.NewClient(store)
	return nil
}

func (k *kilnSystem) close() {
	if k.cancel != nil {
		k.stop(context.Background())
	}
	k.store.Close()
	k.pool.Close()
}

func (k *kilnSystem) insertMany(ctx context.Context, n int) error {
	if len(k.specs) != n {
		k.specs = make([]kiln.Spec, n)
		for i := range k.specs {
			k.specs[i] = kiln.Spec{Args: job{}}
		}
	}
	_, err := k.client.EnqueueMany(ctx, k.specs...)
	return err
}

func (k *kilnSystem) insert(ctx context.Context, seq int) error {
	_, err := k.client.Enqueue(ctx, job{Seq: seq})
	return err
}

func (k *kilnSystem) start(ctx context.Context, workers int, h handler) error {
	m := kiln.NewMux()
	kiln.Handle(m, func(ctx context.Context, j *kiln.Job[job]) error {
		h(ctx, j.Args.Seq)
		return nil
	})
	srv, err := kiln.NewServer(k.client, m, kiln.ServerConfig{
		Queues: map[string]int{kiln.DefaultQueue: workers},
		Logger: logger,
	})
	if err != nil {
		return err
	}
	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	k.server, k.cancel, k.done = srv, cancel, make(chan error, 1)
	go func() { k.done <- srv.Run(runCtx) }()
	return nil
}

func (k *kilnSystem) ready(ctx context.Context) error {
	deadline := time.Now().Add(10 * time.Second)
	for {
		st := k.server.Stats()
		if st.Listening && k.server.Healthy() == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return errors.New("kiln server not listening after 10s")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func (k *kilnSystem) stop(ctx context.Context) error {
	if k.cancel == nil {
		return nil
	}
	k.cancel()
	k.cancel = nil
	select {
	case err := <-k.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(time.Minute):
		return fmt.Errorf("kiln server did not stop within a minute")
	}
}

func (k *kilnSystem) table() string {
	return k.schema + ".jobs"
}

func (k *kilnSystem) completed() string {
	return "SELECT count(*) FROM " + k.schema + ".archive WHERE state = 'succeeded'"
}

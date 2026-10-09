package mssqlstore

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln"
)

type double struct{ N int }

func (double) Kind() string { return "double" }

type flaky struct{}

func (flaky) Kind() string { return "flaky" }

type step struct{ N int }

func (step) Kind() string { return "step" }

func serve(t *testing.T, s *Store, m *kiln.Mux) {
	t.Helper()
	cfg := kiln.ServerConfig{
		Queues:            map[string]int{kiln.DefaultQueue: 8},
		PollInterval:      50 * time.Millisecond,
		FetchCooldown:     time.Millisecond,
		ShutdownTimeout:   time.Second,
		KillGrace:         100 * time.Millisecond,
		HeartbeatInterval: 100 * time.Millisecond,
		LeaderTTL:         time.Second,
		Backoff:           kiln.Constant(10 * time.Millisecond),
	}
	srv, err := kiln.NewServer(kiln.NewClient(s), m, cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("run: %v", err)
		}
	})
}

func waitState(t *testing.T, c *kiln.Client, id int64, st kiln.State) kiln.Record {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		r, err := c.Get(context.Background(), id)
		if err != nil {
			t.Fatalf("job %d: %v", id, err)
		}
		if r.State == st {
			return r
		}
		if time.Now().After(deadline) {
			t.Fatalf("job %d is %s, want %s", id, r.State, st)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestE2E(t *testing.T) {
	t.Parallel()
	s := open(t)
	var running, peak atomic.Int32
	m := kiln.NewMux()
	kiln.Handle(m, func(_ context.Context, j *kiln.Job[double]) error {
		return j.SetOutput(map[string]int{"n": j.Args.N * 2})
	})
	kiln.Handle(m, func(context.Context, *kiln.Job[flaky]) error { return errors.New("boom") })
	kiln.Handle(m, func(context.Context, *kiln.Job[step]) error {
		n := running.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		running.Add(-1)
		return nil
	})
	serve(t, s, m)
	c := kiln.NewClient(s)
	ctx := context.Background()

	id, err := c.Enqueue(ctx, double{N: 21})
	if err != nil {
		t.Fatal(err)
	}
	var out struct{ N int }
	if r := waitState(t, c, id, kiln.Succeeded); json.Unmarshal(r.Output, &out) != nil || out.N != 42 || r.Attempt != 1 {
		t.Fatalf("output %s after attempt %d", r.Output, r.Attempt)
	}

	id, err = c.Enqueue(ctx, flaky{}, kiln.MaxAttempts(2))
	if err != nil {
		t.Fatal(err)
	}
	r := waitState(t, c, id, kiln.Failed)
	var reasons []string
	for _, e := range r.History {
		if e.Reason != "lost" {
			reasons = append(reasons, e.Reason)
		}
	}
	if !slices.Equal(reasons, []string{"retry", "exhausted"}) || r.Attempt != 2 {
		t.Fatalf("history %v after attempt %d", reasons, r.Attempt)
	}

	tx := begin(t, s)
	id, err = c.EnqueueTx(ctx, s.Tx(tx), double{N: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	waitState(t, c, id, kiln.Succeeded)

	specs := make([]kiln.Spec, 8)
	for i := range specs {
		specs[i] = kiln.Spec{Args: step{N: i}, Options: []kiln.InsertOption{kiln.Limit{Key: "export", Max: 2}}}
	}
	res, err := c.EnqueueMany(ctx, specs...)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range res {
		waitState(t, c, r.ID, kiln.Succeeded)
	}
	if p := peak.Load(); p < 1 || p > 2 {
		t.Fatalf("peak concurrency %d under a limit of 2", p)
	}
}

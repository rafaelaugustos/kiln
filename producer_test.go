package kiln

import (
	"context"
	"encoding/json"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln/memstore"
)

func TestServerLimits(t *testing.T) {
	t.Parallel()
	c := NewClient(memstore.New())
	m := NewMux()
	var active, peak atomic.Int32
	m.HandleFunc("limited", func(context.Context, *RawJob) error {
		n := active.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(2 * time.Millisecond)
		active.Add(-1)
		return nil
	})
	cfg := fastConfig()
	cfg.Queues = map[string]int{DefaultQueue: 16}
	s, _ := runServer(t, c, m, cfg)
	ids := mustEnqueueMany(t, c, 60, testArgs{K: "limited"}, Limit{Key: "k", Max: 3})
	eventually(t, 5*time.Second, "limited jobs", func() bool { return s.Stats().Succeeded == uint64(len(ids)) })
	if p := peak.Load(); p > 3 || p < 1 {
		t.Fatalf("peak concurrency %d, limit 3", p)
	}
}

func TestServerFlows(t *testing.T) {
	t.Parallel()
	c := NewClient(memstore.New())
	m := NewMux()
	var mu sync.Mutex
	var order []int
	m.HandleFunc("step", func(_ context.Context, j *RawJob) error {
		mu.Lock()
		defer mu.Unlock()
		var a testArgs
		if err := json.Unmarshal(j.Args, &a); err != nil {
			return err
		}
		order = append(order, a.N)
		return nil
	})
	runServer(t, c, m, fastConfig())

	parent := mustEnqueue(t, c, testArgs{K: "step", N: 1}, Delay(30*time.Millisecond))
	var f Flow
	a := f.Add(testArgs{K: "step", N: 2}, After{parent})
	b := f.Add(testArgs{K: "step", N: 3}, Needs{a})
	f.Add(testArgs{K: "step", N: 4}, Needs{b})
	res, err := c.EnqueueMany(context.Background(), f...)
	if err != nil {
		t.Fatal(err)
	}
	waitState(t, c, res[2].ID, Succeeded)
	mu.Lock()
	defer mu.Unlock()
	if want := []int{1, 2, 3, 4}; !slices.Equal(order, want) {
		t.Fatalf("order = %v, want %v", order, want)
	}
}

func TestServerBatchThen(t *testing.T) {
	t.Parallel()
	c := NewClient(memstore.New())
	m := NewMux()
	var parts atomic.Int32
	done := make(chan int32, 1)
	m.HandleFunc("part", func(context.Context, *RawJob) error {
		time.Sleep(time.Millisecond)
		parts.Add(1)
		return nil
	})
	m.HandleFunc("done", func(context.Context, *RawJob) error {
		done <- parts.Load()
		return nil
	})
	runServer(t, c, m, fastConfig())
	var b Batch
	for i := range 5 {
		b.Add(testArgs{K: "part", N: i})
	}
	b.Then(testArgs{K: "done"})
	id, err := c.StartBatch(context.Background(), &b)
	if err != nil {
		t.Fatal(err)
	}
	if n := receive(t, done); n != 5 {
		t.Fatalf("continuation ran after %d of 5 parts", n)
	}
	eventually(t, time.Second, "batch finished", func() bool {
		bt, err := c.Store().Batch(context.Background(), id)
		return err == nil && !bt.FinishedAt.IsZero()
	})
}

func TestServerUnique(t *testing.T) {
	t.Parallel()
	c := NewClient(memstore.New())
	started := make(chan int64, 1)
	release := make(chan struct{})
	m := NewMux()
	m.HandleFunc("once", func(_ context.Context, j *RawJob) error {
		started <- j.ID
		<-release
		return nil
	})
	runServer(t, c, m, fastConfig())
	first := mustEnqueue(t, c, testArgs{K: "once"}, Unique{})
	receive(t, started)
	if dup := mustEnqueue(t, c, testArgs{K: "once"}, Unique{}); dup != first {
		t.Fatalf("duplicate got id %d, want %d", dup, first)
	}
	close(release)
	waitState(t, c, first, Succeeded)
	if next := mustEnqueue(t, c, testArgs{K: "once"}, Unique{}); next == first {
		t.Fatal("unique key still held after success")
	}
}

func BenchmarkServerThroughput(b *testing.B) {
	c := NewClient(memstore.New())
	m := NewMux()
	m.HandleFunc("noop", func(context.Context, *RawJob) error { return nil })
	for n := b.N; n > 0; n -= 1000 {
		mustEnqueueMany(b, c, min(n, 1000), testArgs{K: "noop"})
	}
	s, err := NewServer(c, m, ServerConfig{Queues: map[string]int{DefaultQueue: 100}})
	if err != nil {
		b.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	b.ResetTimer()
	start := time.Now()
	go func() { done <- s.Run(ctx) }()
	for s.Stats().Succeeded < uint64(b.N) {
		time.Sleep(100 * time.Microsecond)
	}
	elapsed := time.Since(start)
	b.StopTimer()
	cancel()
	if err := <-done; err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(b.N)/elapsed.Seconds(), "jobs/s")
}

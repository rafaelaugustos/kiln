package pgstore_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln"
)

type noop struct{}

func (noop) Kind() string { return "noop" }

func BenchmarkServerPG(b *testing.B) {
	b.Run("throughput", func(b *testing.B) {
		st := newCluster(b).store()
		cl := kiln.NewClient(st)
		ctx := context.Background()
		specs := make([]kiln.Spec, 0, 5000)
		for left := b.N; left > 0; left -= len(specs) {
			specs = specs[:0]
			for range min(left, cap(specs)) {
				specs = append(specs, kiln.Spec{Args: noop{}})
			}
			if _, err := cl.EnqueueMany(ctx, specs...); err != nil {
				b.Fatal(err)
			}
		}
		m := kiln.NewMux()
		kiln.Handle(m, func(context.Context, *kiln.Job[noop]) error { return nil })
		b.ResetTimer()
		srv, stop := serve(b, st, m, kiln.ServerConfig{Queues: map[string]int{kiln.DefaultQueue: 100}})
		waitFor(b, 10*time.Minute, "all jobs to succeed", func() bool { return srv.Stats().Succeeded >= uint64(b.N) })
		b.StopTimer()
		b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "jobs/s")
		stop()
	})
	b.Run("latency", func(b *testing.B) {
		st := newCluster(b).store()
		started := make(chan time.Time, 1)
		m := kiln.NewMux()
		kiln.Handle(m, func(context.Context, *kiln.Job[noop]) error {
			started <- time.Now()
			return nil
		})
		srv, _ := serve(b, st, m, kiln.ServerConfig{Queues: map[string]int{kiln.DefaultQueue: 10}})
		waitFor(b, 5*time.Second, "listen", func() bool { return srv.Stats().Listening })
		cl := kiln.NewClient(st)
		ctx := context.Background()
		lat := make([]time.Duration, b.N)
		b.ResetTimer()
		for i := range lat {
			b.StopTimer()
			time.Sleep(10 * time.Millisecond)
			b.StartTimer()
			t0 := time.Now()
			if _, err := cl.Enqueue(ctx, noop{}); err != nil {
				b.Fatal(err)
			}
			lat[i] = recv(b, started, 10*time.Second).Sub(t0)
		}
		b.StopTimer()
		slices.Sort(lat)
		ms := func(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
		b.ReportMetric(ms(lat[len(lat)/2]), "p50-ms")
		b.ReportMetric(ms(lat[len(lat)*99/100]), "p99-ms")
	})
}

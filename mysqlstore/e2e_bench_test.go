package mysqlstore_test

import (
	"context"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln"
)

type noop struct{}

func (noop) Kind() string { return "noop" }

func BenchmarkServerMySQL(b *testing.B) {
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
}

package kilnotel

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/rafaelaugustos/kiln"
	"github.com/rafaelaugustos/kiln/driver"
	"go.opentelemetry.io/otel/metric"
)

const queueTimeout = 5 * time.Second

type gauges struct {
	server    *kiln.Server
	inspector driver.Inspector
	running   metric.Int64ObservableGauge
	capacity  metric.Int64ObservableGauge
	pending   metric.Int64ObservableGauge
	jobs      metric.Int64ObservableGauge
	failed    metric.Int64ObservableGauge
}

// Observe registers gauges that are read each time metrics are collected:
//
//	kiln.server.running     jobs running on s
//	kiln.server.capacity    workers of s
//	kiln.completer.pending  finished jobs of s whose outcome is not stored yet
//	kiln.queue.jobs         jobs of each queue, by queue and state, from inspector
//	kiln.jobs.failed        jobs that failed for good, from inspector
//
// The queue gauge counts enqueued, processing, scheduled and throttled jobs across the whole
// store. The failed gauge counts the jobs waiting in failed for a requeue or a delete, the number
// to alert on. Both are read with a 5 second timeout per collection. With several servers on one store, pass the
// inspector on one of them only and nil on the others; a process that only enqueues can pass a nil
// server. Observe fails with [kiln.ErrInvalid] when both are nil. Call the returned function to
// unregister the gauges.
func Observe(s *kiln.Server, inspector driver.Inspector, opts ...Option) (func() error, error) {
	if s == nil && inspector == nil {
		return nil, fmt.Errorf("%w: Observe needs a server or an inspector", kiln.ErrInvalid)
	}
	meter := newConfig(opts).meterProvider.Meter(scope)
	running, err1 := meter.Int64ObservableGauge("kiln.server.running",
		metric.WithUnit("{job}"),
		metric.WithDescription("Jobs running on the server."),
	)
	capacity, err2 := meter.Int64ObservableGauge("kiln.server.capacity",
		metric.WithUnit("{job}"),
		metric.WithDescription("Jobs the server can run at once."),
	)
	pending, err3 := meter.Int64ObservableGauge("kiln.completer.pending",
		metric.WithUnit("{job}"),
		metric.WithDescription("Finished jobs whose outcome has not been written to the store yet."),
	)
	jobs, err4 := meter.Int64ObservableGauge("kiln.queue.jobs",
		metric.WithUnit("{job}"),
		metric.WithDescription("Jobs in each queue, by state."),
	)
	failed, err5 := meter.Int64ObservableGauge("kiln.jobs.failed",
		metric.WithUnit("{job}"),
		metric.WithDescription("Jobs that failed for good and wait for a requeue or a delete."),
	)
	if err := errors.Join(err1, err2, err3, err4, err5); err != nil {
		return nil, err
	}
	g := &gauges{server: s, inspector: inspector, running: running, capacity: capacity, pending: pending, jobs: jobs, failed: failed}
	reg, err := meter.RegisterCallback(g.observe, running, capacity, pending, jobs, failed)
	if err != nil {
		if reg != nil {
			err = errors.Join(err, reg.Unregister())
		}
		return nil, err
	}
	return reg.Unregister, nil
}

func (g *gauges) observe(ctx context.Context, o metric.Observer) error {
	if g.server != nil {
		st := g.server.Stats()
		o.ObserveInt64(g.running, int64(st.Running))
		o.ObserveInt64(g.capacity, int64(st.Capacity))
		o.ObserveInt64(g.pending, int64(st.Pending))
	}
	if g.inspector == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, queueTimeout)
	defer cancel()
	queues, err := g.inspector.Queues(ctx)
	if err != nil {
		return fmt.Errorf("kilnotel: queues: %w", err)
	}
	for _, q := range queues {
		g.observeQueue(o, q.Name, kiln.Enqueued, q.Enqueued)
		g.observeQueue(o, q.Name, kiln.Processing, q.Processing)
		g.observeQueue(o, q.Name, kiln.Scheduled, q.Scheduled)
		g.observeQueue(o, q.Name, kiln.Throttled, q.Throttled)
	}
	c, err := g.inspector.Counts(ctx)
	if err != nil {
		return fmt.Errorf("kilnotel: counts: %w", err)
	}
	o.ObserveInt64(g.failed, c.Failed)
	return nil
}

func (g *gauges) observeQueue(o metric.Observer, queue string, state kiln.State, n int64) {
	o.ObserveInt64(g.jobs, n, metric.WithAttributes(queueKey.String(queue), stateKey.String(string(state))))
}

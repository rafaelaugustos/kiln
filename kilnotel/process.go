package kilnotel

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/rafaelaugustos/kiln"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type outcome uint8

const (
	succeeded outcome = iota
	failed
	snoozed
	canceled
	interrupted
)

var outcomes = [...]string{
	succeeded:   "succeeded",
	failed:      "failed",
	snoozed:     "snoozed",
	canceled:    "canceled",
	interrupted: "interrupted",
}

var buckets = []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300, 900, 1800, 3600}

type processor struct {
	tracer     trace.Tracer
	propagator propagation.TextMapPropagator
	processed  metric.Int64Counter
	duration   metric.Float64Histogram
	delay      metric.Float64Histogram
	routes     sync.Map
}

type routeKey struct {
	kind, queue string
}

type route struct {
	spanName string
	started  metric.MeasurementOption
	ended    [len(outcomes)]metric.MeasurementOption
}

// Middleware returns server middleware that runs each attempt in a "kiln process <kind>" consumer
// span, the child of the enqueue span when the job's Meta carries its context, and records these
// metrics, by kind and queue:
//
//	kiln.jobs.processed  counter of attempts, also by outcome
//	kiln.job.duration    histogram of the time spent in the handler, in seconds, also by outcome
//	kiln.job.delay       histogram of the time from the job's RunAt to the handler's start, in seconds
//
// The outcome is succeeded, failed, snoozed, canceled or interrupted. An attempt kiln will retry
// counts as failed; canceled means the job was deleted while it ran or its handler returned an
// error from [kiln.Cancel], and interrupted that the server shut down and put the job back.
//
// Give Middleware to [kiln.Mux.Use] before any other middleware, so that the others run inside the
// span and the outcome matches what kiln records.
func Middleware(opts ...Option) kiln.Middleware {
	p := newProcessor(newConfig(opts))
	return func(next kiln.HandlerFunc) kiln.HandlerFunc {
		return func(ctx context.Context, j *kiln.RawJob) error {
			return p.process(ctx, j, next)
		}
	}
}

func newProcessor(c config) *processor {
	meter := c.meterProvider.Meter(scope)
	processed, err1 := meter.Int64Counter("kiln.jobs.processed",
		metric.WithUnit("{job}"),
		metric.WithDescription("Jobs run by a handler, by outcome."),
	)
	duration, err2 := meter.Float64Histogram("kiln.job.duration",
		metric.WithUnit("s"),
		metric.WithDescription("Time spent running a job."),
		metric.WithExplicitBucketBoundaries(buckets...),
	)
	delay, err3 := meter.Float64Histogram("kiln.job.delay",
		metric.WithUnit("s"),
		metric.WithDescription("Time from when a job was due to when its handler started."),
		metric.WithExplicitBucketBoundaries(buckets...),
	)
	if err := errors.Join(err1, err2, err3); err != nil {
		otel.Handle(err)
	}
	return &processor{
		tracer:     c.tracerProvider.Tracer(scope),
		propagator: c.propagator,
		processed:  processed,
		duration:   duration,
		delay:      delay,
	}
}

func (p *processor) process(ctx context.Context, j *kiln.RawJob, next kiln.HandlerFunc) error {
	start := time.Now()
	r := p.route(j.Kind, j.Queue)
	ctx = p.propagator.Extract(ctx, propagation.MapCarrier(j.Meta))
	ctx, span := p.tracer.Start(ctx, r.spanName,
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			messagingSystem.String("kiln"),
			messagingOperation.String("process"),
			messagingDestination.String(j.Queue),
			messagingMessageID.String(strconv.FormatInt(j.ID, 10)),
			jobKind.String(j.Kind),
			jobAttempt.Int(j.Attempt),
		),
	)
	defer span.End()
	if !j.RunAt.IsZero() {
		p.delay.Record(ctx, max(start.Sub(j.RunAt), 0).Seconds(), r.started)
	}

	err := next(ctx, j)

	o := classify(ctx, err)
	switch o {
	case snoozed:
		span.AddEvent("kiln.job.snoozed")
	case interrupted:
		span.AddEvent("kiln.job.interrupted")
	case failed, canceled:
		var opts []trace.EventOption
		if pe, ok := errors.AsType[*kiln.PanicError](err); ok {
			opts = append(opts, trace.WithAttributes(exceptionStacktrace.String(string(pe.Stack))))
		}
		span.RecordError(err, opts...)
		span.SetStatus(codes.Error, err.Error())
	}
	p.processed.Add(ctx, 1, r.ended[o])
	p.duration.Record(ctx, time.Since(start).Seconds(), r.ended[o])
	return err
}

func classify(ctx context.Context, err error) outcome {
	if err == nil {
		return succeeded
	}
	cause := context.Cause(ctx)
	switch {
	case errors.Is(err, kiln.ErrCanceled), errors.Is(cause, kiln.ErrCanceled):
		return canceled
	case errors.Is(cause, kiln.ErrShutdown):
		return interrupted
	case snoozes(err) && !errors.Is(err, kiln.ErrPermanent):
		return snoozed
	}
	return failed
}

func snoozes(err error) bool {
	if err == nil || err == kiln.ErrSnoozed {
		return false
	}
	if e, ok := err.(interface{ Is(error) bool }); ok && e.Is(kiln.ErrSnoozed) {
		return true
	}
	switch e := err.(type) {
	case interface{ Unwrap() error }:
		return snoozes(e.Unwrap())
	case interface{ Unwrap() []error }:
		return slices.ContainsFunc(e.Unwrap(), snoozes)
	}
	return false
}

func (p *processor) route(kind, queue string) *route {
	key := routeKey{kind, queue}
	if r, ok := p.routes.Load(key); ok {
		return r.(*route)
	}
	k, q := kindKey.String(kind), queueKey.String(queue)
	r := &route{
		spanName: "kiln process " + kind,
		started:  metric.WithAttributeSet(attribute.NewSet(k, q)),
	}
	for o, name := range outcomes {
		r.ended[o] = metric.WithAttributeSet(attribute.NewSet(k, q, outcomeKey.String(name)))
	}
	v, _ := p.routes.LoadOrStore(key, r)
	return v.(*route)
}

package kilnotel

import (
	"context"
	"maps"
	"slices"
	"strconv"

	"github.com/rafaelaugustos/kiln"
	"github.com/rafaelaugustos/kiln/driver"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type enqueuer struct {
	tracer     trace.Tracer
	propagator propagation.TextMapPropagator
}

// EnqueueMiddleware returns client middleware that wraps each insert call in a "kiln enqueue"
// producer span and writes the span's context into the Meta of every job inserted, as traceparent
// and tracestate with the W3C Trace Context propagator. Meta maps from the caller are copied, never
// modified.
func EnqueueMiddleware(opts ...Option) kiln.EnqueueMiddleware {
	c := newConfig(opts)
	e := &enqueuer{tracer: c.tracerProvider.Tracer(scope), propagator: c.propagator}
	return func(next kiln.EnqueueFunc) kiln.EnqueueFunc {
		return func(ctx context.Context, w driver.Writer, jobs []driver.InsertParams) ([]driver.Inserted, error) {
			return e.enqueue(ctx, w, jobs, next)
		}
	}
}

func (e *enqueuer) enqueue(ctx context.Context, w driver.Writer, jobs []driver.InsertParams, next kiln.EnqueueFunc) ([]driver.Inserted, error) {
	ctx, span := e.tracer.Start(ctx, "kiln enqueue",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(sendAttributes(jobs)...),
	)
	defer span.End()
	res, err := next(ctx, w, e.inject(ctx, jobs))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return res, err
	}
	if len(jobs) == 1 && len(res) == 1 {
		span.SetAttributes(messagingMessageID.String(strconv.FormatInt(res[0].ID, 10)))
	}
	return res, nil
}

func (e *enqueuer) inject(ctx context.Context, jobs []driver.InsertParams) []driver.InsertParams {
	carrier := make(propagation.MapCarrier, 2)
	e.propagator.Inject(ctx, carrier)
	if len(carrier) == 0 {
		return jobs
	}
	out := slices.Clone(jobs)
	for i := range out {
		meta := make(map[string]string, len(out[i].Meta)+len(carrier))
		maps.Copy(meta, out[i].Meta)
		maps.Copy(meta, carrier)
		out[i].Meta = meta
	}
	return out
}

func sendAttributes(jobs []driver.InsertParams) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, 5)
	attrs = append(attrs, messagingSystem.String("kiln"), messagingOperation.String("send"))
	if len(jobs) == 0 {
		return attrs
	}
	queue, kind := jobs[0].Queue, jobs[0].Kind
	for i := 1; i < len(jobs); i++ {
		if jobs[i].Queue != queue {
			queue = ""
		}
		if jobs[i].Kind != kind {
			kind = ""
		}
	}
	if queue != "" {
		attrs = append(attrs, messagingDestination.String(queue))
	}
	if len(jobs) > 1 {
		attrs = append(attrs, messagingBatchCount.Int(len(jobs)))
	}
	if kind != "" {
		attrs = append(attrs, jobKind.String(kind))
	}
	return attrs
}

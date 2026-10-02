package kilnotel

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const scope = "github.com/rafaelaugustos/kiln/kilnotel"

const (
	messagingSystem      attribute.Key = "messaging.system"
	messagingOperation   attribute.Key = "messaging.operation.type"
	messagingDestination attribute.Key = "messaging.destination.name"
	messagingBatchCount  attribute.Key = "messaging.batch.message_count"
	messagingMessageID   attribute.Key = "messaging.message.id"
	jobKind              attribute.Key = "kiln.job.kind"
	jobAttempt           attribute.Key = "kiln.job.attempt"
	exceptionStacktrace  attribute.Key = "exception.stacktrace"
)

const (
	kindKey    attribute.Key = "kind"
	queueKey   attribute.Key = "queue"
	outcomeKey attribute.Key = "outcome"
	stateKey   attribute.Key = "state"
)

// Option makes kilnotel use a given provider or propagator instead of the global one.
type Option func(*config)

type config struct {
	tracerProvider trace.TracerProvider
	meterProvider  metric.MeterProvider
	propagator     propagation.TextMapPropagator
}

func newConfig(opts []Option) config {
	c := config{
		tracerProvider: otel.GetTracerProvider(),
		meterProvider:  otel.GetMeterProvider(),
		propagator:     otel.GetTextMapPropagator(),
	}
	for _, o := range opts {
		o(&c)
	}
	return c
}

// WithTracerProvider makes kilnotel create its tracer from tp. A nil tp is ignored.
func WithTracerProvider(tp trace.TracerProvider) Option {
	return func(c *config) {
		if tp != nil {
			c.tracerProvider = tp
		}
	}
}

// WithMeterProvider makes kilnotel create its meter from mp. A nil mp is ignored.
func WithMeterProvider(mp metric.MeterProvider) Option {
	return func(c *config) {
		if mp != nil {
			c.meterProvider = mp
		}
	}
}

// WithPropagator makes kilnotel carry trace context through job meta with p. A nil p is ignored.
func WithPropagator(p propagation.TextMapPropagator) Option {
	return func(c *config) {
		if p != nil {
			c.propagator = p
		}
	}
}

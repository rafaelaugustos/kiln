package kilnotel_test

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln"
	"github.com/rafaelaugustos/kiln/kilnotel"
	"github.com/rafaelaugustos/kiln/memstore"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

func tracedMux(tel *telemetry) (*kiln.Client, *kiln.Mux) {
	client := kiln.NewClient(memstore.New(), kilnotel.EnqueueMiddleware(tel.opts...))
	mux := kiln.NewMux()
	mux.Use(kilnotel.Middleware(tel.opts...))
	return client, mux
}

func TestTraceFollowsJob(t *testing.T) {
	t.Parallel()
	tel := newTelemetry(t)
	client, mux := tracedMux(tel)
	seen := make(chan trace.SpanContext, 1)
	mux.HandleFunc("send_email", func(ctx context.Context, j *kiln.RawJob) error {
		seen <- trace.SpanContextFromContext(ctx)
		return nil
	})
	runServer(t, client, mux, kiln.ServerConfig{})

	ctx, request := tel.tracer.Start(context.Background(), "request")
	id, err := client.Enqueue(ctx, work("send_email"))
	request.End()
	if err != nil {
		t.Fatal(err)
	}
	producer := tel.span(t, "kiln enqueue")
	consumer := tel.span(t, "kiln process send_email")
	handler := receive(t, seen)

	if producer.SpanKind != trace.SpanKindProducer || consumer.SpanKind != trace.SpanKindConsumer {
		t.Errorf("span kinds = %s, %s, want producer, consumer", producer.SpanKind, consumer.SpanKind)
	}
	if producer.Parent.SpanID() != request.SpanContext().SpanID() {
		t.Errorf("enqueue span parent = %s, want the request span %s", producer.Parent.SpanID(), request.SpanContext().SpanID())
	}
	if consumer.SpanContext.TraceID() != producer.SpanContext.TraceID() {
		t.Errorf("process span trace = %s, want %s", consumer.SpanContext.TraceID(), producer.SpanContext.TraceID())
	}
	if consumer.Parent.SpanID() != producer.SpanContext.SpanID() || !consumer.Parent.IsRemote() {
		t.Errorf("process span parent = %+v, want the remote enqueue span %s", consumer.Parent, producer.SpanContext.SpanID())
	}
	if handler.SpanID() != consumer.SpanContext.SpanID() {
		t.Errorf("handler ran in span %s, want %s", handler.SpanID(), consumer.SpanContext.SpanID())
	}
	if name := consumer.InstrumentationScope.Name; name != "github.com/rafaelaugustos/kiln/kilnotel" {
		t.Errorf("instrumentation scope = %q", name)
	}

	msgID := attribute.String("messaging.message.id", strconv.FormatInt(id, 10))
	checkAttributes(t, producer,
		attribute.String("messaging.system", "kiln"),
		attribute.String("messaging.operation.type", "send"),
		attribute.String("messaging.destination.name", kiln.DefaultQueue),
		attribute.String("kiln.job.kind", "send_email"),
		msgID,
	)
	checkAttributes(t, consumer,
		attribute.String("messaging.system", "kiln"),
		attribute.String("messaging.operation.type", "process"),
		attribute.String("messaging.destination.name", kiln.DefaultQueue),
		attribute.String("kiln.job.kind", "send_email"),
		attribute.Int("kiln.job.attempt", 1),
		msgID,
	)

	r, err := client.Get(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if carried(r.Meta).SpanID() != producer.SpanContext.SpanID() {
		t.Errorf("stored meta %v does not carry the enqueue span %s", r.Meta, producer.SpanContext.SpanID())
	}
}

func TestProcessOutcomes(t *testing.T) {
	t.Parallel()
	tel := newTelemetry(t)
	client, mux := tracedMux(tel)
	mux.HandleFunc("ok", func(context.Context, *kiln.RawJob) error { return nil })
	mux.HandleFunc("fail", func(context.Context, *kiln.RawJob) error { return errors.New("boom") })
	mux.HandleFunc("snooze", func(context.Context, *kiln.RawJob) error { return kiln.Snooze(time.Hour) })
	mux.HandleFunc("snooze_sentinel", func(context.Context, *kiln.RawJob) error { return kiln.ErrSnoozed })
	mux.HandleFunc("crash", func(context.Context, *kiln.RawJob) error { panic("kaboom") })
	mux.HandleFunc("cancel", func(context.Context, *kiln.RawJob) error { return kiln.Cancel(errors.New("not needed")) })
	mux.HandleFunc("give_up", func(context.Context, *kiln.RawJob) error { return kiln.Permanent(kiln.Snooze(time.Minute)) })
	runServer(t, client, mux, kiln.ServerConfig{})

	tests := []struct {
		kind    string
		status  codes.Code
		message string
		events  []string
		outcome string
	}{
		{kind: "ok", status: codes.Unset, outcome: "succeeded"},
		{kind: "fail", status: codes.Error, message: "boom", events: []string{"exception"}, outcome: "failed"},
		{kind: "snooze", status: codes.Unset, events: []string{"kiln.job.snoozed"}, outcome: "snoozed"},
		{kind: "snooze_sentinel", status: codes.Error, message: "kiln: job snoozed", events: []string{"exception"}, outcome: "failed"},
		{kind: "crash", status: codes.Error, message: "kiln: panic: kaboom", events: []string{"exception"}, outcome: "failed"},
		{kind: "cancel", status: codes.Error, message: "not needed", events: []string{"exception"}, outcome: "canceled"},
		{kind: "give_up", status: codes.Error, message: "kiln: job snoozed for 1m0s", events: []string{"exception"}, outcome: "failed"},
	}
	for _, tt := range tests {
		enqueue(t, client, work(tt.kind), kiln.MaxAttempts(1))
	}
	for _, tt := range tests {
		t.Run(tt.kind, func(t *testing.T) {
			s := tel.span(t, "kiln process "+tt.kind)
			if s.Status.Code != tt.status || s.Status.Description != tt.message {
				t.Errorf("status = %+v, want %s %q", s.Status, tt.status, tt.message)
			}
			var events []string
			for _, e := range s.Events {
				events = append(events, e.Name)
			}
			if !slices.Equal(events, tt.events) {
				t.Errorf("events = %v, want %v", events, tt.events)
			}
		})
	}

	t.Run("panic stack", func(t *testing.T) {
		s := tel.span(t, "kiln process crash")
		for _, e := range s.Events {
			for _, kv := range e.Attributes {
				if kv.Key == "exception.stacktrace" && strings.Contains(kv.Value.AsString(), "goroutine") {
					return
				}
			}
		}
		t.Errorf("no exception event carries the panic stack: %+v", s.Events)
	})

	t.Run("metrics", func(t *testing.T) {
		m := tel.collect(t)
		processed, duration, delay := m["kiln.jobs.processed"], m["kiln.job.duration"], m["kiln.job.delay"]
		if processed.Unit != "{job}" || duration.Unit != "s" || delay.Unit != "s" {
			t.Errorf("units = %q, %q, %q", processed.Unit, duration.Unit, delay.Unit)
		}
		if n := series(t, processed); n != len(tests) {
			t.Errorf("kiln.jobs.processed has %d series, want %d", n, len(tests))
		}
		queue := attribute.String("queue", kiln.DefaultQueue)
		for _, tt := range tests {
			kind, outcome := attribute.String("kind", tt.kind), attribute.String("outcome", tt.outcome)
			if n := value(t, processed, kind, queue, outcome); n != 1 {
				t.Errorf("kiln.jobs.processed{%s, %s} = %d, want 1", tt.kind, tt.outcome, n)
			}
			if p := histogram(t, duration, kind, queue, outcome); p.Count != 1 {
				t.Errorf("kiln.job.duration{%s, %s} has %d samples, want 1", tt.kind, tt.outcome, p.Count)
			}
			if p := histogram(t, delay, kind, queue); p.Count != 1 || p.Sum < 0 || p.Sum > 10 {
				t.Errorf("kiln.job.delay{%s} has %d samples totalling %gs, want 1 short one", tt.kind, p.Count, p.Sum)
			}
		}
	})
}

func TestProcessCanceled(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		status  codes.Code
		outcome string
		cancel  func(t *testing.T, c *kiln.Client, id int64, stop func())
	}{
		{"deleted", codes.Error, "canceled", func(t *testing.T, c *kiln.Client, id int64, _ func()) {
			if _, err := c.Delete(context.Background(), id); err != nil {
				t.Fatal(err)
			}
		}},
		{"shutdown", codes.Unset, "interrupted", func(_ *testing.T, _ *kiln.Client, _ int64, stop func()) {
			stop()
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tel := newTelemetry(t)
			client, mux := tracedMux(tel)
			started := make(chan struct{}, 1)
			mux.HandleFunc("wait", func(ctx context.Context, j *kiln.RawJob) error {
				started <- struct{}{}
				<-ctx.Done()
				return ctx.Err()
			})
			_, stop := runServer(t, client, mux, kiln.ServerConfig{ShutdownTimeout: 10 * time.Millisecond})
			id := enqueue(t, client, work("wait"))
			receive(t, started)
			tt.cancel(t, client, id, stop)

			s := tel.span(t, "kiln process wait")
			if s.Status.Code != tt.status {
				t.Errorf("status = %+v, want %v", s.Status, tt.status)
			}
			labels := []attribute.KeyValue{
				attribute.String("kind", "wait"),
				attribute.String("queue", kiln.DefaultQueue),
				attribute.String("outcome", tt.outcome),
			}
			if n := value(t, tel.collect(t)["kiln.jobs.processed"], labels...); n != 1 {
				t.Errorf("%s = %d, want 1", tt.outcome, n)
			}
		})
	}
}

func TestProcessTimings(t *testing.T) {
	t.Parallel()
	tel := newTelemetry(t)
	handle := kilnotel.Middleware(tel.opts...)(func(ctx context.Context, j *kiln.RawJob) error {
		if j.Kind == "slow" {
			time.Sleep(20 * time.Millisecond)
		}
		return nil
	})
	now := time.Now()
	for i, j := range []*kiln.RawJob{
		{Kind: "late", RunAt: now.Add(-2 * time.Second)},
		{Kind: "early", RunAt: now.Add(time.Minute)},
		{Kind: "slow", RunAt: now},
		{Kind: "undated"},
	} {
		j.ID, j.Queue, j.Attempt = int64(i+1), kiln.DefaultQueue, 1
		if err := handle(context.Background(), j); err != nil {
			t.Fatal(err)
		}
	}

	m := tel.collect(t)
	queue := attribute.String("queue", kiln.DefaultQueue)
	if p := histogram(t, m["kiln.job.delay"], attribute.String("kind", "late"), queue); p.Sum < 2 || p.Sum > 10 {
		t.Errorf("late delay = %gs, want about 2s", p.Sum)
	}
	if p := histogram(t, m["kiln.job.delay"], attribute.String("kind", "early"), queue); p.Sum != 0 {
		t.Errorf("early delay = %gs, want 0", p.Sum)
	}
	if n := series(t, m["kiln.job.delay"]); n != 3 {
		t.Errorf("kiln.job.delay has %d series, want 3: a job without RunAt has no delay", n)
	}
	slow := histogram(t, m["kiln.job.duration"], attribute.String("kind", "slow"), queue, attribute.String("outcome", "succeeded"))
	if slow.Sum < 0.02 {
		t.Errorf("slow duration = %gs, want at least 20ms", slow.Sum)
	}
	for _, s := range tel.spans.GetSpans() {
		if s.Parent.IsValid() {
			t.Errorf("%s has parent %s, want a root span when the job carries no trace context", s.Name, s.Parent.SpanID())
		}
	}
}

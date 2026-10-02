package kilnotel_test

import (
	"context"
	"errors"
	"maps"
	"testing"

	"github.com/rafaelaugustos/kiln"
	"github.com/rafaelaugustos/kiln/driver"
	"github.com/rafaelaugustos/kiln/kilnotel"
	"github.com/rafaelaugustos/kiln/memstore"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

func inserted(_ context.Context, _ driver.Writer, jobs []driver.InsertParams) ([]driver.Inserted, error) {
	res := make([]driver.Inserted, len(jobs))
	for i := range res {
		res[i] = driver.Inserted{ID: int64(i + 1), State: kiln.Enqueued}
	}
	return res, nil
}

func TestEnqueueSpan(t *testing.T) {
	t.Parallel()
	system := attribute.String("messaging.system", "kiln")
	send := attribute.String("messaging.operation.type", "send")
	queue := func(name string) attribute.KeyValue { return attribute.String("messaging.destination.name", name) }
	kind := func(name string) attribute.KeyValue { return attribute.String("kiln.job.kind", name) }
	count := attribute.Int("messaging.batch.message_count", 2)
	down := errors.New("store down")
	tests := []struct {
		name string
		jobs []driver.InsertParams
		err  error
		want []attribute.KeyValue
	}{
		{
			name: "one job",
			jobs: []driver.InsertParams{{Kind: "a", Queue: "default"}},
			want: []attribute.KeyValue{system, send, queue("default"), kind("a"), attribute.String("messaging.message.id", "1")},
		},
		{
			name: "one queue",
			jobs: []driver.InsertParams{{Kind: "a", Queue: "default"}, {Kind: "b", Queue: "default"}},
			want: []attribute.KeyValue{system, send, queue("default"), count},
		},
		{
			name: "one kind",
			jobs: []driver.InsertParams{{Kind: "a", Queue: "default"}, {Kind: "a", Queue: "mail"}},
			want: []attribute.KeyValue{system, send, kind("a"), count},
		},
		{
			name: "mixed",
			jobs: []driver.InsertParams{{Kind: "a", Queue: "default"}, {Kind: "b", Queue: "mail"}},
			want: []attribute.KeyValue{system, send, count},
		},
		{
			name: "store error",
			jobs: []driver.InsertParams{{Kind: "a", Queue: "default"}},
			err:  down,
			want: []attribute.KeyValue{system, send, queue("default"), kind("a")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tel := newTelemetry(t)
			insert := inserted
			if tt.err != nil {
				insert = func(context.Context, driver.Writer, []driver.InsertParams) ([]driver.Inserted, error) {
					return nil, tt.err
				}
			}
			_, err := kilnotel.EnqueueMiddleware(tel.opts...)(insert)(context.Background(), nil, tt.jobs)
			if !errors.Is(err, tt.err) {
				t.Fatalf("err = %v, want %v", err, tt.err)
			}
			s := tel.span(t, "kiln enqueue")
			if s.SpanKind != trace.SpanKindProducer {
				t.Errorf("span kind = %s, want producer", s.SpanKind)
			}
			checkAttributes(t, s, tt.want...)
			if tt.err == nil {
				if s.Status.Code != codes.Unset {
					t.Errorf("status = %+v, want unset", s.Status)
				}
				return
			}
			if s.Status.Code != codes.Error || s.Status.Description != tt.err.Error() {
				t.Errorf("status = %+v, want an error %q", s.Status, tt.err)
			}
			if len(s.Events) != 1 || s.Events[0].Name != "exception" {
				t.Errorf("events = %+v, want one exception", s.Events)
			}
		})
	}
}

func TestEnqueueLeavesCallerMeta(t *testing.T) {
	t.Parallel()
	tel := newTelemetry(t)
	shared := map[string]string{"tenant": "acme"}
	jobs := []driver.InsertParams{
		{Kind: "a", Queue: "default", Meta: shared},
		{Kind: "b", Queue: "default", Meta: shared},
		{Kind: "c", Queue: "default"},
	}
	var sent []driver.InsertParams
	insert := func(ctx context.Context, w driver.Writer, jobs []driver.InsertParams) ([]driver.Inserted, error) {
		sent = jobs
		return inserted(ctx, w, jobs)
	}
	if _, err := kilnotel.EnqueueMiddleware(tel.opts...)(insert)(context.Background(), nil, jobs); err != nil {
		t.Fatal(err)
	}

	if want := map[string]string{"tenant": "acme"}; !maps.Equal(shared, want) {
		t.Errorf("caller's meta = %v, want %v", shared, want)
	}
	if jobs[2].Meta != nil {
		t.Errorf("caller's job meta = %v, want nil", jobs[2].Meta)
	}
	producer := tel.span(t, "kiln enqueue").SpanContext
	for i, p := range sent {
		if sc := carried(p.Meta); sc.TraceID() != producer.TraceID() || sc.SpanID() != producer.SpanID() {
			t.Errorf("job %d carries span %s/%s, want the enqueue span %s/%s", i, sc.TraceID(), sc.SpanID(), producer.TraceID(), producer.SpanID())
		}
	}
	if sent[0].Meta["tenant"] != "acme" || sent[1].Meta["tenant"] != "acme" {
		t.Errorf("existing meta was dropped: %v, %v", sent[0].Meta, sent[1].Meta)
	}
}

func TestEnqueueWithoutTraceContext(t *testing.T) {
	t.Parallel()
	var sent []driver.InsertParams
	insert := func(ctx context.Context, w driver.Writer, jobs []driver.InsertParams) ([]driver.Inserted, error) {
		sent = jobs
		return inserted(ctx, w, jobs)
	}
	mw := kilnotel.EnqueueMiddleware(
		kilnotel.WithTracerProvider(tracenoop.NewTracerProvider()),
		kilnotel.WithPropagator(propagation.TraceContext{}),
	)
	if _, err := mw(insert)(context.Background(), nil, []driver.InsertParams{{Kind: "a", Queue: "default"}}); err != nil {
		t.Fatal(err)
	}
	if sent[0].Meta != nil {
		t.Errorf("meta = %v, want nil when there is nothing to propagate", sent[0].Meta)
	}
}

func TestEnqueueThroughClient(t *testing.T) {
	t.Parallel()
	tel := newTelemetry(t)
	client := kiln.NewClient(memstore.New(), kilnotel.EnqueueMiddleware(tel.opts...))
	meta := kiln.Meta{"tenant": "acme"}
	ctx, request := tel.tracer.Start(context.Background(), "request")
	res, err := client.EnqueueMany(ctx,
		kiln.Spec{Args: work("a"), Options: []kiln.InsertOption{meta}},
		kiln.Spec{Args: work("b"), Options: []kiln.InsertOption{meta, kiln.Queue("mail")}},
	)
	request.End()
	if err != nil {
		t.Fatal(err)
	}

	if len(meta) != 1 {
		t.Errorf("caller's meta = %v, want it unchanged", meta)
	}
	producer := tel.span(t, "kiln enqueue")
	if producer.Parent.SpanID() != request.SpanContext().SpanID() {
		t.Errorf("enqueue span parent = %s, want the request span %s", producer.Parent.SpanID(), request.SpanContext().SpanID())
	}
	for _, ins := range res {
		r, err := client.Get(context.Background(), ins.ID)
		if err != nil {
			t.Fatal(err)
		}
		if carried(r.Meta).SpanID() != producer.SpanContext.SpanID() || r.Meta["tenant"] != "acme" {
			t.Errorf("job %d meta = %v, want the tenant and the enqueue span %s", r.ID, r.Meta, producer.SpanContext.SpanID())
		}
	}
}

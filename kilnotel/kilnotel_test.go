package kilnotel_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln"
	"github.com/rafaelaugustos/kiln/driver"
	"github.com/rafaelaugustos/kiln/kilnotel"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

type work string

func (w work) Kind() string { return string(w) }

type telemetry struct {
	tracer trace.Tracer
	spans  *tracetest.InMemoryExporter
	reader *sdkmetric.ManualReader
	opts   []kilnotel.Option
}

func newTelemetry(t *testing.T) *telemetry {
	t.Helper()
	spans := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(spans))
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() {
		tp.Shutdown(context.Background())
		mp.Shutdown(context.Background())
	})
	return &telemetry{
		tracer: tp.Tracer("test"),
		spans:  spans,
		reader: reader,
		opts: []kilnotel.Option{
			kilnotel.WithTracerProvider(tp),
			kilnotel.WithMeterProvider(mp),
			kilnotel.WithPropagator(propagation.TraceContext{}),
		},
	}
}

func (tel *telemetry) span(t *testing.T, name string) tracetest.SpanStub {
	t.Helper()
	var found tracetest.SpanStub
	waitFor(t, "a "+name+" span", func() bool {
		for _, s := range tel.spans.GetSpans() {
			if s.Name == name {
				found = s
				return true
			}
		}
		return false
	})
	return found
}

func (tel *telemetry) collect(t *testing.T) map[string]metricdata.Metrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := tel.reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	return byName(rm)
}

func byName(rm metricdata.ResourceMetrics) map[string]metricdata.Metrics {
	out := make(map[string]metricdata.Metrics)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			out[m.Name] = m
		}
	}
	return out
}

func value(t *testing.T, m metricdata.Metrics, attrs ...attribute.KeyValue) int64 {
	t.Helper()
	var points []metricdata.DataPoint[int64]
	switch d := m.Data.(type) {
	case metricdata.Sum[int64]:
		points = d.DataPoints
	case metricdata.Gauge[int64]:
		points = d.DataPoints
	default:
		t.Fatalf("metric %q holds %T", m.Name, m.Data)
	}
	want := attribute.NewSet(attrs...)
	for _, p := range points {
		if p.Attributes.Equals(&want) {
			return p.Value
		}
	}
	t.Fatalf("metric %q has no point for {%s}", m.Name, want.Encoded(attribute.DefaultEncoder()))
	return 0
}

func series(t *testing.T, m metricdata.Metrics) int {
	t.Helper()
	switch d := m.Data.(type) {
	case nil:
		return 0
	case metricdata.Sum[int64]:
		return len(d.DataPoints)
	case metricdata.Gauge[int64]:
		return len(d.DataPoints)
	case metricdata.Histogram[float64]:
		return len(d.DataPoints)
	}
	t.Fatalf("metric %q holds %T", m.Name, m.Data)
	return 0
}

func histogram(t *testing.T, m metricdata.Metrics, attrs ...attribute.KeyValue) metricdata.HistogramDataPoint[float64] {
	t.Helper()
	h, ok := m.Data.(metricdata.Histogram[float64])
	if !ok {
		t.Fatalf("metric %q holds %T", m.Name, m.Data)
	}
	want := attribute.NewSet(attrs...)
	for _, p := range h.DataPoints {
		if p.Attributes.Equals(&want) {
			return p
		}
	}
	t.Fatalf("metric %q has no point for {%s}", m.Name, want.Encoded(attribute.DefaultEncoder()))
	return metricdata.HistogramDataPoint[float64]{}
}

func carried(meta map[string]string) trace.SpanContext {
	return trace.SpanContextFromContext(propagation.TraceContext{}.Extract(context.Background(), propagation.MapCarrier(meta)))
}

func checkAttributes(t *testing.T, s tracetest.SpanStub, want ...attribute.KeyValue) {
	t.Helper()
	got, exp := attribute.NewSet(s.Attributes...), attribute.NewSet(want...)
	if !got.Equals(&exp) {
		enc := attribute.DefaultEncoder()
		t.Errorf("%q attributes:\n got {%s}\nwant {%s}", s.Name, got.Encoded(enc), exp.Encoded(enc))
	}
}

func runServer(t *testing.T, client *kiln.Client, mux *kiln.Mux, cfg kiln.ServerConfig) (*kiln.Server, func()) {
	t.Helper()
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 10 * time.Millisecond
	}
	server, err := kiln.NewServer(client, mux, cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Run(ctx) }()
	stop := sync.OnceFunc(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("run: %v", err)
		}
	})
	t.Cleanup(stop)
	waitFor(t, "the server to start", func() bool { return server.Healthy() == nil })
	return server, stop
}

func enqueue(t *testing.T, c *kiln.Client, args kiln.Args, opts ...kiln.InsertOption) int64 {
	t.Helper()
	id, err := c.Enqueue(context.Background(), args, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting on channel")
	}
	var zero T
	return zero
}

func TestGlobalProviders(t *testing.T) {
	spans := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(spans))
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() {
		tp.Shutdown(context.Background())
		mp.Shutdown(context.Background())
	})
	otel.SetTracerProvider(tp)
	otel.SetMeterProvider(mp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	var sent []driver.InsertParams
	insert := func(_ context.Context, _ driver.Writer, jobs []driver.InsertParams) ([]driver.Inserted, error) {
		sent = jobs
		return []driver.Inserted{{ID: 1, State: kiln.Enqueued}}, nil
	}
	_, err := kilnotel.EnqueueMiddleware()(insert)(context.Background(), nil, []driver.InsertParams{{Kind: "ping", Queue: kiln.DefaultQueue}})
	if err != nil {
		t.Fatal(err)
	}
	handle := kilnotel.Middleware()(func(context.Context, *kiln.RawJob) error { return nil })
	job := &kiln.RawJob{ID: 1, Kind: "ping", Queue: kiln.DefaultQueue, Attempt: 1, RunAt: time.Now(), Meta: sent[0].Meta}
	if err := handle(context.Background(), job); err != nil {
		t.Fatal(err)
	}

	got := spans.GetSpans()
	if len(got) != 2 {
		t.Fatalf("recorded %d spans, want 2", len(got))
	}
	producer, consumer := got[0], got[1]
	if consumer.Parent.SpanID() != producer.SpanContext.SpanID() {
		t.Errorf("process span parent = %s, want the enqueue span %s", consumer.Parent.SpanID(), producer.SpanContext.SpanID())
	}
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	processed, ok := byName(rm)["kiln.jobs.processed"]
	if !ok {
		t.Fatal("kiln.jobs.processed was not recorded on the global meter provider")
	}
	if n := value(t, processed, attribute.String("kind", "ping"), attribute.String("queue", kiln.DefaultQueue), attribute.String("outcome", "succeeded")); n != 1 {
		t.Errorf("processed = %d, want 1", n)
	}
}

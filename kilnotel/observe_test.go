package kilnotel_test

import (
	"context"
	"errors"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln"
	"github.com/rafaelaugustos/kiln/driver"
	"github.com/rafaelaugustos/kiln/kilnotel"
	"github.com/rafaelaugustos/kiln/memstore"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

var gaugeNames = []string{"kiln.completer.pending", "kiln.jobs.failed", "kiln.queue.jobs", "kiln.server.capacity", "kiln.server.running"}

type stubInspector struct {
	driver.Inspector
	queues func(context.Context) ([]driver.QueueInfo, error)
	counts func(context.Context) (driver.Counts, error)
}

func (s stubInspector) Queues(ctx context.Context) ([]driver.QueueInfo, error) {
	return s.queues(ctx)
}

func (s stubInspector) Counts(ctx context.Context) (driver.Counts, error) {
	if s.counts == nil {
		return driver.Counts{Failed: 2}, nil
	}
	return s.counts(ctx)
}

func idleServer(t *testing.T, workers int) *kiln.Server {
	t.Helper()
	mux := kiln.NewMux()
	mux.HandleFunc("noop", func(context.Context, *kiln.RawJob) error { return nil })
	s, err := kiln.NewServer(kiln.NewClient(memstore.New()), mux, kiln.ServerConfig{Queues: map[string]int{kiln.DefaultQueue: workers}})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestObserve(t *testing.T) {
	t.Parallel()
	tel := newTelemetry(t)
	store := memstore.New()
	client := kiln.NewClient(store)
	mux := kiln.NewMux()
	started, release := make(chan struct{}, 1), make(chan struct{})
	mux.HandleFunc("wait", func(context.Context, *kiln.RawJob) error {
		started <- struct{}{}
		<-release
		return nil
	})
	mux.HandleFunc("fail", func(context.Context, *kiln.RawJob) error { return errors.New("card declined") })
	server, _ := runServer(t, client, mux, kiln.ServerConfig{Queues: map[string]int{kiln.DefaultQueue: 3}})
	defer close(release)
	unregister, err := kilnotel.Observe(server, store, tel.opts...)
	if err != nil {
		t.Fatal(err)
	}
	enqueue(t, client, work("wait"))
	enqueue(t, client, work("wait"), kiln.Queue("mail"), kiln.Delay(time.Hour))
	receive(t, started)
	enqueue(t, client, work("fail"), kiln.MaxAttempts(1))
	waitFor(t, "a failed job", func() bool {
		c, err := store.Counts(context.Background())
		return err == nil && c.Failed == 1
	})

	m := tel.collect(t)
	for name, want := range map[string]int64{"kiln.server.running": 1, "kiln.server.capacity": 3, "kiln.completer.pending": 0, "kiln.jobs.failed": 1} {
		if got := value(t, m[name]); got != want {
			t.Errorf("%s = %d, want %d", name, got, want)
		}
	}
	jobs := m["kiln.queue.jobs"]
	if jobs.Unit != "{job}" {
		t.Errorf("kiln.queue.jobs unit = %q", jobs.Unit)
	}
	want := map[[2]string]int64{
		{kiln.DefaultQueue, "enqueued"}:   0,
		{kiln.DefaultQueue, "processing"}: 1,
		{kiln.DefaultQueue, "scheduled"}:  0,
		{kiln.DefaultQueue, "throttled"}:  0,
		{"mail", "enqueued"}:              0,
		{"mail", "processing"}:            0,
		{"mail", "scheduled"}:             1,
		{"mail", "throttled"}:             0,
	}
	for k, n := range want {
		if got := value(t, jobs, attribute.String("queue", k[0]), attribute.String("state", k[1])); got != n {
			t.Errorf("kiln.queue.jobs{%s, %s} = %d, want %d", k[0], k[1], got, n)
		}
	}
	if n := series(t, jobs); n != len(want) {
		t.Errorf("kiln.queue.jobs has %d series, want %d", n, len(want))
	}

	if err := unregister(); err != nil {
		t.Fatal(err)
	}
	m = tel.collect(t)
	for _, name := range gaugeNames {
		if n := series(t, m[name]); n != 0 {
			t.Errorf("%s still has %d series after unregister", name, n)
		}
	}
	if err := unregister(); err != nil {
		t.Errorf("second unregister: %v", err)
	}
}

func TestObserveQueueErrors(t *testing.T) {
	t.Parallel()
	tel := newTelemetry(t)
	down := errors.New("store down")
	var deadline time.Time
	inspector := stubInspector{queues: func(ctx context.Context) ([]driver.QueueInfo, error) {
		deadline, _ = ctx.Deadline()
		return nil, down
	}}
	unregister, err := kilnotel.Observe(idleServer(t, 2), inspector, tel.opts...)
	if err != nil {
		t.Fatal(err)
	}
	defer unregister()

	var rm metricdata.ResourceMetrics
	if err := tel.reader.Collect(context.Background(), &rm); !errors.Is(err, down) {
		t.Fatalf("collect: %v, want %v", err, down)
	}
	if left := time.Until(deadline); deadline.IsZero() || left > 5*time.Second {
		t.Errorf("queues ran with %v left before its deadline, want at most 5s", left)
	}
	if n := value(t, byName(rm)["kiln.server.capacity"]); n != 2 {
		t.Errorf("kiln.server.capacity = %d, want 2 even when the queues cannot be read", n)
	}
}

func TestObserveEitherSide(t *testing.T) {
	t.Parallel()
	if _, err := kilnotel.Observe(nil, nil); !errors.Is(err, kiln.ErrInvalid) {
		t.Errorf("Observe(nil, nil) = %v, want %v", err, kiln.ErrInvalid)
	}
	queues := stubInspector{queues: func(context.Context) ([]driver.QueueInfo, error) {
		return []driver.QueueInfo{{Name: kiln.DefaultQueue, Enqueued: 4}}, nil
	}}
	tests := []struct {
		name      string
		server    *kiln.Server
		inspector driver.Inspector
		want      []string
	}{
		{"server", idleServer(t, 1), nil, []string{"kiln.completer.pending", "kiln.server.capacity", "kiln.server.running"}},
		{"inspector", nil, queues, []string{"kiln.jobs.failed", "kiln.queue.jobs"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tel := newTelemetry(t)
			unregister, err := kilnotel.Observe(tt.server, tt.inspector, tel.opts...)
			if err != nil {
				t.Fatal(err)
			}
			defer unregister()
			if got := slices.Sorted(maps.Keys(tel.collect(t))); !slices.Equal(got, tt.want) {
				t.Errorf("reported %v, want %v", got, tt.want)
			}
		})
	}
}

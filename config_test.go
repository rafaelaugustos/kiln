package kiln

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln/memstore"
)

func TestServerConfig(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		cfg  ServerConfig
		ok   bool
	}{
		{"defaults", ServerConfig{}, true},
		{"short dead after", ServerConfig{DeadAfter: 20 * time.Second}, false},
		{"slow heartbeat", ServerConfig{HeartbeatInterval: 30 * time.Second}, false},
		{"slow heartbeat long dead after", ServerConfig{HeartbeatInterval: 30 * time.Second, DeadAfter: 2 * time.Minute}, true},
		{"queue in two pools", ServerConfig{Queues: map[string]int{"a": 1}, Pools: []Pool{{Queues: []string{"b", "a"}, Workers: 2}}}, false},
		{"invalid queue", ServerConfig{Queues: map[string]int{"Mail Out": 1}}, false},
		{"no workers", ServerConfig{Pools: []Pool{{Queues: []string{"a"}}}}, false},
		{"no queues", ServerConfig{Pools: []Pool{{Workers: 1}}}, false},
		{"negative poll", ServerConfig{PollInterval: -time.Second}, false},
		{"negative batch", ServerConfig{FetchBatch: -1}, false},
		{"nanosecond leader ttl", ServerConfig{LeaderTTL: 2}, false},
		{"nanosecond heartbeat", ServerConfig{HeartbeatInterval: 5}, false},
		{"nanosecond poll", ServerConfig{PollInterval: 5}, false},
		{"microsecond leader ttl", ServerConfig{LeaderTTL: 900 * time.Microsecond}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			m := NewMux()
			m.HandleFunc("x", func(context.Context, *RawJob) error { return nil })
			_, err := NewServer(NewClient(memstore.New()), m, tc.cfg)
			if (err == nil) != tc.ok {
				t.Fatalf("err = %v, want ok %v", err, tc.ok)
			}
			if err != nil && !errors.Is(err, ErrInvalid) {
				t.Fatalf("err = %v, want ErrInvalid", err)
			}
		})
	}
}

func TestServerConfigResolve(t *testing.T) {
	t.Parallel()
	cfg, err := ServerConfig{
		Queues:     map[string]int{"low": 2, "high": 4},
		Pools:      []Pool{{Queues: []string{"critical", "default"}, Workers: 300}},
		FetchBatch: 500,
		Timeout:    NoTimeout,
	}.resolve()
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, p := range cfg.Pools {
		names = append(names, p.Queues...)
	}
	if want := []string{"critical", "default", "high", "low"}; !slices.Equal(names, want) {
		t.Fatalf("queues = %v, want %v", names, want)
	}
	if cfg.Timeout != NoTimeout || cfg.PollInterval != time.Second || cfg.DeadAfter != time.Minute || cfg.Retention.Failed != Forever || cfg.Name == "" || cfg.Logger == nil {
		t.Fatalf("defaults not applied: %+v", cfg)
	}
	m := NewMux()
	m.HandleFunc("x", func(context.Context, *RawJob) error { return nil })
	s, err := NewServer(NewClient(memstore.New()), m, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if s.capacity != 306 || s.prods[0].batch != maxFetchBatch || s.prods[1].batch != 4 {
		t.Fatalf("capacity %d batches %d %d", s.capacity, s.prods[0].batch, s.prods[1].batch)
	}
}

func TestServerNeedsHandlers(t *testing.T) {
	t.Parallel()
	if _, err := NewServer(NewClient(memstore.New()), NewMux(), ServerConfig{}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("err = %v", err)
	}
}

func TestServerHealth(t *testing.T) {
	t.Parallel()
	c := NewClient(memstore.New())
	m := NewMux()
	m.HandleFunc("x", func(context.Context, *RawJob) error { return nil })
	s, err := NewServer(c, m, fastConfig())
	if err != nil {
		t.Fatal(err)
	}
	if s.ID() != "" || s.Healthy() == nil {
		t.Fatalf("before run: id %q healthy %v", s.ID(), s.Healthy())
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	eventually(t, 5*time.Second, "leader and listener", func() bool {
		st := s.Stats()
		return s.Healthy() == nil && st.Leader && st.Listening
	})
	st := s.Stats()
	if st.Capacity != 8 || st.Fenced || st.HeartbeatAge > time.Second {
		t.Fatalf("stats %+v", st)
	}
	servers, err := c.Store().Servers(ctx)
	if err != nil || len(servers) != 1 || servers[0].ID != s.ID() || !slices.Equal(servers[0].Kinds, []string{"x"}) || servers[0].Version != version {
		t.Fatalf("servers %+v %v", servers, err)
	}
	if err := s.Run(ctx); err == nil {
		t.Fatal("second Run succeeded")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if s.Healthy() == nil {
		t.Fatal("healthy after stop")
	}
}

package redisbus

import (
	"cmp"
	"context"
	"math/rand/v2"
	"net"
	"os"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
	"github.com/redis/go-redis/v9"
)

func redisAddr() string {
	return cmp.Or(os.Getenv("KILN_REDIS_ADDR"), "localhost:56379")
}

func connect(tb testing.TB) *redis.Client {
	return dial(tb, &redis.Options{Addr: redisAddr()})
}

func dial(tb testing.TB, o *redis.Options) *redis.Client {
	tb.Helper()
	o.ContextTimeoutEnabled = true
	c := redis.NewClient(o)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Ping(ctx).Err(); err != nil {
		c.Close()
		tb.Skipf("redis unavailable at %s: %v", o.Addr, err)
	}
	tb.Cleanup(func() { c.Close() })
	return c
}

func unreachable(tb testing.TB) string {
	tb.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

func channel() string {
	return "kiln-test-" + strconv.FormatUint(rand.Uint64(), 36)
}

func publish(tb testing.TB, b *Bus, events ...driver.Event) {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := b.Publish(ctx, events); err != nil {
		tb.Fatal(err)
	}
}

type recorder struct {
	mu      sync.Mutex
	events  []driver.Event
	changed chan struct{}
}

func newRecorder() *recorder {
	return &recorder{changed: make(chan struct{}, 1)}
}

func (r *recorder) add(e driver.Event) {
	r.mu.Lock()
	r.events = append(r.events, e)
	r.mu.Unlock()
	signal(r.changed)
}

func (r *recorder) snapshot() []driver.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.events)
}

func (r *recorder) wait(tb testing.TB, what string, done func([]driver.Event) bool) []driver.Event {
	tb.Helper()
	timeout := time.NewTimer(5 * time.Second)
	defer timeout.Stop()
	for {
		evs := r.snapshot()
		if done(evs) {
			return evs
		}
		select {
		case <-r.changed:
		case <-timeout.C:
			tb.Fatalf("timed out waiting for %s after %d events, the last were %v", what, len(evs), evs[max(0, len(evs)-5):])
		}
	}
}

func resyncs(n int) func([]driver.Event) bool {
	return func(evs []driver.Event) bool {
		return count(evs, driver.Event{Kind: driver.Resync}) >= n
	}
}

func has(e driver.Event) func([]driver.Event) bool {
	return func(evs []driver.Event) bool { return slices.Contains(evs, e) }
}

func count(evs []driver.Event, e driver.Event) int {
	n := 0
	for _, v := range evs {
		if v == e {
			n++
		}
	}
	return n
}

func run(t *testing.T, b *Bus, fn func(driver.Event)) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- b.Subscribe(ctx, fn) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("subscribe: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("subscribe did not return after cancel")
		}
	})
}

func listen(t *testing.T, b *Bus) *recorder {
	t.Helper()
	r := newRecorder()
	run(t, b, r.add)
	r.wait(t, "the first resync", resyncs(1))
	return r
}

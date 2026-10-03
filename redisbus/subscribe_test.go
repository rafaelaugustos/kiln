package redisbus

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
	"github.com/redis/go-redis/v9"
)

func TestReconnect(t *testing.T) {
	t.Parallel()
	admin := connect(t)
	name, ch := channel(), channel()
	pub := New(admin, Channel(ch))
	r := listen(t, New(dial(t, &redis.Options{Addr: redisAddr(), ClientName: name}), Channel(ch)))

	for i := range 2 {
		kill(t, admin, name)
		r.wait(t, "a resync after the reconnect", resyncs(i+2))
		want := driver.Event{Kind: driver.JobsReady, Queue: "after-" + strconv.Itoa(i)}
		publish(t, pub, want)
		r.wait(t, "an event after the reconnect", has(want))
	}
}

func TestReconnectBackoff(t *testing.T) {
	t.Parallel()
	admin := connect(t)
	name := channel()
	r := listen(t, New(dial(t, &redis.Options{Addr: redisAddr(), ClientName: name}), Channel(channel())))

	n := 1
	for deadline := time.Now().Add(500 * time.Millisecond); time.Now().Before(deadline); {
		kill(t, admin, name)
		n++
		r.wait(t, "a resync after the reconnect", resyncs(n))
	}
	if n > 7 {
		t.Errorf("%d subscriptions in 500ms of immediate disconnects, want a growing backoff", n)
	}
}

func kill(t *testing.T, c *redis.Client, name string) {
	t.Helper()
	ctx := context.Background()
	list, err := c.ClientList(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	killed := 0
	for line := range strings.Lines(list) {
		f := map[string]string{}
		for kv := range strings.FieldsSeq(line) {
			k, v, _ := strings.Cut(kv, "=")
			f[k] = v
		}
		if f["name"] != name || f["sub"] == "0" {
			continue
		}
		if err := c.ClientKillByFilter(ctx, "ID", f["id"]).Err(); err != nil {
			t.Fatal(err)
		}
		killed++
	}
	if killed == 0 {
		t.Fatalf("no subscribed connection named %s", name)
	}
}

func TestSilentConnection(t *testing.T) {
	t.Parallel()
	pub := connect(t)
	p := newProxy(t, redisAddr())
	ch := channel()
	b := New(dial(t, &redis.Options{Addr: p.addr()}), Channel(ch))
	b.idle = 100 * time.Millisecond
	r := listen(t, b)

	time.Sleep(5 * b.idle)
	if n := count(r.snapshot(), driver.Event{Kind: driver.Resync}); n != 1 {
		t.Fatalf("%d resyncs while pings were answered", n)
	}
	p.mute()
	r.wait(t, "a resync after the silent connection was dropped", resyncs(2))
	want := driver.Event{Kind: driver.JobsReady, Queue: "after"}
	publish(t, New(pub, Channel(ch)), want)
	r.wait(t, "an event on the new connection", has(want))
}

func TestSlowSubscriber(t *testing.T) {
	t.Parallel()
	c := connect(t)
	ch := channel()
	pub := New(c, Channel(ch))
	block := driver.Event{Kind: driver.JobsReady, Queue: "block"}
	blocked, release := make(chan struct{}), make(chan struct{})
	r := newRecorder()
	run(t, New(c, Channel(ch)), func(e driver.Event) {
		r.add(e)
		if e == block {
			close(blocked)
			<-release
		}
	})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)

	r.wait(t, "the first resync", resyncs(1))
	publish(t, pub, block)
	select {
	case <-blocked:
	case <-time.After(5 * time.Second):
		t.Fatal("the blocking event never arrived")
	}

	flood := driver.Event{Kind: driver.JobsReady, Queue: "flood"}
	batch := slices.Repeat([]driver.Event{flood}, 100)
	const messages = 50
	start := time.Now()
	for range messages {
		publish(t, pub, batch...)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Errorf("publishing while a subscriber is blocked took %v", d)
	}
	unblock()

	r.wait(t, "a resync for the dropped events", resyncs(2))
	marker := driver.Event{Kind: driver.JobsReady, Queue: "marker"}
	deadline := time.Now().Add(20 * time.Second)
	got := r.snapshot()
	for !has(marker)(got) {
		if time.Now().After(deadline) {
			t.Fatalf("no event got through after the backlog; %d events, the last were %v", len(got), got[max(0, len(got)-5):])
		}
		publish(t, pub, marker)
		select {
		case <-r.changed:
		case <-time.After(500 * time.Millisecond):
		}
		got = r.snapshot()
	}
	if n := count(got, flood); n >= messages*len(batch) {
		t.Errorf("all %d events reached a blocked subscriber, want drops", n)
	}
}

func TestCancel(t *testing.T) {
	t.Parallel()
	t.Run("connected", func(t *testing.T) {
		t.Parallel()
		b := New(connect(t), Channel(channel()))
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		r := newRecorder()
		done := make(chan error, 1)
		go func() { done <- b.Subscribe(ctx, r.add) }()
		r.wait(t, "the first resync", resyncs(1))
		cancel()
		returns(t, done, 500*time.Millisecond)
	})
	t.Run("unreachable", func(t *testing.T) {
		t.Parallel()
		c := redis.NewClient(&redis.Options{Addr: unreachable(t)})
		t.Cleanup(func() { c.Close() })
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		done := make(chan error, 1)
		go func() {
			done <- New(c).Subscribe(ctx, func(e driver.Event) { t.Errorf("event %v without a connection", e) })
		}()
		returns(t, done, time.Second)
	})
}

func returns(t *testing.T, done <-chan error, within time.Duration) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("subscribe returned %v", err)
		}
	case <-time.After(within):
		t.Fatalf("subscribe did not return within %v", within)
	}
}

func TestClosedClient(t *testing.T) {
	t.Parallel()
	c := redis.NewClient(&redis.Options{Addr: redisAddr()})
	c.Close()
	b := New(c)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := b.Publish(ctx, nil); err != nil {
		t.Fatalf("publishing nothing: %v", err)
	}
	if err := b.Publish(ctx, []driver.Event{{Kind: driver.JobsReady, Queue: "q"}}); !errors.Is(err, redis.ErrClosed) {
		t.Fatalf("publish: %v, want redis.ErrClosed", err)
	}
	done := make(chan error, 1)
	go func() {
		done <- b.Subscribe(ctx, func(e driver.Event) { t.Errorf("event %v on a closed client", e) })
	}()
	select {
	case err := <-done:
		if !errors.Is(err, redis.ErrClosed) {
			t.Fatalf("subscribe: %v, want redis.ErrClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("subscribe kept retrying on a closed client")
	}
}

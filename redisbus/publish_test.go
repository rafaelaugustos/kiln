package redisbus

import (
	"context"
	"errors"
	"net"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
	"github.com/redis/go-redis/v9"
)

func TestPublishInvalid(t *testing.T) {
	t.Parallel()
	b := New(connect(t), Channel(channel()))
	r := listen(t, b)

	valid := driver.Event{Kind: driver.JobsReady, Queue: "a"}
	err := b.Publish(context.Background(), []driver.Event{valid, {Kind: 0}})
	if !errors.Is(err, driver.ErrInvalid) {
		t.Fatalf("err %v, want ErrInvalid", err)
	}
	marker := driver.Event{Kind: driver.JobsReady, Queue: "marker"}
	publish(t, b, marker)
	if got := r.wait(t, "the marker", has(marker)); slices.Contains(got, valid) {
		t.Fatalf("a rejected call published %v", valid)
	}
}

func TestPublishDeadline(t *testing.T) {
	t.Parallel()
	silent, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { silent.Close() })
	go func() {
		var conns []net.Conn
		defer func() {
			for _, c := range conns {
				c.Close()
			}
		}()
		for {
			c, err := silent.Accept()
			if err != nil {
				return
			}
			conns = append(conns, c)
		}
	}()

	for name, addr := range map[string]string{"refused": unreachable(t), "silent": silent.Addr().String()} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			c := redis.NewClient(&redis.Options{Addr: addr, ContextTimeoutEnabled: true})
			t.Cleanup(func() { c.Close() })
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			start := time.Now()
			if err := New(c).Publish(ctx, []driver.Event{{Kind: driver.JobsReady, Queue: "q"}}); err == nil {
				t.Fatal("publish succeeded")
			}
			if d := time.Since(start); d > time.Second {
				t.Errorf("publish took %v with a 100ms deadline", d)
			}
		})
	}
}

func BenchmarkPublish(b *testing.B) {
	ch := channel()
	c := connect(b)
	bus := New(c, Channel(ch))
	ctx := context.Background()
	for _, n := range []int{1, 16} {
		events := make([]driver.Event, n)
		for i := range events {
			events[i] = driver.Event{Kind: driver.JobsReady, Queue: "queue-" + strconv.Itoa(i)}
		}
		b.Run("events="+strconv.Itoa(n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if err := bus.Publish(ctx, events); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
	b.Run("parallel", func(b *testing.B) {
		events := []driver.Event{{Kind: driver.JobsReady, Queue: "default"}}
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if err := bus.Publish(ctx, events); err != nil {
					b.Error(err)
					return
				}
			}
		})
	})
	b.Run("go-redis", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if err := c.Publish(ctx, ch, "\x02\x07default").Err(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

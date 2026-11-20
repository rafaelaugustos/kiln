package redisstore_test

import (
	"cmp"
	"context"
	"math/rand/v2"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
	"github.com/rafaelaugustos/kiln/drivertest"
	"github.com/rafaelaugustos/kiln/redisstore"
	"github.com/redis/go-redis/v9"
)

func connect(tb testing.TB) *redis.Client {
	tb.Helper()
	addr := cmp.Or(os.Getenv("KILN_REDIS_ADDR"), "localhost:56379")
	c := redis.NewClient(&redis.Options{Addr: addr, ContextTimeoutEnabled: true})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Ping(ctx).Err(); err != nil {
		c.Close()
		tb.Skipf("redis unavailable at %s: %v", addr, err)
	}
	tb.Cleanup(func() { c.Close() })
	return c
}

func open(tb testing.TB, c *redis.Client) *redisstore.Store {
	tb.Helper()
	return openAt(tb, c, "kiln-test-"+strconv.FormatUint(rand.Uint64(), 36))
}

func openAt(tb testing.TB, c *redis.Client, prefix string) *redisstore.Store {
	tb.Helper()
	s, err := redisstore.New(context.Background(), c, redisstore.Prefix(prefix))
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() {
		ctx := context.Background()
		iter := c.Scan(ctx, 0, "{"+prefix+"}:*", 1000).Iterator()
		var keys []string
		for iter.Next(ctx) {
			keys = append(keys, iter.Val())
		}
		for chunk := range slicesChunk(keys, 500) {
			c.Unlink(ctx, chunk...)
		}
	})
	return s
}

func slicesChunk(keys []string, n int) func(func([]string) bool) {
	return func(yield func([]string) bool) {
		for len(keys) > 0 {
			k := min(n, len(keys))
			if !yield(keys[:k]) {
				return
			}
			keys = keys[k:]
		}
	}
}

func TestConformance(t *testing.T) {
	c := connect(t)
	drivertest.Run(t, func(t *testing.T) driver.Store { return open(t, c) })
}

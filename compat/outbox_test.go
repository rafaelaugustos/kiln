package compat

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rafaelaugustos/kiln"
	"github.com/rafaelaugustos/kiln/pgstore"
	"github.com/rafaelaugustos/kiln/redisstore"
	"github.com/redis/go-redis/v9"
)

type charge struct{ Order int }

func (charge) Kind() string { return "charge" }

type receipt struct{ Order int }

func (receipt) Kind() string { return "receipt" }

func TestOutboxFromPostgresToRedis(t *testing.T) {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: cmp.Or(os.Getenv("KILN_REDIS_ADDR"), "localhost:56379"), ContextTimeoutEnabled: true})
	defer rdb.Close()
	pctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := rdb.Ping(pctx).Err(); err != nil {
		t.Skipf("redis unavailable: %v", err)
	}
	pool, err := pgxpool.New(ctx, cmp.Or(os.Getenv("KILN_DATABASE_URL"), "postgres://kiln:kiln@localhost:55432/kiln?sslmode=disable"))
	if err != nil {
		t.Skipf("postgres unavailable: %v", err)
	}
	defer pool.Close()
	if err := pool.Ping(pctx); err != nil {
		t.Skipf("postgres unavailable: %v", err)
	}

	name := fmt.Sprintf("ko_%x", rand.Uint64())
	t.Cleanup(func() {
		pool.Exec(ctx, "DROP SCHEMA IF EXISTS "+name+" CASCADE")
		keys, _ := rdb.Keys(ctx, "{"+name+"}:*").Result()
		if len(keys) > 0 {
			rdb.Unlink(ctx, keys...)
		}
	})
	store, err := redisstore.New(ctx, rdb, redisstore.Prefix(name))
	if err != nil {
		t.Fatal(err)
	}
	ob, err := pgstore.NewOutbox(ctx, pool, store, pgstore.Schema(name))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "CREATE TABLE "+name+".orders (id int PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	runs := make(map[string]int)
	ran := func(what string, order int) {
		mu.Lock()
		defer mu.Unlock()
		runs[fmt.Sprintf("%s %d", what, order)]++
	}
	count := func(what string, order int) int {
		mu.Lock()
		defer mu.Unlock()
		return runs[fmt.Sprintf("%s %d", what, order)]
	}
	mux := kiln.NewMux()
	kiln.Handle(mux, func(_ context.Context, j *kiln.Job[charge]) error {
		ran("charge", j.Args.Order)
		return nil
	})
	kiln.Handle(mux, func(_ context.Context, j *kiln.Job[receipt]) error {
		ran("receipt", j.Args.Order)
		return nil
	})
	client := kiln.NewClient(store)
	server, err := kiln.NewServer(client, mux, kiln.ServerConfig{
		PollInterval:    50 * time.Millisecond,
		ShutdownTimeout: time.Second,
		Outbox:          ob,
	})
	if err != nil {
		t.Fatal(err)
	}
	sctx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- server.Run(sctx) }()
	t.Cleanup(func() {
		stop()
		if err := <-done; err != nil {
			t.Errorf("run: %v", err)
		}
	})

	place := func(order int, commit, notify bool) int64 {
		t.Helper()
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		if _, err := tx.Exec(ctx, "INSERT INTO "+name+".orders (id) VALUES ($1)", order); err != nil {
			t.Fatal(err)
		}
		w := ob.Tx(tx)
		id, err := client.EnqueueTx(ctx, w, charge{Order: order})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := client.EnqueueTx(ctx, w, receipt{Order: order}, kiln.After{id}); err != nil {
			t.Fatal(err)
		}
		if !commit {
			return id
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		if notify {
			if err := w.Notify(ctx); err != nil {
				t.Fatalf("notify: %v", err)
			}
		}
		return id
	}
	wait := func(what string, cond func() bool) {
		t.Helper()
		deadline := time.Now().Add(15 * time.Second)
		for !cond() {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for %s", what)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}

	place(1, true, true)
	wait("the notified order to run", func() bool { return count("charge", 1) == 1 && count("receipt", 1) == 1 })

	place(2, true, false)
	wait("the relay to deliver the order whose process never notified", func() bool {
		return count("charge", 2) == 1 && count("receipt", 2) == 1
	})

	gone := place(3, false, false)
	time.Sleep(2500 * time.Millisecond)
	if n := count("charge", 3) + count("receipt", 3); n != 0 {
		t.Fatalf("jobs of the order that rolled back ran %d times", n)
	}
	if _, err := client.Get(ctx, gone); !errors.Is(err, kiln.ErrNotFound) {
		t.Fatalf("job of the order that rolled back: %v, want not found", err)
	}
	for _, order := range []int{1, 2} {
		if count("charge", order) != 1 || count("receipt", order) != 1 {
			t.Fatalf("order %d ran charge %d and receipt %d times, want once each", order, count("charge", order), count("receipt", order))
		}
	}
	var left int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+name+".outbox").Scan(&left); err != nil || left != 0 {
		t.Fatalf("%d entries left in the outbox, %v", left, err)
	}
}

package main

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"text/tabwriter"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rafaelaugustos/kiln"
	"github.com/rafaelaugustos/kiln/driver"
	"github.com/rafaelaugustos/kiln/pgstore"
	"github.com/rafaelaugustos/kiln/redisstore"
	"github.com/redis/go-redis/v9"
)

type placed struct{ ID int }

func (placed) Kind() string { return "order" }

type counters struct {
	wal     int64
	commits int64
	rows    int64
}

func (c counters) minus(o counters) counters {
	return counters{c.wal - o.wal, c.commits - o.commits, c.rows - o.rows}
}

type loadMode struct {
	name  string
	setup func(ctx context.Context, pool *pgxpool.Pool, schema string) (*loadRun, error)
}

type loadRun struct {
	bind  func(tx pgx.Tx) driver.TxWriter
	store driver.Store
	cfg   kiln.ServerConfig
	close func()
}

func loadModes(o options) []loadMode {
	return []loadMode{
		{"orders only, no jobs", func(context.Context, *pgxpool.Pool, string) (*loadRun, error) {
			return &loadRun{close: func() {}}, nil
		}},
		{"kiln on PostgreSQL", func(ctx context.Context, pool *pgxpool.Pool, schema string) (*loadRun, error) {
			st, err := pgstore.New(ctx, pool, pgstore.Schema(schema))
			if err != nil {
				return nil, err
			}
			return &loadRun{
				bind:  func(tx pgx.Tx) driver.TxWriter { return st.Tx(tx) },
				store: st,
				close: st.Close,
			}, nil
		}},
		{"kiln on Redis, PostgreSQL outbox", func(ctx context.Context, pool *pgxpool.Pool, schema string) (*loadRun, error) {
			rdb := redis.NewClient(&redis.Options{Addr: o.redis, ContextTimeoutEnabled: true})
			st, err := redisstore.New(ctx, rdb, redisstore.Prefix(schema))
			if err != nil {
				rdb.Close()
				return nil, err
			}
			ob, err := pgstore.NewOutbox(ctx, pool, st, pgstore.Schema(schema))
			if err != nil {
				rdb.Close()
				return nil, err
			}
			return &loadRun{
				bind:  func(tx pgx.Tx) driver.TxWriter { return ob.Tx(tx) },
				store: st,
				cfg:   kiln.ServerConfig{Outbox: ob},
				close: func() {
					keys, _ := rdb.Keys(context.Background(), "{"+schema+"}:*").Result()
					for len(keys) > 0 {
						n := min(len(keys), 1000)
						rdb.Unlink(context.Background(), keys[:n]...)
						keys = keys[n:]
					}
					rdb.Close()
				},
			}, nil
		}},
	}
}

func runLoad(ctx context.Context, o options, w io.Writer) error {
	admin, err := pgx.Connect(ctx, o.url)
	if err != nil {
		return err
	}
	defer admin.Close(context.Background())
	var rows []string
	var base counters
	for i, m := range loadModes(o) {
		schema := fmt.Sprintf("bench_load_%d", i)
		c, err := measureLoad(ctx, o, admin, m, schema)
		admin.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+schema+" CASCADE")
		if err != nil {
			return fmt.Errorf("%s: %w", m.name, err)
		}
		if i == 0 {
			base = c
		}
		k := c.minus(base)
		n := float64(o.loadJobs)
		rows = append(rows, fmt.Sprintf("%s\t%.0f\t%.2f\t%.2f\t%.0f\t%.2f\t%.2f",
			m.name, float64(c.wal)/n, float64(c.commits)/n, float64(c.rows)/n,
			float64(k.wal)/n, float64(k.commits)/n, float64(k.rows)/n))
	}
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintf(tw, "%d orders, one job each\tWAL bytes\tcommits\trows written\tkiln WAL bytes\tkiln commits\tkiln rows\n", o.loadJobs)
	for _, r := range rows {
		fmt.Fprintln(tw, r)
	}
	return tw.Flush()
}

func measureLoad(ctx context.Context, o options, admin *pgx.Conn, m loadMode, schema string) (counters, error) {
	for _, q := range []string{
		"DROP SCHEMA IF EXISTS " + schema + " CASCADE",
		"CREATE SCHEMA " + schema,
		"CREATE TABLE " + schema + ".orders (id bigint PRIMARY KEY, note text NOT NULL)",
	} {
		if _, err := admin.Exec(ctx, q); err != nil {
			return counters{}, err
		}
	}
	pool, err := pgxpool.New(ctx, o.url)
	if err != nil {
		return counters{}, err
	}
	run, err := m.setup(ctx, pool, schema)
	if err != nil {
		pool.Close()
		return counters{}, err
	}
	var done atomic.Int64
	stop := func() {}
	var client *kiln.Client
	if run.store != nil {
		client = kiln.NewClient(run.store)
		mux := kiln.NewMux()
		kiln.Handle(mux, func(context.Context, *kiln.Job[placed]) error {
			done.Add(1)
			return nil
		})
		cfg := run.cfg
		cfg.Queues = map[string]int{kiln.DefaultQueue: o.workers}
		srv, err := kiln.NewServer(client, mux, cfg)
		if err != nil {
			return counters{}, err
		}
		sctx, cancel := context.WithCancel(ctx)
		exited := make(chan error, 1)
		go func() { exited <- srv.Run(sctx) }()
		stop = func() {
			cancel()
			<-exited
		}
		if !pause(ctx, 2*time.Second) {
			return counters{}, ctx.Err()
		}
	}
	shut := func() {
		stop()
		run.close()
		pool.Close()
	}
	if _, err := admin.Exec(ctx, "CHECKPOINT"); err != nil {
		shut()
		return counters{}, err
	}
	before, err := readCounters(ctx, admin)
	if err != nil {
		shut()
		return counters{}, err
	}

	per := o.loadJobs / o.writers
	err = parallel(o.writers, func(wi int) error {
		for i := range per {
			id := wi*per + i
			tx, err := pool.Begin(ctx)
			if err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, "INSERT INTO "+schema+".orders (id, note) VALUES ($1, 'bench')", id); err != nil {
				tx.Rollback(ctx)
				return err
			}
			var tw driver.TxWriter
			if client != nil {
				tw = run.bind(tx)
				if _, err := client.EnqueueTx(ctx, tw, placed{ID: id}); err != nil {
					tx.Rollback(ctx)
					return err
				}
			}
			if err := tx.Commit(ctx); err != nil {
				return err
			}
			if tw != nil {
				if err := tw.Notify(ctx); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		shut()
		return counters{}, err
	}
	want := int64(per * o.writers)
	for client != nil && done.Load() < want {
		if !pause(ctx, 20*time.Millisecond) {
			shut()
			return counters{}, ctx.Err()
		}
	}
	if !pause(ctx, 3*time.Second) {
		shut()
		return counters{}, ctx.Err()
	}
	shut()
	after, err := readCounters(ctx, admin)
	if err != nil {
		return counters{}, err
	}
	return after.minus(before), nil
}

func readCounters(ctx context.Context, admin *pgx.Conn) (counters, error) {
	var c counters
	if !pause(ctx, 1500*time.Millisecond) {
		return c, ctx.Err()
	}
	if _, err := admin.Exec(ctx, "SELECT pg_stat_force_next_flush()"); err != nil {
		return c, err
	}
	if _, err := admin.Exec(ctx, "SELECT pg_stat_clear_snapshot()"); err != nil {
		return c, err
	}
	err := admin.QueryRow(ctx, `SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::bigint,
		xact_commit, tup_inserted + tup_updated + tup_deleted
		FROM pg_stat_database WHERE datname = current_database()`).Scan(&c.wal, &c.commits, &c.rows)
	return c, err
}

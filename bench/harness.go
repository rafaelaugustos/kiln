package main

import (
	"context"
	"fmt"
	"io"
	"runtime"
	"runtime/debug"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
)

const schemaPrefix = "bench_"

type harness struct {
	o   options
	db  *pgxpool.Pool
	seq int
}

func newHarness(ctx context.Context, o options) (*harness, error) {
	cfg, err := pgxpool.ParseConfig(o.url)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = 2
	db, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	h := &harness{o: o, db: db}
	if err := h.dropStale(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return h, nil
}

func (h *harness) close() {
	h.db.Close()
}

func (h *harness) dropStale(ctx context.Context) error {
	rows, err := h.db.Query(ctx, "SELECT nspname FROM pg_namespace WHERE nspname LIKE $1", schemaPrefix+"%")
	if err != nil {
		return err
	}
	var stale []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return err
		}
		stale = append(stale, s)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, s := range stale {
		h.drop(ctx, s)
	}
	return nil
}

func (h *harness) drop(ctx context.Context, schema string) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
	defer cancel()
	if _, err := h.db.Exec(ctx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE"); err != nil {
		logger.Warn("drop schema", "schema", schema, "err", err)
	}
}

func (h *harness) measure(ctx context.Context, sc scenario, v variant) (result, error) {
	h.seq++
	schema := fmt.Sprintf("%s%d_%s", schemaPrefix, h.seq, v.id)
	if err := h.settle(ctx, ""); err != nil {
		return result{}, err
	}
	s := v.new()
	if err := s.open(ctx, h.o.url, schema); err != nil {
		h.drop(ctx, schema)
		return result{}, err
	}
	defer h.drop(ctx, schema)
	defer s.close()
	return sc.run(ctx, h, s)
}

func (h *harness) settle(ctx context.Context, table string) error {
	q := "VACUUM ANALYZE"
	if table != "" {
		q += " " + table
	}
	if _, err := h.db.Exec(ctx, q); err != nil {
		return fmt.Errorf("vacuum: %w", err)
	}
	if _, err := h.db.Exec(ctx, "CHECKPOINT"); err != nil {
		return fmt.Errorf("checkpoint: %w", err)
	}
	return nil
}

func (h *harness) count(ctx context.Context, query string) (int, error) {
	var n int
	err := h.db.QueryRow(ctx, query).Scan(&n)
	return n, err
}

func (h *harness) awaitCompleted(ctx context.Context, s system, n int, start time.Time) (time.Duration, error) {
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		done, err := h.count(ctx, s.completed())
		if err != nil {
			return 0, err
		}
		if done >= n {
			return time.Since(start), nil
		}
		if time.Since(start) > 10*time.Minute {
			return 0, fmt.Errorf("only %d of %d jobs completed after 10m", done, n)
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-tick.C:
		}
	}
}

func (h *harness) describe(ctx context.Context, w io.Writer) error {
	var pg string
	if err := h.db.QueryRow(ctx, "SELECT version()").Scan(&pg); err != nil {
		return err
	}
	fmt.Fprintf(w, "go        %s %s/%s, %d CPUs\n", runtime.Version(), runtime.GOOS, runtime.GOARCH, runtime.NumCPU())
	fmt.Fprintf(w, "postgres  %s\n", pg)
	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, m := range bi.Deps {
			switch m.Path {
			case "github.com/rafaelaugustos/kiln", "github.com/rafaelaugustos/kiln/pgstore",
				"github.com/riverqueue/river", "github.com/riverqueue/river/riverdriver/riverpgxv5",
				"github.com/jackc/pgx/v5":
				v := m.Version
				if m.Replace != nil {
					v += " => " + m.Replace.Path
				}
				fmt.Fprintf(w, "module    %s %s\n", m.Path, v)
			}
		}
	}
	fmt.Fprintf(w, "river     FetchCooldown %s, FetchPollInterval %s by default\n", river.FetchCooldownDefault, river.FetchPollIntervalDefault)
	fmt.Fprintf(w, "rounds    %d measured after 1 warm-up round\n\n", h.o.rounds)
	return nil
}

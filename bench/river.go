package main

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"
)

type riverSystem struct {
	fast     bool
	cooldown time.Duration

	schema  string
	pool    *pgxpool.Pool
	client  *river.Client[pgx.Tx]
	params  []river.InsertManyParams
	working bool
}

type riverWorker struct {
	river.WorkerDefaults[job]
	h handler
}

func (w *riverWorker) Work(ctx context.Context, j *river.Job[job]) error {
	w.h(ctx, j.Args.Seq)
	return nil
}

func (r *riverSystem) open(ctx context.Context, url, schema string) error {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return err
	}
	if _, err := pool.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		pool.Close()
		return err
	}
	drv := riverpgxv5.New(pool)
	m, err := rivermigrate.New(drv, &rivermigrate.Config{Schema: schema, Logger: logger})
	if err == nil {
		_, err = m.Migrate(ctx, rivermigrate.DirectionUp, nil)
	}
	if err == nil {
		r.client, err = river.NewClient(drv, r.config(schema))
	}
	if err != nil {
		pool.Close()
		return err
	}
	r.schema, r.pool = schema, pool
	return nil
}

func (r *riverSystem) config(schema string) *river.Config {
	return &river.Config{
		Schema:        schema,
		Logger:        logger,
		FetchCooldown: r.cooldown,
	}
}

func (r *riverSystem) close() {
	if r.working {
		r.stop(context.Background())
	}
	r.pool.Close()
}

func (r *riverSystem) insertMany(ctx context.Context, n int) error {
	if len(r.params) != n {
		r.params = make([]river.InsertManyParams, n)
		for i := range r.params {
			r.params[i] = river.InsertManyParams{Args: job{}}
		}
	}
	if r.fast {
		_, err := r.client.InsertManyFast(ctx, r.params)
		return err
	}
	_, err := r.client.InsertMany(ctx, r.params)
	return err
}

func (r *riverSystem) insert(ctx context.Context, seq int) error {
	_, err := r.client.Insert(ctx, job{Seq: seq}, nil)
	return err
}

func (r *riverSystem) start(ctx context.Context, workers int, h handler) error {
	ws := river.NewWorkers()
	river.AddWorker(ws, &riverWorker{h: h})
	cfg := r.config(r.schema)
	cfg.Workers = ws
	cfg.Queues = map[string]river.QueueConfig{river.QueueDefault: {MaxWorkers: workers}}
	c, err := river.NewClient(riverpgxv5.New(r.pool), cfg)
	if err != nil {
		return err
	}
	if err := c.Start(context.WithoutCancel(ctx)); err != nil {
		return err
	}
	r.client, r.working = c, true
	return nil
}

func (r *riverSystem) ready(context.Context) error {
	return nil
}

func (r *riverSystem) stop(ctx context.Context) error {
	if !r.working {
		return nil
	}
	r.working = false
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	return r.client.Stop(ctx)
}

func (r *riverSystem) table() string {
	return r.schema + ".river_job"
}

func (r *riverSystem) completed() string {
	return "SELECT count(*) FROM " + r.schema + ".river_job WHERE state = 'completed'"
}

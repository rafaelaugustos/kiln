package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/rafaelaugustos/kiln"
)

type recorder struct {
	inst string
	b    *backend
	seq  atomic.Int64
}

func work(o options) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go watchParent(ctx, cancel)
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	var b *backend
	err := retry(ctx, time.Hour, func(ctx context.Context) error {
		var err error
		if b, err = open(ctx, o, false); err != nil {
			log.Warn("soak: open", "err", err)
		}
		return err
	})
	if ctx.Err() != nil {
		return nil
	}
	if err != nil {
		return err
	}
	defer b.close()
	r := &recorder{inst: o.worker, b: b}
	m := kiln.NewMux()
	m.Use(r.record)
	handle(m)
	srv, err := kiln.NewServer(kiln.NewClient(b.store), m, kiln.ServerConfig{
		Name:              o.worker,
		Queues:            map[string]int{"default": 16, "slow": 32, "limited": 16},
		PollInterval:      b.poll,
		ShutdownTimeout:   3 * time.Second,
		KillGrace:         2 * time.Second,
		HeartbeatInterval: time.Second,
		DeadAfter:         20 * time.Second,
		LeaderTTL:         3 * time.Second,
		Retention:         kiln.Retention{Succeeded: kiln.Forever, Deleted: kiln.Forever, Failed: kiln.Forever},
		Logger:            log,
	})
	if err != nil {
		return err
	}
	return srv.Run(ctx)
}

func watchParent(ctx context.Context, cancel context.CancelFunc) {
	ppid := os.Getppid()
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if os.Getppid() != ppid {
				cancel()
				return
			}
		}
	}
}

func (r *recorder) record(next kiln.HandlerFunc) kiln.HandlerFunc {
	return func(ctx context.Context, j *kiln.RawJob) error {
		run := fmt.Sprintf("%s-%d", r.inst, r.seq.Add(1))
		started := time.Now().UnixMicro()
		err := retry(ctx, time.Minute, func(ctx context.Context) error {
			_, err := r.b.db.ExecContext(ctx, r.b.insertRun, run, j.ID, j.Attempt, r.inst, started)
			return err
		})
		if err != nil {
			return err
		}
		err = next(ctx, j)
		result := "ok"
		switch {
		case err == nil:
		case ctx.Err() != nil:
			result = "canceled"
		default:
			result = "error"
		}
		finished := time.Now().UnixMicro()
		retry(context.WithoutCancel(ctx), 30*time.Second, func(ctx context.Context) error {
			_, err := r.b.db.ExecContext(ctx, r.b.updateRun, finished, result, run)
			return err
		})
		return err
	}
}

func retry(ctx context.Context, limit time.Duration, f func(context.Context) error) error {
	deadline := time.Now().Add(limit)
	for wait := 50 * time.Millisecond; ; wait = min(2*wait, 2*time.Second) {
		c, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := f(c)
		cancel()
		if err == nil || time.Now().After(deadline) {
			return err
		}
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-time.After(wait):
		}
	}
}

package pgstore

import (
	"context"
	"math/rand/v2"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/rafaelaugustos/kiln/driver"
)

func (s *Store) Subscribe(ctx context.Context, fn func(driver.Event)) error {
	backoff := 50 * time.Millisecond
	for {
		s.watch(ctx, fn, func() { backoff = 50 * time.Millisecond })
		if ctx.Err() != nil {
			return nil
		}
		sleep := backoff/2 + rand.N(backoff/2+1)
		backoff = min(backoff*2, 5*time.Second)
		t := time.NewTimer(sleep)
		select {
		case <-ctx.Done():
			t.Stop()
			return nil
		case <-t.C:
		}
	}
}

func (s *Store) watch(ctx context.Context, fn func(driver.Event), ready func()) {
	conn, err := pgx.ConnectConfig(ctx, s.listen)
	if err != nil {
		return
	}
	defer conn.Close(context.WithoutCancel(ctx))
	jobs, cancel, queue := s.channel("jobs"), s.channel("cancel"), s.channel("queue")
	sql := "LISTEN " + jobs + "; LISTEN " + cancel + "; LISTEN " + queue
	if _, err := conn.PgConn().Exec(ctx, sql).ReadAll(); err != nil {
		return
	}
	ready()
	fn(driver.Event{Kind: driver.Resync})
	for {
		n, err := conn.WaitForNotification(ctx)
		if err != nil {
			return
		}
		switch n.Channel {
		case jobs:
			fn(driver.Event{Kind: driver.JobsReady, Queue: n.Payload})
		case cancel:
			id, err := strconv.ParseInt(strings.TrimSpace(n.Payload), 10, 64)
			if err == nil {
				fn(driver.Event{Kind: driver.CancelRequested, ID: id})
			}
		case queue:
			fn(driver.Event{Kind: driver.QueueChanged, Queue: n.Payload})
		}
	}
}

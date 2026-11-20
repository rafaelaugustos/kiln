package redisstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
	"github.com/redis/go-redis/v9"
)

const (
	backlog    = 1024
	minBackoff = 50 * time.Millisecond
	maxBackoff = 5 * time.Second
)

type inbox struct {
	events chan driver.Event
	resync chan struct{}
}

// Subscribe calls fn with the store's events until ctx is done, as [driver.Notifier] describes.
// The scripts that change the store publish their events on a Redis Pub/Sub channel named after
// the prefix, and Subscribe listens to it on a connection of its own, which it reopens after a
// failure. It returns an error only when the client has been closed.
func (s *Store) Subscribe(ctx context.Context, fn func(driver.Event)) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	in := inbox{events: make(chan driver.Event, backlog), resync: make(chan struct{}, 1)}
	done := make(chan error, 1)
	go func() { done <- s.listen(ctx, in) }()
	for {
		select {
		case <-ctx.Done():
			<-done
			return nil
		case err := <-done:
			return err
		case <-in.resync:
			fn(driver.Event{Kind: driver.Resync})
		case e := <-in.events:
			fn(e)
		}
	}
}

func (s *Store) listen(ctx context.Context, in inbox) error {
	backoff := minBackoff
	for {
		start := time.Now()
		subscribed, err := s.session(ctx, in)
		switch {
		case ctx.Err() != nil:
			return nil
		case errors.Is(err, redis.ErrClosed):
			return fmt.Errorf("kiln: subscribe: %w", err)
		case subscribed && time.Since(start) >= maxBackoff:
			backoff = minBackoff
		}
		t := time.NewTimer(backoff/2 + rand.N(backoff/2+1))
		select {
		case <-ctx.Done():
			t.Stop()
			return nil
		case <-t.C:
		}
		backoff = min(2*backoff, maxBackoff)
	}
}

func (s *Store) session(ctx context.Context, in inbox) (bool, error) {
	ps := s.c.Subscribe(ctx, s.prefix+"ev")
	defer ps.Close()
	stop := context.AfterFunc(ctx, func() { ps.Close() })
	defer stop()
	subscribed, pinged := false, false
	for {
		msg, err := ps.ReceiveTimeout(ctx, s.idle)
		switch {
		case err == nil:
			pinged = false
		case pinged || !errors.Is(err, os.ErrDeadlineExceeded):
			return subscribed, err
		default:
			if err := ps.Ping(ctx); err != nil {
				return subscribed, err
			}
			pinged = true
			continue
		}
		switch m := msg.(type) {
		case *redis.Subscription:
			subscribed = true
			signal(in.resync)
		case *redis.Message:
			for _, e := range decode(m.Payload) {
				select {
				case in.events <- e:
				default:
					signal(in.resync)
				}
			}
		}
	}
}

func signal(c chan<- struct{}) {
	select {
	case c <- struct{}{}:
	default:
	}
}

func decode(payload string) []driver.Event {
	var raw [][]any
	if json.Unmarshal([]byte(payload), &raw) != nil {
		return nil
	}
	out := make([]driver.Event, 0, len(raw))
	for _, r := range raw {
		if len(r) != 3 {
			continue
		}
		kind, _ := r[0].(float64)
		queue, _ := r[1].(string)
		id, _ := r[2].(string)
		e := driver.Event{Kind: driver.EventKind(kind), Queue: queue}
		if e.Kind == driver.CancelRequested {
			e.ID, _ = strconv.ParseInt(id, 10, 64)
		}
		if e.Kind >= driver.JobsReady && e.Kind <= driver.QueueChanged {
			out = append(out, e)
		}
	}
	return out
}

package redisbus

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
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

// Subscribe listens on the channel over a connection of its own and calls fn with each event until
// ctx is done, then returns nil. It calls fn with a Resync event after every subscription, and
// after dropping events because 1024 were already waiting for fn. A lost connection is replaced,
// with a jittered backoff from 50ms to 5s that starts over once a subscription has lasted 5s, and
// a connection quiet for 5s is pinged so that a dead one is noticed. Subscribe returns an error
// only when the Redis client has been closed.
func (b *Bus) Subscribe(ctx context.Context, fn func(driver.Event)) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	in := inbox{events: make(chan driver.Event, backlog), resync: make(chan struct{}, 1)}
	done := make(chan error, 1)
	go func() { done <- b.listen(ctx, in) }()
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

func (b *Bus) listen(ctx context.Context, in inbox) error {
	backoff := minBackoff
	for {
		start := time.Now()
		subscribed, err := b.session(ctx, in)
		switch {
		case ctx.Err() != nil:
			return nil
		case errors.Is(err, redis.ErrClosed):
			return fmt.Errorf("kiln: subscribe: %w", err)
		case subscribed && time.Since(start) >= maxBackoff:
			backoff = minBackoff
		}
		if !sleep(ctx, backoff/2+rand.N(backoff/2+1)) {
			return nil
		}
		backoff = min(2*backoff, maxBackoff)
	}
}

func (b *Bus) session(ctx context.Context, in inbox) (bool, error) {
	ps := b.c.Subscribe(ctx, b.channel)
	defer ps.Close()
	stop := context.AfterFunc(ctx, func() { ps.Close() })
	defer stop()
	subscribed, pinged := false, false
	for {
		msg, err := ps.ReceiveTimeout(ctx, b.idle)
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
			for e := range decode(m.Payload) {
				in.push(e)
			}
		}
	}
}

func (in inbox) push(e driver.Event) {
	select {
	case in.events <- e:
	default:
		signal(in.resync)
	}
}

func signal(c chan<- struct{}) {
	select {
	case c <- struct{}{}:
	default:
	}
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

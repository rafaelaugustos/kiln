package redisbus

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
	"github.com/redis/go-redis/v9"
)

var _ driver.Bus = (*Bus)(nil)

// Bus is a [driver.Bus] on one Redis Pub/Sub channel. It is safe for concurrent use.
type Bus struct {
	c       redis.UniversalClient
	channel string
	idle    time.Duration
}

// Option configures a [Bus].
type Option func(*Bus)

// Channel sets the Pub/Sub channel, "kiln" by default. Installations of kiln that share a Redis
// should each use their own channel, as they use their own tables; sharing one is harmless but
// wakes the servers of one installation for the jobs of the other.
func Channel(name string) Option {
	return func(b *Bus) { b.channel = name }
}

// New returns a Bus that publishes and subscribes through c, which can be a standalone, Sentinel
// or Cluster client. Set ContextTimeoutEnabled on c, so that the deadline of a Publish context
// also bounds its network reads and writes. Close c only after the kiln servers have stopped.
func New(c redis.UniversalClient, opts ...Option) *Bus {
	b := &Bus{c: c, channel: "kiln", idle: 5 * time.Second}
	for _, o := range opts {
		o(b)
	}
	return b
}

// Publish sends events to every subscriber of the channel in one message. It returns an error
// wrapping [driver.ErrInvalid] for an event kind it cannot encode, and the error of Redis when the
// message could not be published.
func (b *Bus) Publish(ctx context.Context, events []driver.Event) error {
	if len(events) == 0 {
		return nil
	}
	f := frames.Get().(*frame)
	defer f.free()
	var err error
	if f.b, err = appendEvents(f.b[:0], events); err != nil {
		return err
	}
	if err := b.c.Publish(ctx, b.channel, f).Err(); err != nil {
		return fmt.Errorf("kiln: publish: %w", err)
	}
	return nil
}

var frames = sync.Pool{New: func() any { return new(frame) }}

type frame struct{ b []byte }

func (f *frame) MarshalBinary() ([]byte, error) {
	return f.b, nil
}

func (f *frame) free() {
	if cap(f.b) <= 64<<10 {
		frames.Put(f)
	}
}

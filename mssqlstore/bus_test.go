package mssqlstore

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/rafaelaugustos/kiln/driver"
)

type bus struct {
	mu   sync.Mutex
	subs map[chan driver.Event]struct{}
	down error
}

func newBus() *bus {
	return &bus{subs: make(map[chan driver.Event]struct{})}
}

func (b *bus) Subscribe(ctx context.Context, fn func(driver.Event)) error {
	ch := make(chan driver.Event, 1024)
	b.mu.Lock()
	b.subs[ch] = struct{}{}
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		delete(b.subs, ch)
		b.mu.Unlock()
	}()
	fn(driver.Event{Kind: driver.Resync})
	for {
		select {
		case <-ctx.Done():
			return nil
		case e := <-ch:
			fn(e)
		}
	}
}

func (b *bus) Publish(_ context.Context, evs []driver.Event) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.down != nil {
		return b.down
	}
	for _, e := range evs {
		for ch := range b.subs {
			select {
			case ch <- e:
			default:
			}
		}
	}
	return nil
}

func TestSubscribeNeedsBus(t *testing.T) {
	t.Parallel()
	err := (&Store{}).Subscribe(context.Background(), func(e driver.Event) { t.Errorf("event %+v without a bus", e) })
	if !errors.Is(err, errors.ErrUnsupported) {
		t.Fatalf("subscribe without a bus: %v, want errors.ErrUnsupported", err)
	}
}

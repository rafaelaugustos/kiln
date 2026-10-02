package sqlitestore

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/rafaelaugustos/kiln/driver"
)

var errClosed = errors.New("kiln: store closed")

type hub struct {
	mu     sync.Mutex
	subs   map[*sub]struct{}
	n      atomic.Int32
	closed chan struct{}
	once   sync.Once
	bus    driver.Bus
	relay  *relay
}

type sub struct {
	events chan driver.Event
	resync chan struct{}
}

func newHub(b driver.Bus) *hub {
	h := &hub{subs: make(map[*sub]struct{}), closed: make(chan struct{}), bus: b}
	if b != nil {
		h.relay = newRelay(b)
	}
	return h
}

// Subscribe calls fn with the events of the writes made through this Store, and with those of the
// bus given with [Bus], until ctx is done, then returns nil. It returns sooner when the Store is
// closed, with an error, and when the bus's Subscribe returns, with that call's error. Each
// subscriber has a buffer of 256 events; when one falls behind, the events that do not fit are
// dropped and fn gets a [driver.Resync] instead.
func (s *Store) Subscribe(ctx context.Context, fn func(driver.Event)) error {
	h := s.hub
	sb := &sub{events: make(chan driver.Event, 256), resync: make(chan struct{}, 1)}
	h.mu.Lock()
	h.subs[sb] = struct{}{}
	h.n.Add(1)
	h.mu.Unlock()
	defer func() {
		h.mu.Lock()
		delete(h.subs, sb)
		h.n.Add(-1)
		h.mu.Unlock()
	}()
	var (
		remote <-chan struct{}
		err    error
	)
	if h.bus != nil {
		bctx, cancel := context.WithCancel(ctx)
		done := make(chan struct{})
		go func() {
			defer close(done)
			err = h.bus.Subscribe(bctx, func(e driver.Event) { sb.send(e) })
		}()
		defer func() {
			cancel()
			<-done
		}()
		remote = done
	}
	fn(driver.Event{Kind: driver.Resync})
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-h.closed:
			return errClosed
		case <-remote:
			if ctx.Err() != nil {
				return nil
			}
			return err
		case e := <-sb.events:
			fn(e)
		case <-sb.resync:
			fn(driver.Event{Kind: driver.Resync})
		}
	}
}

func (h *hub) publish(evs ...driver.Event) {
	h.relay.post(evs)
	h.deliver(evs)
}

func (h *hub) deliver(evs []driver.Event) {
	if len(evs) == 0 || h.n.Load() == 0 {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for sb := range h.subs {
		sb.send(evs...)
	}
}

func (sb *sub) send(evs ...driver.Event) {
	for _, e := range evs {
		select {
		case sb.events <- e:
		default:
			select {
			case sb.resync <- struct{}{}:
			default:
			}
			return
		}
	}
}

func (h *hub) wanted() bool {
	return h.relay != nil || h.n.Load() > 0
}

func (h *hub) ready(queues []string) {
	if len(queues) == 0 || !h.wanted() {
		return
	}
	h.publish(jobsReady(queues)...)
}

func (h *hub) cancel(ids []int64) {
	if len(ids) == 0 || !h.wanted() {
		return
	}
	evs := make([]driver.Event, len(ids))
	for i, id := range ids {
		evs[i] = driver.Event{Kind: driver.CancelRequested, ID: id}
	}
	h.publish(evs...)
}

func (h *hub) notify(ctx context.Context, queues []string) error {
	if len(queues) == 0 {
		return nil
	}
	evs := jobsReady(queues)
	h.deliver(evs)
	if h.bus == nil {
		return nil
	}
	if err := h.bus.Publish(ctx, evs); err != nil {
		return fmt.Errorf("kiln: notify: %w", err)
	}
	return nil
}

func jobsReady(queues []string) []driver.Event {
	evs := make([]driver.Event, len(queues))
	for i, q := range queues {
		evs[i] = driver.Event{Kind: driver.JobsReady, Queue: q}
	}
	return evs
}

func (h *hub) close() {
	h.once.Do(func() { close(h.closed) })
	h.relay.close()
}

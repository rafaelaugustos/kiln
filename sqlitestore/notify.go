package sqlitestore

import (
	"context"
	"errors"
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
}

type sub struct {
	events chan driver.Event
	resync chan struct{}
}

func newHub() *hub {
	return &hub{subs: make(map[*sub]struct{}), closed: make(chan struct{})}
}

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
	fn(driver.Event{Kind: driver.Resync})
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-h.closed:
			return errClosed
		case e := <-sb.events:
			fn(e)
		case <-sb.resync:
			fn(driver.Event{Kind: driver.Resync})
		}
	}
}

func (h *hub) publish(evs ...driver.Event) {
	if len(evs) == 0 || h.n.Load() == 0 {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for sb := range h.subs {
		sb.send(evs)
	}
}

func (sb *sub) send(evs []driver.Event) {
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

func (h *hub) ready(queues []string) {
	if len(queues) == 0 || h.n.Load() == 0 {
		return
	}
	evs := make([]driver.Event, len(queues))
	for i, q := range queues {
		evs[i] = driver.Event{Kind: driver.JobsReady, Queue: q}
	}
	h.publish(evs...)
}

func (h *hub) cancel(ids []int64) {
	if len(ids) == 0 || h.n.Load() == 0 {
		return
	}
	evs := make([]driver.Event, len(ids))
	for i, id := range ids {
		evs[i] = driver.Event{Kind: driver.CancelRequested, ID: id}
	}
	h.publish(evs...)
}

func (h *hub) close() {
	h.once.Do(func() { close(h.closed) })
}

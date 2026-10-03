package mysqlstore

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

var errNoBus = fmt.Errorf("kiln: subscribe: no bus configured: %w", errors.ErrUnsupported)

// Subscribe hands fn the events of the bus given with [Bus], through the bus's own Subscribe.
// Without a bus it returns at once with an error wrapping [errors.ErrUnsupported], and servers
// rely on polling.
func (s *Store) Subscribe(ctx context.Context, fn func(driver.Event)) error {
	if s.bus == nil {
		return errNoBus
	}
	return s.bus.Subscribe(ctx, fn)
}

type wake struct {
	queues []string
	late   map[string]rule
}

func (w *wake) merge(o wake) {
	w.queues = merge(w.queues, o.queues...)
	if len(o.late) == 0 {
		return
	}
	if w.late == nil {
		w.late = make(map[string]rule, len(o.late))
	}
	maps.Copy(w.late, o.late)
}

func (w *wake) hold(keys []string) {
	for _, key := range keys {
		if _, ok := w.late[key]; ok {
			continue
		}
		if w.late == nil {
			w.late = make(map[string]rule, len(keys))
		}
		w.late[key] = rule{}
	}
}

func (s *Store) admitLate(ctx context.Context, w *wake) error {
	if len(w.late) == 0 {
		return nil
	}
	a, err := s.admitKeys(ctx, w.late)
	if err != nil {
		return fmt.Errorf("kiln: admit: %w", err)
	}
	w.queues = merge(w.queues, a.queues...)
	return nil
}

func (s *Store) publish(ctx context.Context, queues []string) error {
	if s.bus == nil || len(queues) == 0 {
		return nil
	}
	evs := make([]driver.Event, len(queues))
	for i, q := range queues {
		evs[i] = driver.Event{Kind: driver.JobsReady, Queue: q}
	}
	if err := s.bus.Publish(ctx, evs); err != nil {
		return fmt.Errorf("kiln: notify: %w", err)
	}
	return nil
}

type notifier struct {
	bus     driver.Bus
	mu      sync.Mutex
	pending map[driver.Event]struct{}
	kick    chan struct{}
	stop    chan struct{}
	done    chan struct{}
	once    sync.Once
}

func newNotifier(b driver.Bus) *notifier {
	n := &notifier{
		bus:     b,
		pending: make(map[driver.Event]struct{}),
		kick:    make(chan struct{}, 1),
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
	go n.run()
	return n
}

func (n *notifier) ready(queues []string) {
	if n == nil || len(queues) == 0 {
		return
	}
	n.mu.Lock()
	for _, q := range queues {
		n.pending[driver.Event{Kind: driver.JobsReady, Queue: q}] = struct{}{}
	}
	n.mu.Unlock()
	n.signal()
}

func (n *notifier) cancel(ids []int64) {
	if n == nil || len(ids) == 0 {
		return
	}
	n.mu.Lock()
	for _, id := range ids {
		n.pending[driver.Event{Kind: driver.CancelRequested, ID: id}] = struct{}{}
	}
	n.mu.Unlock()
	n.signal()
}

func (n *notifier) changed(queue string) {
	if n == nil {
		return
	}
	n.mu.Lock()
	n.pending[driver.Event{Kind: driver.QueueChanged, Queue: queue}] = struct{}{}
	n.mu.Unlock()
	n.signal()
}

func (n *notifier) signal() {
	select {
	case n.kick <- struct{}{}:
	default:
	}
}

func (n *notifier) run() {
	defer close(n.done)
	for {
		select {
		case <-n.stop:
			n.flush()
			return
		case <-n.kick:
			n.flush()
		}
	}
}

func (n *notifier) flush() {
	n.mu.Lock()
	if len(n.pending) == 0 {
		n.mu.Unlock()
		return
	}
	evs := slices.Collect(maps.Keys(n.pending))
	clear(n.pending)
	n.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	n.bus.Publish(ctx, evs)
}

func (n *notifier) close() {
	if n == nil {
		return
	}
	n.once.Do(func() {
		close(n.stop)
		<-n.done
	})
}

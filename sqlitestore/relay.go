package sqlitestore

import (
	"context"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

type relay struct {
	bus     driver.Bus
	mu      sync.Mutex
	pending map[driver.Event]struct{}
	kick    chan struct{}
	stop    chan struct{}
	done    chan struct{}
	once    sync.Once
}

func newRelay(b driver.Bus) *relay {
	r := &relay{
		bus:     b,
		pending: make(map[driver.Event]struct{}),
		kick:    make(chan struct{}, 1),
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
	go r.run()
	return r
}

func (r *relay) post(evs []driver.Event) {
	if r == nil || len(evs) == 0 {
		return
	}
	r.mu.Lock()
	for _, e := range evs {
		r.pending[e] = struct{}{}
	}
	r.mu.Unlock()
	select {
	case r.kick <- struct{}{}:
	default:
	}
}

func (r *relay) run() {
	defer close(r.done)
	for {
		select {
		case <-r.stop:
			r.flush()
			return
		case <-r.kick:
			r.flush()
		}
	}
}

func (r *relay) flush() {
	r.mu.Lock()
	if len(r.pending) == 0 {
		r.mu.Unlock()
		return
	}
	evs := slices.Collect(maps.Keys(r.pending))
	clear(r.pending)
	r.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r.bus.Publish(ctx, evs)
}

func (r *relay) close() {
	if r == nil {
		return
	}
	r.once.Do(func() {
		close(r.stop)
		<-r.done
	})
}

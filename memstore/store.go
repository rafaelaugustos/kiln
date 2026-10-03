package memstore

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

var (
	_ driver.Store       = (*Store)(nil)
	_ driver.Notifier    = (*Store)(nil)
	_ driver.Transactor  = (*Store)(nil)
	_ driver.LimitReader = (*Store)(nil)
	_ driver.Console     = (*Store)(nil)
	_ driver.TxWriter    = (*Tx)(nil)
)

const nstates = len(driver.States)

// Option configures a [Store].
type Option func(*Store)

// Clock makes the store take the time from now instead of [time.Now]. Run times, leases, unique
// key windows, retention and statistics all follow it, so a test can move it forward to make a
// delayed job due or a lease expire. now must be safe for concurrent use.
func Clock(now func() time.Time) Option {
	return func(s *Store) { s.clock = now }
}

// Store is a [driver.Store] kept in memory, which also implements [driver.Notifier],
// [driver.Transactor], [driver.LimitReader] and [driver.Console]. It is safe for concurrent use.
// Every write runs under one lock, so it takes effect all at once, even a Delete or Requeue of many
// jobs. The store
// copies the values it is given and those it returns, so callers may change them afterwards.
type Store struct {
	mu    sync.Mutex
	clock func() time.Time

	jobSeq    int64
	batchSeq  int64
	jobs      map[int64]*job
	states    [nstates]map[int64]*job
	queues    map[string]*queue
	due       jobHeap
	limits    map[string]*throttle
	uniques   map[string]*uniq
	batches   map[int64]*batch
	servers   map[string]driver.ServerInfo
	running   map[string]map[int64]*job
	leases    map[string]*lease
	recurring map[string]*driver.Recurring
	stats     map[bucket]*counters
	total     counters

	now      time.Time
	server   string
	ready    []string
	events   []driver.Event
	resolved []change
	done     []*batch
	admit    []string

	subMu     sync.Mutex
	subs      map[*sub]struct{}
	listeners atomic.Int32
}

// New returns an empty Store.
func New(opts ...Option) *Store {
	s := &Store{
		clock:     time.Now,
		jobs:      make(map[int64]*job),
		queues:    make(map[string]*queue),
		due:       jobHeap{less: byRunAt},
		limits:    make(map[string]*throttle),
		uniques:   make(map[string]*uniq),
		batches:   make(map[int64]*batch),
		servers:   make(map[string]driver.ServerInfo),
		running:   make(map[string]map[int64]*job),
		leases:    make(map[string]*lease),
		recurring: make(map[string]*driver.Recurring),
		stats:     make(map[bucket]*counters),
		subs:      make(map[*sub]struct{}),
	}
	for i := range s.states {
		s.states[i] = make(map[int64]*job)
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// Now returns the time on the store's clock, which is [time.Now] unless [Clock] set another. It
// never fails.
func (s *Store) Now(context.Context) (time.Time, error) {
	return s.clock(), nil
}

func (s *Store) begin() {
	s.mu.Lock()
	s.now = s.clock()
	s.server = ""
}

func (s *Store) end() {
	var evs []driver.Event
	if s.listeners.Load() > 0 {
		evs = s.events
		for _, q := range s.ready {
			evs = append(evs, driver.Event{Kind: driver.JobsReady, Queue: q})
		}
	}
	s.events = nil
	s.ready = s.ready[:0]
	s.mu.Unlock()
	s.publish(evs)
}

func limitOr(n, def int) int {
	if n <= 0 {
		return def
	}
	return n
}

func pop[T any](q *[]T) T {
	var zero T
	n := len(*q) - 1
	v := (*q)[n]
	(*q)[n] = zero
	*q = (*q)[:n]
	return v
}

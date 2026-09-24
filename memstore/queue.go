package memstore

import (
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

type queue struct {
	paused bool
	row    bool
	counts [nstates]int64
	ready  map[string]*jobHeap
}

func (s *Store) queue(name string) *queue {
	q := s.queues[name]
	if q == nil {
		q = &queue{ready: make(map[string]*jobHeap)}
		s.queues[name] = q
	}
	return q
}

func (q *queue) heapFor(kind string) *jobHeap {
	h := q.ready[kind]
	if h == nil {
		h = &jobHeap{less: byPriority}
		q.ready[kind] = h
	}
	return h
}

func (q *queue) next(kinds []string) *job {
	var best *job
	if len(kinds) == 0 {
		for _, h := range q.ready {
			best = better(best, h.peek())
		}
		return best
	}
	for _, k := range kinds {
		if h := q.ready[k]; h != nil {
			best = better(best, h.peek())
		}
	}
	return best
}

func better(a, b *job) *job {
	if a == nil || b != nil && byPriority(b, a) {
		return b
	}
	return a
}

func (q *queue) live() int64 {
	var n int64
	for i, c := range q.counts {
		if !driver.States[i].Archived() {
			n += c
		}
	}
	return n
}

func (q *queue) latency(now time.Time) time.Duration {
	var oldest time.Time
	for _, h := range q.ready {
		for _, j := range h.jobs {
			if oldest.IsZero() || j.runAt.Before(oldest) {
				oldest = j.runAt
			}
		}
	}
	if oldest.IsZero() {
		return 0
	}
	return max(now.Sub(oldest), 0)
}

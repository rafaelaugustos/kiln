package memstore

import (
	"slices"

	"github.com/rafaelaugustos/kiln/driver"
)

type throttle struct {
	max     int
	active  int
	refs    int
	waiting jobHeap
}

func (s *Store) limitFor(j *job) *throttle {
	l := s.limits[j.limit]
	if l == nil {
		l = &throttle{max: j.limitMax, waiting: jobHeap{less: byPriority}}
		s.limits[j.limit] = l
	}
	return l
}

func (s *Store) admitting(key string) {
	if !slices.Contains(s.admit, key) {
		s.admit = append(s.admit, key)
	}
}

func (s *Store) admitKey(key string) int {
	l := s.limits[key]
	n := 0
	for l.active < l.max {
		j := l.waiting.peek()
		if j == nil {
			break
		}
		s.move(j, driver.Enqueued)
		n++
	}
	return n
}

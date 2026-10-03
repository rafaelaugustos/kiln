package memstore

import (
	"slices"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

const admitBatch = 1000

type rule struct {
	max   int
	rate  int
	per   time.Duration
	burst int
}

type throttle struct {
	rule
	active   int
	reserved int
	refs     int
	tat      time.Time
	admitTat time.Time
	waiting  jobHeap
}

func ruleOf(p *driver.InsertParams) rule {
	return rule{max: p.LimitMax, rate: p.LimitRate, per: p.LimitPer, burst: p.LimitBurst}
}

func (s *Store) limitFor(key string) *throttle {
	l := s.limits[key]
	if l == nil {
		l = &throttle{waiting: jobHeap{less: byPriority}}
		s.limits[key] = l
	}
	return l
}

func (l *throttle) full() bool {
	return l.max > 0 && l.active >= l.max
}

func (l *throttle) interval() time.Duration {
	return l.per / time.Duration(l.rate)
}

func (l *throttle) tau() time.Duration {
	return time.Duration(l.burst-1) * l.interval()
}

func (l *throttle) slot(now time.Time) time.Time {
	return later(now, l.tat.Add(-l.tau()))
}

func (l *throttle) take(at time.Time) {
	l.tat = later(l.tat, at).Add(l.interval())
}

func (l *throttle) open(now time.Time) bool {
	return !l.admitTat.Add(-l.tau() - l.interval()/2).After(now)
}

func (l *throttle) admit(now time.Time) {
	l.admitTat = later(l.admitTat, now).Add(l.interval())
	l.tat = later(l.tat, l.admitTat)
}

func later(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

func (s *Store) admitting(key string) {
	if !slices.Contains(s.admit, key) {
		s.admit = append(s.admit, key)
	}
}

func (s *Store) admitKey(key string, most int) int {
	l := s.limits[key]
	n := 0
	for ; n < most && (l.rate == 0 || n < admitBatch); n++ {
		j := l.waiting.peek()
		if j == nil || j.granted && l.full() {
			break
		}
		rated := l.rate > 0 && (!j.granted || !l.open(s.now))
		at := s.now
		if rated {
			at = l.slot(s.now)
		}
		if at.After(s.now) {
			l.take(at)
			j.granted, j.runAt = true, at
			s.move(j, driver.Scheduled)
			continue
		}
		if l.full() {
			break
		}
		if rated {
			l.take(at)
		}
		if l.rate > 0 {
			l.admit(s.now)
		}
		j.granted = false
		s.move(j, driver.Enqueued)
	}
	return n
}

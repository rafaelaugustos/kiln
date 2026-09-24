package kiln

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

const stuckAfter = 30 * time.Second

type Stats struct {
	Running      int
	Capacity     int
	Claimed      uint64
	Succeeded    uint64
	Failed       uint64
	Retried      uint64
	Snoozed      uint64
	Canceled     uint64
	Abandoned    uint64
	Stale        uint64
	Busy         uint64
	EmptyFetches uint64
	Pending      int
	HeartbeatAge time.Duration
	Leader       bool
	Fenced       bool
	Listening    bool
}

type counters struct {
	claimed      atomic.Uint64
	succeeded    atomic.Uint64
	failed       atomic.Uint64
	retried      atomic.Uint64
	snoozed      atomic.Uint64
	canceled     atomic.Uint64
	abandoned    atomic.Uint64
	stale        atomic.Uint64
	busy         atomic.Uint64
	emptyFetches atomic.Uint64
}

func (c *counters) count(o *driver.Outcome) {
	switch o.State {
	case driver.Succeeded:
		c.succeeded.Add(1)
	case driver.Failed:
		c.failed.Add(1)
	case driver.Deleted:
		c.canceled.Add(1)
	case driver.Scheduled:
		if o.Refund {
			c.snoozed.Add(1)
		} else {
			c.retried.Add(1)
		}
	}
}

func (s *Server) Stats() Stats {
	st := Stats{
		Capacity:     s.capacity,
		Claimed:      s.stats.claimed.Load(),
		Succeeded:    s.stats.succeeded.Load(),
		Failed:       s.stats.failed.Load(),
		Retried:      s.stats.retried.Load(),
		Snoozed:      s.stats.snoozed.Load(),
		Canceled:     s.stats.canceled.Load(),
		Abandoned:    s.stats.abandoned.Load(),
		Stale:        s.stats.stale.Load(),
		Busy:         s.stats.busy.Load(),
		EmptyFetches: s.stats.emptyFetches.Load(),
		Pending:      s.comp.backlog(),
		Leader:       s.leader.Load(),
		Fenced:       s.fenced.Load(),
		Listening:    s.listening.Load(),
	}
	for _, p := range s.prods {
		st.Running += int(p.running.Load())
	}
	if b := s.lastBeat.Load(); b != 0 {
		st.HeartbeatAge = time.Since(time.Unix(0, b))
	}
	return st
}

func (s *Server) Healthy() error {
	if s.fenced.Load() {
		return errors.New("kiln: server is fenced after failed heartbeats")
	}
	b := s.lastBeat.Load()
	if b == 0 {
		return errors.New("kiln: server is not running")
	}
	if age := time.Since(time.Unix(0, b)); age > s.cfg.DeadAfter/2 {
		return fmt.Errorf("kiln: last heartbeat %s ago", age.Round(time.Millisecond))
	}
	if since := s.comp.since.Load(); since != 0 {
		if age := time.Since(time.Unix(0, since)); age > stuckAfter {
			return fmt.Errorf("kiln: outcomes pending for %s", age.Round(time.Millisecond))
		}
	}
	return nil
}

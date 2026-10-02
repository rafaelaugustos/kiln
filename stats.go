package kiln

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

const stuckAfter = 30 * time.Second

// Stats is a snapshot of a server's activity. The counters start at zero with the server and only
// grow; those about attempts count outcomes once the store has applied them.
type Stats struct {
	Running      int           // jobs claimed and not finished yet
	Capacity     int           // workers across all pools
	Claimed      uint64        // jobs claimed
	Succeeded    uint64        // attempts that succeeded
	Failed       uint64        // attempts that left their job failed
	Retried      uint64        // failed attempts scheduled to run again
	Snoozed      uint64        // attempts rescheduled without counting, as by Snooze
	Canceled     uint64        // attempts that left their job deleted
	Abandoned    uint64        // jobs handed back at shutdown while their handlers still ran
	Stale        uint64        // outcomes dropped because the job had moved on
	Busy         uint64        // outcomes sent again because the job's row was locked
	EmptyFetches uint64        // claims that found no jobs
	Pending      int           // finished jobs whose outcome is not stored yet
	HeartbeatAge time.Duration // age of the last successful heartbeat; zero when there is none
	Leader       bool          // the server is the leader
	Fenced       bool          // the server stopped work after failing to heartbeat
	Listening    bool          // the store's notifications are reaching the server
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

// Stats returns a snapshot of the server's counters and state.
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

// Healthy returns an error when the server should not be trusted with work: it is not running or
// has not managed a first heartbeat, it has fenced itself, its last heartbeat is older than half
// of DeadAfter, or outcomes have waited more than 30s to be stored. It suits readiness and
// liveness probes.
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

package kiln

import (
	"context"
	"slices"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

func (s *Server) join(ctx context.Context) bool {
	delay := 100 * time.Millisecond
	for s.beat(ctx) != nil {
		if !sleep(ctx, delay) {
			return false
		}
		delay = min(2*delay, s.cfg.HeartbeatInterval)
	}
	return true
}

func (s *Server) heartbeat(ctx context.Context) {
	tick := time.NewTicker(s.cfg.HeartbeatInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		case <-s.beatNow:
		}
		s.beat(ctx)
	}
}

func (s *Server) beat(ctx context.Context) error {
	seq := s.seq.Add(1)
	ctx, cancel := context.WithTimeout(ctx, s.cfg.HeartbeatInterval)
	d, err := s.store.Heartbeat(ctx, s.info())
	cancel()
	if err != nil {
		s.fail("heartbeat", err)
		return err
	}
	s.unfence()
	if old := s.paused.Load(); old == nil || !slices.Equal(*old, d.Paused) {
		s.paused.Store(&d.Paused)
		if old != nil {
			s.pokeAll()
		}
	}
	s.reconcile(d.Leases, seq)
	return nil
}

func (s *Server) info() driver.ServerInfo {
	info := driver.ServerInfo{
		ID:        s.id,
		Host:      s.cfg.Name,
		PID:       s.pid,
		Version:   version,
		Queues:    s.queues,
		Kinds:     s.mux.kinds,
		Workers:   s.capacity,
		StartedAt: s.startedAt,
	}
	for _, p := range s.prods {
		info.Running += int(p.running.Load())
	}
	return info
}

func (s *Server) reconcile(leases []driver.Lease, seq uint64) {
	var lost []pending
	held := make(map[int64]int32, len(leases))
	s.mu.Lock()
	for _, l := range leases {
		held[l.ID] = l.Claim
		t := s.tasks[l.ID]
		switch {
		case t != nil:
			if l.Cancel && t.ref.Claim == l.Claim && !t.settled.Load() {
				t.cancel(ErrCanceled)
			}
		case l.Age > 2*s.cfg.HeartbeatInterval:
			lost = append(lost, pending{Outcome: driver.Outcome{Ref: l.Ref, State: driver.Enqueued, Reason: "lost", Error: ErrLost.Error()}})
		}
	}
	for id, t := range s.tasks {
		if claim, ok := held[id]; (!ok || claim != t.ref.Claim) && t.seq < seq && !t.settled.Load() {
			t.cancel(errFenced)
		}
	}
	s.mu.Unlock()
	for _, p := range lost {
		s.comp.submit(p)
	}
	if len(lost) > 0 {
		s.log.Warn("kiln: requeued lost claims", "id", s.id, "count", len(lost))
	}
}

func (s *Server) fenceAfter() time.Duration {
	return s.cfg.DeadAfter - s.cfg.HeartbeatInterval - s.cfg.KillGrace
}

func (s *Server) fence() {
	s.fenceMu.Lock()
	defer s.fenceMu.Unlock()
	if s.fenced.Load() {
		return
	}
	if left := s.fenceAfter() - time.Since(time.Unix(0, s.lastBeat.Load())); left > 0 {
		s.fencer.Reset(left)
		return
	}
	s.fenced.Store(true)
	s.log.Warn("kiln: heartbeats failing, fencing server", "id", s.id)
	s.cancelAll(errFenced)
}

func (s *Server) unfence() {
	s.fenceMu.Lock()
	s.lastBeat.Store(time.Now().UnixNano())
	s.fencer.Reset(s.fenceAfter())
	restored := s.fenced.Swap(false)
	s.fenceMu.Unlock()
	if restored {
		s.log.Info("kiln: heartbeat restored", "id", s.id)
		s.pokeAll()
	}
}

func (s *Server) pokeAll() {
	for _, p := range s.prods {
		p.poke()
	}
}

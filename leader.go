package kiln

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

const (
	leaderLease = "kiln.leader"
	dueLimit    = 100
	orphanLimit = 1000
	recurRetry  = time.Minute
)

func (s *Server) lead(ctx context.Context) {
	var (
		stop    func()
		renewed time.Time
	)
	step := func() {
		if stop == nil {
			return
		}
		stop()
		stop = nil
		s.leader.Store(false)
		if ctx.Err() == nil {
			s.log.Info("kiln: leadership lost", "id", s.id)
		}
	}
	tick := time.NewTicker(s.cfg.LeaderTTL / 3)
	defer tick.Stop()
	for {
		lctx, cancel := context.WithTimeout(ctx, s.cfg.LeaderTTL/3)
		held, ok, err := s.store.Lead(lctx, leaderLease, s.id, s.cfg.LeaderTTL)
		cancel()
		switch {
		case err != nil:
			s.fail("lead", err)
			if time.Since(renewed) >= s.cfg.LeaderTTL {
				step()
			}
		case ok:
			renewed = time.Now()
			s.since.Store(renewed.Add(-held).UnixNano())
			if stop == nil {
				stop = s.govern(ctx)
			}
		default:
			step()
		}
		select {
		case <-ctx.Done():
			step()
			if !renewed.IsZero() {
				ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
				if err := s.store.Resign(ctx, leaderLease, s.id); err != nil {
					s.fail("resign", err)
				}
				cancel()
			}
			return
		case <-tick.C:
		}
	}
}

func (s *Server) govern(ctx context.Context) func() {
	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for _, f := range []func(context.Context){s.recur, s.rescue, s.sweep, s.prune} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f(ctx)
		}()
	}
	s.leader.Store(true)
	s.log.Info("kiln: leadership acquired", "id", s.id)
	return func() {
		cancel()
		wg.Wait()
	}
}

func (s *Server) recur(ctx context.Context) {
	every(ctx, func(ctx context.Context) time.Duration {
		rs, now, err := s.store.Due(ctx, dueLimit)
		if err != nil {
			s.fail("due", err)
			return time.Second
		}
		wait, moved := time.Second, false
		for _, r := range rs {
			f, err := planFire(r, now)
			if err != nil {
				s.fail("recurring", err)
				f = driver.Fire{ID: r.ID, Version: r.Version, NextRunAt: now.Add(recurRetry), LastRunAt: r.LastRunAt}
			}
			if _, err := s.store.Fire(ctx, f); err != nil {
				if !errors.Is(err, driver.ErrConflict) {
					s.fail("fire", err)
				}
				continue
			}
			moved = true
			if p := s.byQueue[r.Template.Queue]; p != nil && len(f.Jobs) > 0 {
				p.poke()
			}
			if !f.NextRunAt.IsZero() {
				wait = min(wait, f.NextRunAt.Sub(now))
			}
		}
		if moved && len(rs) == dueLimit {
			return 0
		}
		return max(wait, 0)
	})
}

func (s *Server) rescue(ctx context.Context) {
	every(ctx, func(ctx context.Context) time.Duration {
		if time.Since(time.Unix(0, s.since.Load())) >= s.cfg.DeadAfter+s.cfg.HeartbeatInterval {
			s.reap(ctx)
		}
		return s.cfg.HeartbeatInterval
	})
}

func (s *Server) reap(ctx context.Context) {
	orphans, err := s.store.Orphans(ctx, s.cfg.DeadAfter, orphanLimit)
	if err != nil {
		s.fail("orphans", err)
		return
	}
	if len(orphans) == 0 {
		return
	}
	outs := make([]driver.Outcome, len(orphans))
	for i, o := range orphans {
		outs[i] = s.orphaned(o)
	}
	slices.SortFunc(outs, func(a, b driver.Outcome) int { return cmp.Compare(a.ID, b.ID) })
	if _, err := s.store.Finish(ctx, s.id, outs); err != nil {
		s.fail("finish", err)
		return
	}
	s.log.Warn("kiln: rescued orphaned jobs", "id", s.id, "count", len(outs))
}

func (s *Server) orphaned(o driver.Orphan) driver.Outcome {
	err := fmt.Errorf("kiln: server %s stopped heartbeating", o.Server)
	out := driver.Outcome{Ref: o.Ref, Reason: "orphaned", Error: err.Error()}
	switch {
	case o.Cancel:
		out.State = driver.Deleted
	case o.Attempt >= o.MaxAttempts:
		out.State = driver.Failed
	default:
		out.State = driver.Scheduled
		out.Delay = s.retryDelay(o.Kind, o.Attempt, err)
	}
	return out
}

func (s *Server) retryDelay(kind string, attempt int, err error) (d time.Duration) {
	defer func() {
		if v := recover(); v != nil {
			if !s.muted("backoff") {
				s.log.Error("kiln: backoff panicked", "id", s.id, "kind", kind, "panic", v)
			}
			d = defaultBackoff(attempt, err)
		}
	}()
	return max(backoffFor(s.mux.routes[kind], s.cfg.Backoff)(attempt, err), 0)
}

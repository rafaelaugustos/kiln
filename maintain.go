package kiln

import (
	"context"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

const (
	promoteLimit = 1000
	blindPromote = 100 * time.Millisecond
	sweepLimit   = 1000
	pruneLimit   = 5000
	sweepEvery   = time.Second
	pruneEvery   = time.Minute
	keepServers  = time.Hour
	keepStats    = 14 * 24 * time.Hour
	callTimeout  = 15 * time.Second
)

func (s *Server) promote(ctx context.Context) {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		case <-s.promoteNow:
		}
		timer.Reset(s.promoteDue(ctx))
	}
}

func (s *Server) promoteDue(ctx context.Context) time.Duration {
	ctx, cancel := context.WithTimeout(ctx, max(s.cfg.PollInterval, s.cfg.HeartbeatInterval))
	p, err := s.store.Promote(ctx, promoteLimit)
	cancel()
	if err != nil {
		s.fail("promote", err)
		return jitter(s.cfg.PollInterval)
	}
	for _, q := range p.Queues {
		if pr := s.byQueue[q]; pr != nil {
			pr.poke()
		}
	}
	if p.Count >= promoteLimit {
		return 0
	}
	d := jitter(s.cfg.PollInterval)
	if !s.listening.Load() {
		d = min(d, blindPromote)
	}
	if p.Next > 0 {
		d = min(d, p.Next)
	}
	return d
}

func (s *Server) sweep(ctx context.Context) {
	every(ctx, func(ctx context.Context) time.Duration {
		n, err := s.store.Sweep(ctx, sweepLimit)
		if err != nil {
			s.fail("sweep", err)
		}
		if n >= sweepLimit {
			return 0
		}
		return sweepEvery
	})
}

func (s *Server) prune(ctx context.Context) {
	p := driver.PruneParams{Retention: s.cfg.Retention, Servers: keepServers, Stats: keepStats, Limit: pruneLimit}
	every(ctx, func(ctx context.Context) time.Duration {
		n, err := s.store.Prune(ctx, p)
		if err != nil {
			s.fail("prune", err)
		}
		if n >= pruneLimit {
			return 0
		}
		return pruneEvery
	})
}

func every(ctx context.Context, f func(context.Context) time.Duration) {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		fctx, cancel := context.WithTimeout(ctx, callTimeout)
		timer.Reset(f(fctx))
		cancel()
	}
}

func sleep(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

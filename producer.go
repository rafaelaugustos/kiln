package kiln

import (
	"cmp"
	"context"
	"slices"
	"sync/atomic"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

type producer struct {
	s       *Server
	queues  []string
	workers int
	batch   int
	eager   int
	running atomic.Int64
	dirty   atomic.Bool
	wake    chan struct{}
	open    []string
	tasks   []*task
}

func newProducer(s *Server, p Pool) *producer {
	batch := min(cmp.Or(s.cfg.FetchBatch, p.Workers), p.Workers, maxFetchBatch)
	return &producer{
		s:       s,
		queues:  p.Queues,
		workers: p.Workers,
		batch:   batch,
		eager:   max(batch/2, 1),
		wake:    make(chan struct{}, 1),
	}
}

func (p *producer) poke() {
	p.dirty.Store(true)
	signal(p.wake)
}

func (p *producer) release() {
	p.running.Add(-1)
	if p.dirty.Load() {
		signal(p.wake)
	}
}

func (p *producer) free() int {
	return p.workers - int(p.running.Load())
}

func (p *producer) run(ctx context.Context) {
	tick := time.NewTicker(p.s.cfg.PollInterval)
	defer tick.Stop()
	cool := time.NewTimer(time.Hour)
	cool.Stop()
	var (
		last time.Time
		full bool
	)
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.wake:
		case <-tick.C:
		}
		if !p.cooldown(ctx, cool, last, full) {
			return
		}
		select {
		case <-p.wake:
		default:
		}
		last = time.Now()
		full = p.fetch()
	}
}

func (p *producer) cooldown(ctx context.Context, cool *time.Timer, last time.Time, full bool) bool {
	for {
		wait := p.s.cfg.FetchCooldown - time.Since(last)
		if wait <= 0 || full && p.free() >= p.eager {
			return true
		}
		cool.Reset(wait)
		select {
		case <-ctx.Done():
			cool.Stop()
			return false
		case <-cool.C:
			return true
		case <-p.wake:
		}
	}
}

func (p *producer) fetch() bool {
	s := p.s
	if s.fenced.Load() {
		return false
	}
	p.dirty.Store(false)
	free := p.free()
	if free <= 0 {
		p.dirty.Store(true)
		if p.free() > 0 {
			signal(p.wake)
		}
		return true
	}
	queues := p.active()
	if len(queues) == 0 {
		return false
	}
	n := min(free, p.batch)
	ctx, cancel := context.WithTimeout(s.base, s.cfg.HeartbeatInterval)
	jobs, err := s.store.Claim(ctx, driver.ClaimQuery{Queues: queues, Kinds: s.mux.kinds, Limit: n, Server: s.id})
	cancel()
	if err != nil {
		s.fail("claim", err)
		return false
	}
	if len(jobs) == 0 {
		s.stats.emptyFetches.Add(1)
		return false
	}
	s.stats.claimed.Add(uint64(len(jobs)))
	p.running.Add(int64(len(jobs)))
	s.start(p, jobs)
	if len(jobs) < n {
		return false
	}
	p.dirty.Store(true)
	if p.free() > 0 {
		signal(p.wake)
	}
	return true
}

func (p *producer) active() []string {
	paused := p.s.paused.Load()
	if paused == nil || len(*paused) == 0 {
		return p.queues
	}
	p.open = p.open[:0]
	for _, q := range p.queues {
		if !slices.Contains(*paused, q) {
			p.open = append(p.open, q)
		}
	}
	return p.open
}

func signal(c chan struct{}) {
	select {
	case c <- struct{}{}:
	default:
	}
}

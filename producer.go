package kiln

import (
	"cmp"
	"context"
	"math/rand/v2"
	"slices"
	"sync/atomic"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

type producer struct {
	s        *Server
	queues   []string
	weights  []int
	workers  int
	batch    int
	eager    int
	running  atomic.Int64
	claiming atomic.Int64
	dirty    atomic.Bool
	wake     chan struct{}
	open     []string
	left     []int
	tasks    []*task
}

func newProducer(s *Server, pl Pool) *producer {
	batch := min(cmp.Or(s.cfg.FetchBatch, pl.Workers), pl.Workers, maxFetchBatch)
	p := &producer{
		s:       s,
		queues:  pl.Queues,
		workers: pl.Workers,
		batch:   batch,
		eager:   max(batch/2, 1),
		wake:    make(chan struct{}, 1),
	}
	if len(pl.Weights) > 0 {
		p.weights = make([]int, len(pl.Queues))
		for i, q := range pl.Queues {
			p.weights[i] = cmp.Or(pl.Weights[q], 1)
		}
	}
	return p
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
	p.claiming.Store(int64(time.Since(s.startedAt)) + 1)
	defer p.claiming.Store(0)
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
	var paused []string
	if ps := p.s.paused.Load(); ps != nil {
		paused = *ps
	}
	if len(paused) == 0 && p.weights == nil {
		return p.queues
	}
	p.open, p.left = p.open[:0], p.left[:0]
	for i, q := range p.queues {
		if !slices.Contains(paused, q) {
			p.open = append(p.open, q)
			if p.weights != nil {
				p.left = append(p.left, p.weights[i])
			}
		}
	}
	if p.weights != nil {
		draw(p.open, p.left)
	}
	return p.open
}

func draw(queues []string, weights []int) {
	total := 0
	for _, w := range weights {
		total += w
	}
	for i := range len(queues) - 1 {
		r, j := rand.IntN(total), i
		for r >= weights[j] {
			r -= weights[j]
			j++
		}
		queues[i], queues[j] = queues[j], queues[i]
		weights[i], weights[j] = weights[j], weights[i]
		total -= weights[i]
	}
}

func signal(c chan struct{}) {
	select {
	case c <- struct{}{}:
	default:
	}
}

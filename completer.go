package kiln

import (
	"cmp"
	"context"
	"slices"
	"sync/atomic"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

const (
	flushSize     = 256
	flushTimeout  = 15 * time.Second
	minFlushRetry = 10 * time.Millisecond
	maxFlushRetry = 5 * time.Second
)

type pending struct {
	driver.Outcome
	t         *task
	rewritten bool
}

type completer struct {
	s     *Server
	in    chan pending
	quit  chan struct{}
	done  chan struct{}
	buf   []pending
	outs  []driver.Outcome
	held  atomic.Int64
	since atomic.Int64
}

func newCompleter(s *Server, size int) *completer {
	return &completer{
		s:    s,
		in:   make(chan pending, size),
		quit: make(chan struct{}),
		done: make(chan struct{}),
		buf:  make([]pending, 0, flushSize),
		outs: make([]driver.Outcome, 0, flushSize),
	}
}

func (c *completer) submit(p pending) {
	select {
	case c.in <- p:
	case <-c.done:
	}
}

func (c *completer) stop() {
	close(c.quit)
	<-c.done
}

func (c *completer) backlog() int {
	return len(c.in) + int(c.held.Load())
}

func (c *completer) run() {
	defer close(c.done)
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	var retry time.Duration
	for {
		switch {
		case len(c.buf) == 0:
			select {
			case p := <-c.in:
				c.add(p)
			case <-c.quit:
				c.final()
				return
			}
		case retry > 0:
			timer.Reset(retry)
			select {
			case <-timer.C:
			case <-c.quit:
				timer.Stop()
				c.final()
				return
			}
		}
		c.drain()
		ctx, cancel := context.WithTimeout(c.s.base, flushTimeout)
		n, err := c.flush(ctx)
		cancel()
		retry = c.backoff(retry, n, err)
	}
}

func (c *completer) final() {
	ctx, cancel := context.WithTimeout(c.s.base, flushTimeout)
	defer cancel()
	var retry time.Duration
	for {
		c.drain()
		if len(c.buf) == 0 {
			return
		}
		n, err := c.flush(ctx)
		if ctx.Err() != nil {
			c.s.log.Error("kiln: outcomes dropped at shutdown", "id", c.s.id, "count", c.backlog())
			return
		}
		if retry = c.backoff(retry, n, err); retry > 0 && !sleep(ctx, retry) {
			return
		}
	}
}

func (c *completer) backoff(prev time.Duration, n int, err error) time.Duration {
	switch {
	case err != nil:
		c.s.fail("finish", err)
		return min(max(2*prev, minFlushRetry), maxFlushRetry)
	case n == 0:
		return minFlushRetry
	}
	return 0
}

func (c *completer) add(p pending) {
	if len(c.buf) == 0 {
		c.since.Store(time.Now().UnixNano())
	}
	c.buf = append(c.buf, p)
}

func (c *completer) drain() {
	for len(c.buf) < flushSize {
		select {
		case p := <-c.in:
			c.add(p)
		default:
			c.held.Store(int64(len(c.buf)))
			return
		}
	}
	c.held.Store(int64(len(c.buf)))
}

func (c *completer) flush(ctx context.Context) (int, error) {
	s := c.s
	slices.SortFunc(c.buf, func(a, b pending) int { return cmp.Compare(a.ID, b.ID) })
	c.outs = c.outs[:0]
	for i := range c.buf {
		c.outs = append(c.outs, c.buf[i].Outcome)
	}
	res, err := s.store.Finish(ctx, s.id, c.outs)
	clear(c.outs)
	if err != nil {
		return 0, err
	}
	keep := c.buf[:0]
	n, dropped, soon := 0, 0, false
	s.mu.Lock()
	for i, r := range res {
		p := c.buf[i]
		switch r {
		case driver.Busy:
			s.stats.busy.Add(1)
			keep = append(keep, p)
			continue
		case driver.Rejected:
			if !p.rewritten {
				p.State, p.Delay, p.Refund, p.Reason, p.Error, p.Trace, p.Output = driver.Failed, 0, false, "rejected", "kiln: outcome rejected", "", nil
				p.rewritten = true
				keep = append(keep, p)
				continue
			}
			dropped++
		case driver.Stale:
			s.stats.stale.Add(1)
		case driver.Applied:
			s.stats.count(&p.Outcome)
			soon = soon || p.State == driver.Scheduled && p.Delay > 0 && p.Delay < s.cfg.PollInterval || p.t != nil && p.t.limited
		}
		if p.t != nil && s.tasks[p.ID] == p.t {
			delete(s.tasks, p.ID)
		}
		n++
	}
	s.mu.Unlock()
	clear(c.buf[len(keep):])
	c.buf = keep
	c.held.Store(int64(len(c.buf)))
	if len(c.buf) == 0 {
		c.since.Store(0)
	}
	if soon {
		signal(s.promoteNow)
	}
	if dropped > 0 {
		s.log.Error("kiln: store rejected failure outcomes", "id", s.id, "count", dropped)
	}
	return n, nil
}

package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/rafaelaugustos/kiln"
)

const drainLimit = 10 * time.Minute

type harness struct {
	o      options
	b      *backend
	fleet  *fleet
	enq    *enqueuer
	limits []kiln.Limit
	dir    string
	start  time.Time

	out       sync.Mutex
	kills     atomic.Int64
	terms     atomic.Int64
	restarts  atomic.Int64
	restarted []int64
}

func soak(o options) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	b, err := open(ctx, o, true)
	if err != nil {
		return err
	}
	defer b.close()
	self, err := os.Executable()
	if err != nil {
		return err
	}
	dir, err := os.MkdirTemp("", "kiln-soak-")
	if err != nil {
		return err
	}
	h := &harness{o: o, b: b, dir: dir, start: time.Now(), limits: limitsFor(o.rate)}
	h.fleet = newFleet(self, dir, o)
	h.enq = &enqueuer{b: b, client: kiln.NewClient(b.store), rate: o.rate, limits: h.limits}
	restarts := h.o.container
	if restarts == "" {
		restarts = "off"
	}
	fmt.Printf("kiln soak on %s for %s: %d workers, %g enqueues/s, database restarts %s, logs in %s\n", o.backend, o.duration, o.workers, o.rate, restarts, dir)

	h.fleet.start()
	run, cancel := context.WithTimeout(ctx, o.duration)
	var wg sync.WaitGroup
	wg.Go(func() { h.enq.run(run) })
	wg.Go(func() { h.chaos(run) })
	wg.Go(func() { h.status(run) })
	wg.Wait()
	cancel()
	stop()

	h.event("stopped enqueuing after %d jobs, draining", h.enq.jobs.Load())
	drained, derr := h.drain()
	if derr != nil {
		h.event("%v", derr)
	}
	h.event("stopping workers")
	h.fleet.stop()
	vctx, vcancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer vcancel()
	r, err := h.verify(vctx)
	if err != nil {
		return err
	}
	r.drained, r.drainErr = drained, derr
	h.print(r)
	if !r.passed() {
		return errors.New("checks failed")
	}
	return nil
}

func (h *harness) event(format string, args ...any) {
	h.out.Lock()
	defer h.out.Unlock()
	d := time.Since(h.start).Round(time.Second)
	fmt.Printf("%02d:%02d:%02d  %s\n", int(d.Hours()), int(d.Minutes())%60, int(d.Seconds())%60, fmt.Sprintf(format, args...))
}

func (h *harness) status(ctx context.Context) {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		c, err := h.b.store.Counts(cctx)
		cancel()
		if err != nil {
			h.event("counts: %v", err)
			continue
		}
		h.event("%d jobs enqueued, %d batches failed; now enqueued %d, processing %d, scheduled %d, throttled %d, awaiting %d, failed %d",
			h.enq.jobs.Load(), h.enq.failed.Load(), c.Enqueued, c.Processing, c.Scheduled, c.Throttled, c.Awaiting, c.Failed)
	}
}

func (h *harness) drain() (time.Duration, error) {
	start := time.Now()
	last := start
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		c, err := h.b.store.Counts(ctx)
		cancel()
		live := c.Awaiting + c.Scheduled + c.Throttled + c.Enqueued + c.Processing
		if err == nil && live == 0 {
			return time.Since(start), nil
		}
		if time.Since(start) > drainLimit {
			return time.Since(start), fmt.Errorf("drain gave up after %s with %d live jobs", drainLimit, live)
		}
		if time.Since(last) >= 30*time.Second {
			last = time.Now()
			h.event("draining: %d live jobs (enqueued %d, processing %d, scheduled %d, throttled %d, awaiting %d)", live, c.Enqueued, c.Processing, c.Scheduled, c.Throttled, c.Awaiting)
		}
		time.Sleep(time.Second)
	}
}

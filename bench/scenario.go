package main

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

type metric int

const (
	rate metric = iota
	p50
	p99
)

type result struct {
	rate float64
	p50  time.Duration
	p99  time.Duration
}

type scenario struct {
	id       string
	title    string
	metrics  []metric
	variants []variant
	run      func(ctx context.Context, h *harness, s system) (result, error)
}

func (h *harness) scenarios() []scenario {
	kilnV := variant{id: "kiln", name: "kiln", new: func() system { return &kilnSystem{} }}
	riverV := variant{id: "river", name: "River", new: func() system { return &riverSystem{} }}
	workers := []variant{kilnV, riverV}
	if h.o.cooldown > 0 {
		cd := h.o.cooldown
		workers = append(workers, variant{
			id:   "river_tuned",
			name: fmt.Sprintf("River, FetchCooldown %s", cd),
			new:  func() system { return &riverSystem{cooldown: cd} },
		})
	}
	return []scenario{
		{
			id:      "1",
			title:   fmt.Sprintf("bulk insert, %d jobs in calls of %d", h.o.bulkJobs, h.o.chunk),
			metrics: []metric{rate},
			variants: []variant{
				{id: "kiln", name: "kiln EnqueueMany", new: func() system { return &kilnSystem{} }},
				{id: "river", name: "River InsertMany", new: func() system { return &riverSystem{} }},
				{id: "river_fast", name: "River InsertManyFast", new: func() system { return &riverSystem{fast: true} }},
			},
			run: bulkInsert,
		},
		{
			id:       "2",
			title:    fmt.Sprintf("concurrent single inserts, %d goroutines x %d", h.o.writers, h.o.perWriter),
			metrics:  []metric{rate, p50, p99},
			variants: []variant{kilnV, riverV},
			run:      concurrentInsert,
		},
		{
			id:       "3",
			title:    fmt.Sprintf("drain %d no-op jobs, %d workers", h.o.drainJobs, h.o.workers),
			metrics:  []metric{rate},
			variants: workers,
			run:      drain(h.o.drainJobs, func(context.Context, int) {}),
		},
		{
			id:       "4",
			title:    fmt.Sprintf("enqueue to handler start, %d enqueues %s apart", h.o.latJobs, h.o.spacing),
			metrics:  []metric{p50, p99},
			variants: workers,
			run:      latency,
		},
		{
			id:       "5",
			title:    fmt.Sprintf("drain %d jobs with a 1ms handler, %d workers", h.o.sleepJobs, h.o.workers),
			metrics:  []metric{rate},
			variants: workers,
			run:      drain(h.o.sleepJobs, func(context.Context, int) { time.Sleep(time.Millisecond) }),
		},
	}
}

func bulkInsert(ctx context.Context, h *harness, s system) (result, error) {
	total := h.o.bulkJobs
	start := time.Now()
	if err := fill(ctx, s, total, h.o.chunk); err != nil {
		return result{}, err
	}
	elapsed := time.Since(start)
	if err := h.expectRows(ctx, s, total); err != nil {
		return result{}, err
	}
	return result{rate: float64(total) / elapsed.Seconds()}, nil
}

func concurrentInsert(ctx context.Context, h *harness, s system) (result, error) {
	writers, each := h.o.writers, h.o.perWriter
	err := parallel(writers, func(int) error {
		for range 5 {
			if err := s.insert(ctx, 0); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return result{}, err
	}
	lat := make([]time.Duration, writers*each)
	start := time.Now()
	err = parallel(writers, func(w int) error {
		mine := lat[w*each : (w+1)*each]
		for i := range mine {
			t0 := time.Now()
			if err := s.insert(ctx, 0); err != nil {
				return err
			}
			mine[i] = time.Since(t0)
		}
		return nil
	})
	elapsed := time.Since(start)
	if err != nil {
		return result{}, err
	}
	if err := h.expectRows(ctx, s, writers*(each+5)); err != nil {
		return result{}, err
	}
	slices.Sort(lat)
	return result{rate: float64(len(lat)) / elapsed.Seconds(), p50: quantile(lat, 0.50), p99: quantile(lat, 0.99)}, nil
}

func drain(n int, work handler) func(context.Context, *harness, system) (result, error) {
	return func(ctx context.Context, h *harness, s system) (result, error) {
		if err := fill(ctx, s, n, h.o.chunk); err != nil {
			return result{}, err
		}
		if err := h.settle(ctx, s.table()); err != nil {
			return result{}, err
		}
		start := time.Now()
		if err := s.start(ctx, h.o.workers, work); err != nil {
			return result{}, err
		}
		elapsed, err := h.awaitCompleted(ctx, s, n, start)
		err = errors.Join(err, s.stop(ctx))
		if err != nil {
			return result{}, err
		}
		return result{rate: float64(n) / elapsed.Seconds()}, nil
	}
}

func latency(ctx context.Context, h *harness, s system) (result, error) {
	const warm = 20
	total := warm + h.o.latJobs
	started := make([]atomic.Int64, total)
	var seen atomic.Int64
	work := func(_ context.Context, seq int) {
		if seq >= 0 && seq < total && started[seq].CompareAndSwap(0, time.Now().UnixNano()) {
			seen.Add(1)
		}
	}
	if err := s.start(ctx, h.o.workers, work); err != nil {
		return result{}, err
	}
	if err := s.ready(ctx); err != nil {
		return result{}, err
	}
	if !pause(ctx, time.Second) {
		return result{}, ctx.Err()
	}

	sent := make([]int64, total)
	begin := time.Now()
	for i := range total {
		if !pause(ctx, time.Until(begin.Add(time.Duration(i)*h.o.spacing))) {
			return result{}, ctx.Err()
		}
		sent[i] = time.Now().UnixNano()
		if err := s.insert(ctx, i); err != nil {
			return result{}, err
		}
	}
	deadline := time.Now().Add(30 * time.Second)
	for seen.Load() < int64(total) {
		if time.Now().After(deadline) {
			return result{}, fmt.Errorf("only %d of %d jobs started within 30s", seen.Load(), total)
		}
		if !pause(ctx, 5*time.Millisecond) {
			return result{}, ctx.Err()
		}
	}
	if err := s.stop(ctx); err != nil {
		return result{}, err
	}

	lat := make([]time.Duration, 0, h.o.latJobs)
	for i := warm; i < total; i++ {
		lat = append(lat, time.Duration(started[i].Load()-sent[i]))
	}
	slices.Sort(lat)
	return result{p50: quantile(lat, 0.50), p99: quantile(lat, 0.99)}, nil
}

func fill(ctx context.Context, s system, n, chunk int) error {
	for left := n; left > 0; left -= chunk {
		if err := s.insertMany(ctx, min(chunk, left)); err != nil {
			return err
		}
	}
	return nil
}

func (h *harness) expectRows(ctx context.Context, s system, want int) error {
	got, err := h.count(ctx, "SELECT count(*) FROM "+s.table())
	if err != nil {
		return err
	}
	if got != want {
		return fmt.Errorf("%s has %d rows, want %d", s.table(), got, want)
	}
	return nil
}

func parallel(n int, fn func(i int) error) error {
	var (
		wg   sync.WaitGroup
		errs = make([]error, n)
	)
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = fn(i)
		}()
	}
	wg.Wait()
	return errors.Join(errs...)
}

func pause(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

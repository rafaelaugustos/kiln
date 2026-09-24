package pgstore

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/rafaelaugustos/kiln/driver"
)

func limited(key string, n int) func(*driver.InsertParams) {
	return func(p *driver.InsertParams) { p.LimitKey, p.LimitMax = key, n }
}

func TestLimitAdmission(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	res := insert(t, s,
		job("a", limited("k", 2)),
		job("a", limited("k", 2)),
		job("a", limited("k", 2), func(p *driver.InsertParams) { p.Priority = 5 }),
		job("a", limited("k", 2)),
	)
	states := map[driver.State]int{}
	for _, r := range res {
		states[r.State]++
	}
	if states[driver.Throttled] != 4 {
		t.Fatalf("insert states %+v", res)
	}
	jobs := claim(t, s, 10)
	if len(jobs) != 2 || jobs[0].ID != res[2].ID || jobs[1].ID != res[0].ID {
		t.Fatalf("first admission %+v", jobs)
	}
	finish(t, s, driver.Outcome{Ref: jobs[0].Ref, State: driver.Failed, Reason: "permanent"})
	next := claim(t, s, 10)
	if len(next) != 1 || next[0].ID != res[1].ID {
		t.Fatalf("fifo admission %+v", next)
	}
	if n, err := s.Delete(ctx, driver.Filter{IDs: []int64{res[3].ID}}); err != nil || n != 1 {
		t.Fatalf("delete throttled: %d %v", n, err)
	}
	finish(t, s, driver.Outcome{Ref: next[0].Ref, State: driver.Scheduled, Reason: "retry"})
	again := claim(t, s, 10)
	if len(again) != 1 || again[0].ID != res[1].ID {
		t.Fatalf("readmission %+v", again)
	}
	var active int
	if err := s.pool.QueryRow(ctx, "SELECT active FROM "+s.schema+".limits WHERE key = 'k'").Scan(&active); err != nil {
		t.Fatal(err)
	}
	if active != 2 {
		t.Fatalf("active %d, want 2", active)
	}
}

func TestLimitUnderConcurrency(t *testing.T) {
	t.Parallel()
	s := open(t, MaxConns(16))
	ctx := context.Background()
	const (
		max  = 3
		jobs = 300
	)
	ps := make([]driver.InsertParams, jobs)
	for i := range ps {
		ps[i] = job("a", limited("shared", max))
	}
	insert(t, s, ps...)
	var (
		running, peak, done atomic.Int64
		wg                  sync.WaitGroup
	)
	for range 8 {
		wg.Go(func() {
			for done.Load() < jobs {
				got, err := s.Claim(ctx, driver.ClaimQuery{Queues: []string{"default"}, Limit: 2, Server: "srv"})
				if err != nil {
					t.Error(err)
					return
				}
				n := running.Add(int64(len(got)))
				for {
					p := peak.Load()
					if n <= p || peak.CompareAndSwap(p, n) {
						break
					}
				}
				outs := make([]driver.Outcome, len(got))
				for i, j := range got {
					outs[i] = driver.Outcome{Ref: j.Ref, State: driver.Succeeded}
				}
				running.Add(-int64(len(got)))
				for len(outs) > 0 {
					res, err := s.Finish(ctx, "srv", outs)
					if err != nil {
						t.Error(err)
						return
					}
					var busy []driver.Outcome
					for i, r := range res {
						switch r {
						case driver.Applied:
							done.Add(1)
						case driver.Busy:
							busy = append(busy, outs[i])
						}
					}
					outs = busy
				}
			}
		})
	}
	wg.Wait()
	if p := peak.Load(); p > max {
		t.Fatalf("peak running %d exceeds %d", p, max)
	}
	c, err := s.Counts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if c.Succeeded != jobs || c.Throttled != 0 || c.Enqueued != 0 {
		t.Fatalf("counts %+v", c)
	}
}

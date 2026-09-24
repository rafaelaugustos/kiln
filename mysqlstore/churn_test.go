package mysqlstore

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

func metric(t *testing.T, s *Store, name string) int64 {
	t.Helper()
	var n int64
	if err := s.db.QueryRow(render("SELECT count FROM information_schema.INNODB_METRICS WHERE name = ?", name)).Scan(&n); err != nil {
		t.Logf("innodb metric %s: %v", name, err)
	}
	return n
}

type pool struct {
	mu  sync.Mutex
	ids []int64
}

func (p *pool) add(ids ...int64) {
	p.mu.Lock()
	p.ids = append(p.ids, ids...)
	if len(p.ids) > 256 {
		p.ids = p.ids[len(p.ids)-256:]
	}
	p.mu.Unlock()
}

func (p *pool) pick(rng *rand.Rand) int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.ids) == 0 {
		return 0
	}
	return p.ids[rng.IntN(len(p.ids))]
}

func TestChurn(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	deadlocks, timeouts := metric(t, s, "lock_deadlocks"), metric(t, s, "lock_timeouts")
	maxes := map[string]int{"a": 2, "b": 3, "r": 3}
	masks := []driver.Mask{driver.OnSucceeded, driver.OnFinished, driver.OnFailed | driver.OnSucceeded, driver.OnDeleted}
	var (
		recent             pool
		wg                 sync.WaitGroup
		inserted, finished atomic.Int64
	)
	stop := make(chan struct{})
	running := func() bool {
		select {
		case <-stop:
			return false
		default:
			return true
		}
	}
	for w := range 4 {
		wg.Go(func() {
			rng := rand.New(rand.NewPCG(uint64(w), 1))
			for running() {
				ps := make([]driver.InsertParams, 1+rng.IntN(6))
				for i := range ps {
					p := job("churn")
					switch rng.IntN(7) {
					case 0:
						unique(fmt.Sprint("key", rng.IntN(6)), 0)(&p)
					case 1:
						k := []string{"a", "b"}[rng.IntN(2)]
						limited(k, maxes[k])(&p)
					case 2:
						if id := recent.pick(rng); id != 0 {
							after(masks[rng.IntN(len(masks))], id)(&p)
						}
					case 3:
						if i > 0 {
							p.Parents = []driver.Parent{{Index: rng.IntN(i), On: masks[rng.IntN(len(masks))]}}
						}
					case 4:
						unique(fmt.Sprint("key", rng.IntN(6)), 0)(&p)
						limited("a", maxes["a"])(&p)
					case 5:
						limited("r", maxes["r"])(&p)
						rated("r", 2000, time.Second, 4)(&p)
					}
					ps[i] = p
				}
				res, err := s.Insert(ctx, ps)
				if err != nil {
					t.Errorf("insert: %v", err)
					return
				}
				for _, r := range res {
					if !r.Duplicate {
						recent.add(r.ID)
						inserted.Add(1)
					}
				}
			}
		})
	}
	for w := range 4 {
		wg.Go(func() {
			rng := rand.New(rand.NewPCG(uint64(w), 2))
			server := fmt.Sprint("w", w)
			for running() {
				js, err := s.Claim(ctx, driver.ClaimQuery{Queues: []string{"default"}, Limit: 8, Server: server})
				if err != nil {
					t.Errorf("claim: %v", err)
					return
				}
				outs := make([]driver.Outcome, len(js))
				for i, j := range js {
					out := driver.Outcome{Ref: j.Ref, State: driver.Succeeded}
					switch n := rng.IntN(20); {
					case n < 2:
						out.State, out.Reason = driver.Failed, "exhausted"
					case n < 4:
						out.State, out.Reason = driver.Deleted, "canceled"
					case n < 7:
						out.State, out.Reason = driver.Scheduled, "retry"
					case n < 8:
						out.State, out.Refund, out.Reason = driver.Enqueued, true, "shutdown"
					}
					outs[i] = out
				}
				for len(outs) > 0 {
					rs, err := s.Finish(ctx, server, outs)
					if err != nil {
						t.Errorf("finish: %v", err)
						return
					}
					busy := outs[:0]
					for i, r := range rs {
						switch r {
						case driver.Busy:
							busy = append(busy, outs[i])
						case driver.Applied:
							finished.Add(1)
						default:
							t.Errorf("finish job %d: result %d", outs[i].ID, r)
						}
					}
					outs = busy
				}
			}
		})
	}
	wg.Go(func() {
		rng := rand.New(rand.NewPCG(9, 9))
		for running() {
			if id := recent.pick(rng); id != 0 {
				if _, err := s.Delete(ctx, driver.Filter{IDs: []int64{id}}); err != nil {
					t.Errorf("delete: %v", err)
				}
			}
			if _, err := s.Requeue(ctx, driver.Filter{State: driver.Failed}); err != nil {
				t.Errorf("requeue: %v", err)
			}
			if _, err := s.Sweep(ctx, 100); err != nil {
				t.Errorf("sweep: %v", err)
			}
			if _, err := s.Promote(ctx, 100); err != nil {
				t.Errorf("promote: %v", err)
			}
			time.Sleep(10 * time.Millisecond)
		}
	})
	wg.Go(func() {
		q := "SELECT limit_key, COUNT(*) FROM kiln_jobs WHERE state IN ('enqueued', 'processing') AND limit_key IS NOT NULL GROUP BY limit_key"
		for running() {
			rows, err := s.db.QueryContext(ctx, q)
			if err != nil {
				t.Errorf("watch: %v", err)
				return
			}
			for rows.Next() {
				var (
					key string
					n   int
				)
				if err := rows.Scan(&key, &n); err != nil {
					t.Errorf("watch: %v", err)
				}
				if n > maxes[key] {
					t.Errorf("limit %s has %d jobs enqueued or processing, max %d", key, n, maxes[key])
				}
			}
			rows.Close()
			time.Sleep(2 * time.Millisecond)
		}
	})
	time.Sleep(3 * time.Second)
	close(stop)
	wg.Wait()
	if t.Failed() {
		return
	}
	drain(t, s)
	var active int
	if err := s.db.QueryRow("SELECT COALESCE(SUM(active), 0) FROM kiln_limits").Scan(&active); err != nil || active != 0 {
		t.Fatalf("limits still count %d active slots after draining, %v", active, err)
	}
	t.Logf("inserted %d, finished %d outcomes, innodb deadlocks %d, lock wait timeouts %d",
		inserted.Load(), finished.Load(), metric(t, s, "lock_deadlocks")-deadlocks, metric(t, s, "lock_timeouts")-timeouts)
}

func drain(t *testing.T, s *Store) {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(30 * time.Second)
	for {
		c, err := s.Counts(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if c.Awaiting+c.Scheduled+c.Throttled+c.Enqueued+c.Processing+c.Failed == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("store did not drain: %+v", c)
		}
		if _, err := s.Requeue(ctx, driver.Filter{State: driver.Failed}); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Promote(ctx, 1000); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Sweep(ctx, 1000); err != nil {
			t.Fatal(err)
		}
		for _, j := range claim(t, s, 500) {
			finish(t, s, driver.Outcome{Ref: j.Ref, State: driver.Succeeded})
		}
	}
}

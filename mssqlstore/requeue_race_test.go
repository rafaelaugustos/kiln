package mssqlstore

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

type recent struct {
	mu  sync.Mutex
	ids []int64
}

func (r *recent) add(id int64) {
	r.mu.Lock()
	r.ids = append(r.ids, id)
	if len(r.ids) > 400 {
		r.ids = append(r.ids[:0], r.ids[len(r.ids)-200:]...)
	}
	r.mu.Unlock()
}

func (r *recent) snapshot() []int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]int64(nil), r.ids[max(0, len(r.ids)-50):]...)
}

func TestTxInsertBesideRequeue(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	var ids recent
	var deadlocks, inserted, other atomic.Int64
	var mu sync.Mutex
	kinds := map[string]int{}
	var wg sync.WaitGroup
	for w := range 12 {
		wg.Go(func() {
			rng := rand.New(rand.NewPCG(uint64(w), 1))
			for ctx.Err() == nil {
				tx, err := s.db.BeginTx(ctx, readCommitted)
				if err != nil {
					continue
				}
				ps := make([]driver.InsertParams, 1+rng.IntN(4))
				known := ids.snapshot()
				parented := -1
				if len(known) > 0 && rng.IntN(2) == 0 {
					parented = rng.IntN(len(ps))
				}
				for i := range ps {
					p := job("k", func(p *driver.InsertParams) { p.MaxAttempts = 1 })
					if rng.IntN(2) == 0 {
						unique(fmt.Sprint("key", rng.IntN(6)))(&p)
					}
					if rng.IntN(2) == 0 {
						limited("L", 2)(&p)
					}
					if i == parented {
						p.Parents = []driver.Parent{{ID: known[rng.IntN(len(known))], On: driver.OnFinished}}
					}
					ps[i] = p
				}
				res, err := s.Tx(tx).Insert(ctx, ps)
				if err == nil {
					err = tx.Commit()
				} else {
					tx.Rollback()
				}
				switch e := sqlError(err); {
				case err == nil:
					inserted.Add(1)
					for _, r := range res {
						if !r.Duplicate {
							ids.add(r.ID)
						}
					}
				case e != nil && e.Number == errDeadlock:
					deadlocks.Add(1)
				case ctx.Err() == nil:
					other.Add(1)
					mu.Lock()
					kinds[err.Error()[:min(len(err.Error()), 80)]]++
					mu.Unlock()
				}
			}
		})
	}
	for w := range 3 {
		wg.Go(func() {
			server := fmt.Sprint("srv", w)
			for ctx.Err() == nil {
				js, err := s.Claim(ctx, driver.ClaimQuery{Queues: []string{"default"}, Limit: 8, Server: server})
				if err != nil || len(js) == 0 {
					time.Sleep(5 * time.Millisecond)
					continue
				}
				outs := make([]driver.Outcome, len(js))
				for i, j := range js {
					st := driver.Failed
					if j.ID%2 == 0 {
						st = driver.Succeeded
					}
					outs[i] = driver.Outcome{Ref: j.Ref, State: st, Reason: "boom"}
				}
				s.Finish(ctx, server, outs)
			}
		})
	}
	for range 3 {
		wg.Go(func() {
			for ctx.Err() == nil {
				s.Requeue(ctx, driver.Filter{State: driver.Failed})
				s.Requeue(ctx, driver.Filter{State: driver.Succeeded})
				time.Sleep(20 * time.Millisecond)
			}
		})
	}
	wg.Wait()
	if deadlocks.Load() > 0 || other.Load() > 0 || inserted.Load() == 0 {
		t.Errorf("%d transactions committed, %d deadlocked, %d failed otherwise: %v", inserted.Load(), deadlocks.Load(), other.Load(), kinds)
	}
}

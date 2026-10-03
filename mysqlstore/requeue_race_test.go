package mysqlstore

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

func TestTxInsertBesideRequeue(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	var ids pool
	var deadlocks, inserted, other atomic.Int64
	var mu sync.Mutex
	kinds := map[string]int{}
	var wg sync.WaitGroup
	for w := range 6 {
		wg.Go(func() {
			rng := rand.New(rand.NewPCG(uint64(w), 1))
			for ctx.Err() == nil {
				tx, err := s.db.BeginTx(ctx, readCommitted)
				if err != nil {
					continue
				}
				wr := s.Tx(tx)
				ps := make([]driver.InsertParams, 1+rng.IntN(4))
				known := ids.snapshot()
				for i := range ps {
					p := job("k", func(p *driver.InsertParams) { p.MaxAttempts = 1 })
					if rng.IntN(2) == 0 {
						unique(fmt.Sprint("key", rng.IntN(6)), 0)(&p)
					}
					if rng.IntN(2) == 0 {
						limited("L", 2)(&p)
					}
					if len(known) > 0 && rng.IntN(2) == 0 {
						p.Parents = []driver.Parent{{ID: known[rng.IntN(len(known))], On: driver.OnFinished}}
					}
					ps[i] = p
				}
				res, err := wr.Insert(ctx, ps)
				if err == nil {
					err = tx.Commit()
				} else {
					tx.Rollback()
				}
				switch me := mysqlError(err); {
				case err == nil:
					inserted.Add(1)
					for i, r := range res {
						if !r.Duplicate && len(ps[i].Parents) == 0 {
							ids.add(r.ID)
						}
					}
				case me != nil && me.Number == errDeadlock:
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
	for w := range 2 {
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
					outs[i] = driver.Outcome{Ref: j.Ref, State: driver.Failed, Reason: "boom"}
				}
				s.Finish(ctx, server, outs)
			}
		})
	}
	for range 2 {
		wg.Go(func() {
			for ctx.Err() == nil {
				s.Requeue(ctx, driver.Filter{State: driver.Failed})
				s.Requeue(ctx, driver.Filter{State: driver.Succeeded})
				time.Sleep(20 * time.Millisecond)
			}
		})
	}
	wg.Wait()
	if deadlocks.Load() == 0 && other.Load() == 0 && inserted.Load() > 0 {
		return
	}
	t.Errorf("%d transactions committed, %d deadlocked, %d failed otherwise: %v", inserted.Load(), deadlocks.Load(), other.Load(), kinds)
	var typ, name, status string
	if err := s.db.QueryRow("SHOW ENGINE INNODB STATUS").Scan(&typ, &name, &status); err == nil {
		if i := strings.Index(status, "LATEST DETECTED DEADLOCK"); i >= 0 {
			end := strings.Index(status[i:], "TRANSACTIONS\n------------")
			if end < 0 {
				end = 6000
			}
			t.Log(status[i : i+min(end, len(status)-i)])
		}
	}
}

func (p *pool) snapshot() []int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := len(p.ids)
	return append([]int64(nil), p.ids[max(0, n-50):]...)
}

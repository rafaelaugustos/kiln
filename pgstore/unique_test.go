package pgstore

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

func unique(key string, window time.Duration) func(*driver.InsertParams) {
	return func(p *driver.InsertParams) { p.UniqueKey, p.UniqueFor = []byte(key), window }
}

func TestUniqueLive(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	first := insert(t, s, job("a", unique("k1", 0)))[0]
	res := insert(t, s, job("a", unique("k1", 0)), job("b"), job("a", unique("k2", 0)), job("a", unique("k2", 0)))
	if !res[0].Duplicate || res[0].ID != first.ID || res[0].State != driver.Enqueued {
		t.Fatalf("live duplicate %+v", res[0])
	}
	if res[1].Duplicate || res[2].Duplicate || !res[3].Duplicate || res[3].ID != res[2].ID {
		t.Fatalf("in-call duplicates %+v", res)
	}
	j := claim(t, s, 1)[0]
	finish(t, s, driver.Outcome{Ref: j.Ref, State: driver.Failed, Reason: "permanent"})
	again := insert(t, s, job("a", unique("k1", 0)))[0]
	if again.Duplicate {
		t.Fatalf("key not released on failure: %+v", again)
	}
	if n, err := s.Requeue(ctx, driver.Filter{IDs: []int64{first.ID}}); err != nil || n != 0 {
		t.Fatalf("requeue with held key: %d %v", n, err)
	}
	if n, err := s.Delete(ctx, driver.Filter{IDs: []int64{again.ID}}); err != nil || n != 1 {
		t.Fatalf("delete: %d %v", n, err)
	}
	if n, err := s.Requeue(ctx, driver.Filter{IDs: []int64{first.ID}}); err != nil || n != 1 {
		t.Fatalf("requeue after release: %d %v", n, err)
	}
	if dup := insert(t, s, job("a", unique("k1", 0)))[0]; !dup.Duplicate || dup.ID != first.ID {
		t.Fatalf("requeued job does not hold key: %+v", dup)
	}
}

func TestUniqueWindow(t *testing.T) {
	t.Parallel()
	s := open(t)
	first := insert(t, s, job("a", unique("w", 300*time.Millisecond)))[0]
	j := claim(t, s, 1)[0]
	finish(t, s, driver.Outcome{Ref: j.Ref, State: driver.Succeeded})
	dup := insert(t, s, job("a", unique("w", 300*time.Millisecond)))[0]
	if !dup.Duplicate || dup.ID != first.ID || dup.State != driver.Succeeded {
		t.Fatalf("window duplicate %+v", dup)
	}
	time.Sleep(350 * time.Millisecond)
	if next := insert(t, s, job("a", unique("w", 300*time.Millisecond)))[0]; next.Duplicate {
		t.Fatalf("window not expired: %+v", next)
	}
}

func TestUniqueContention(t *testing.T) {
	t.Parallel()
	s := open(t, MaxConns(16))
	for round := range 5 {
		key := "c" + string(rune('a'+round))
		var (
			wg     sync.WaitGroup
			winner atomic.Int64
			ids    sync.Map
		)
		for range 16 {
			wg.Go(func() {
				res, err := s.Insert(context.Background(), []driver.InsertParams{job("a", unique(key, 0))})
				if err != nil {
					t.Error(err)
					return
				}
				if !res[0].Duplicate {
					winner.Add(1)
				}
				ids.Store(res[0].ID, true)
			})
		}
		wg.Wait()
		n := 0
		ids.Range(func(any, any) bool { n++; return true })
		if winner.Load() != 1 || n != 1 {
			t.Fatalf("round %d: %d winners, %d distinct ids", round, winner.Load(), n)
		}
	}
}

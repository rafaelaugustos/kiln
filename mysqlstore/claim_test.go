package mysqlstore

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

func TestClaimSkipsLockedRows(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	ids := make([]int64, 0, 3)
	for _, r := range insert(t, s, job("a"), job("a"), job("a")) {
		ids = append(ids, r.ID)
	}
	tx := begin(t, s)
	if _, err := tx.ExecContext(ctx, render("SELECT id FROM kiln_jobs WHERE id = ? FOR UPDATE", ids[0])); err != nil {
		t.Fatal(err)
	}
	soon, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	js, err := s.Claim(soon, driver.ClaimQuery{Queues: []string{"default"}, Limit: 5, Server: "srv"})
	if err != nil {
		t.Fatalf("claim behind a locked row: %v", err)
	}
	var got []int64
	for _, j := range js {
		got = append(got, j.ID)
	}
	if !slices.Equal(got, ids[1:]) {
		t.Fatalf("claimed %v, want %v", got, ids[1:])
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if js := claim(t, s, 5); len(js) != 1 || js[0].ID != ids[0] {
		t.Fatalf("claimed %+v after commit, want %d", js, ids[0])
	}
}

func TestClaimWhileInserting(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		inserted = make(map[int64]bool)
		claimed  = make(map[int64]int)
		done     atomic.Bool
	)
	for range 2 {
		wg.Go(func() {
			for range 60 {
				ps := make([]driver.InsertParams, 20)
				for i := range ps {
					ps[i] = job("a", func(p *driver.InsertParams) { p.Priority = int16(i % 3) })
				}
				res, err := s.Insert(ctx, ps)
				if err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				for _, r := range res {
					inserted[r.ID] = true
				}
				mu.Unlock()
			}
		})
	}
	var claimers sync.WaitGroup
	for range 8 {
		claimers.Go(func() {
			for {
				js, err := s.Claim(ctx, driver.ClaimQuery{Queues: []string{"default"}, Limit: 7, Server: "srv"})
				if err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				for _, j := range js {
					claimed[j.ID]++
				}
				mu.Unlock()
				if len(js) == 0 && done.Load() {
					return
				}
			}
		})
	}
	wg.Wait()
	done.Store(true)
	claimers.Wait()
	if t.Failed() {
		return
	}
	for id, n := range claimed {
		if n != 1 || !inserted[id] {
			t.Fatalf("job %d claimed %d times (inserted %v)", id, n, inserted[id])
		}
	}
	if len(claimed) != len(inserted) {
		t.Fatalf("claimed %d of %d jobs", len(claimed), len(inserted))
	}
}

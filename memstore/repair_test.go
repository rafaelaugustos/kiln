package memstore

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

func TestSweepRepairsLostWork(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := New()
	b, _ := s.OpenBatch(ctx, driver.NewBatch{})
	job := func(kind string) driver.InsertParams {
		return driver.InsertParams{Kind: kind, Queue: "q", Args: []byte(`{}`), MaxAttempts: 1}
	}
	parent, member, limited := job("p"), job("m"), job("l")
	member.BatchID = b
	limited.LimitKey, limited.LimitMax = "k", 1
	child := job("c")
	child.Parents = []driver.Parent{{Index: 0, On: driver.OnSucceeded}}
	then := job("t")
	then.AfterBatch = b
	res, err := s.Insert(ctx, []driver.InsertParams{parent, child, member, then, limited})
	if err != nil {
		t.Fatal(err)
	}

	s.begin()
	s.move(s.jobs[res[0].ID], driver.Succeeded)
	s.batches[b].sealed = true
	s.move(s.jobs[res[2].ID], driver.Deleted)
	s.batches[b].finished = time.Time{}
	s.resolved, s.done = nil, nil
	s.limits["k"].active = 0
	s.end()

	for _, i := range []int{1, 3} {
		if st := s.jobs[res[i].ID].state; st != driver.Awaiting {
			t.Fatalf("job %d is %s before sweep", i, st)
		}
	}
	n, err := s.Sweep(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	if n < 3 {
		t.Fatalf("sweep changed %d rows", n)
	}
	for _, i := range []int{1, 3} {
		if st := s.jobs[res[i].ID].state; st != driver.Enqueued {
			t.Fatalf("job %d is %s after sweep", i, st)
		}
	}
	if s.batches[b].finished.IsZero() || s.limits["k"].active != 1 {
		t.Fatalf("batch finished %v, active %d", s.batches[b].finished, s.limits["k"].active)
	}
	if n, _ := s.Sweep(ctx, 100); n != 0 {
		t.Fatalf("second sweep changed %d rows", n)
	}
}

func TestSweepRepairsMissingParents(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := New()
	b, _ := s.OpenBatch(ctx, driver.NewBatch{})
	job := func(kind string) driver.InsertParams {
		return driver.InsertParams{Kind: kind, Queue: "q", Args: []byte(`{}`), MaxAttempts: 1}
	}
	doomed, resumed, then := job("d"), job("r"), job("t")
	doomed.Parents = []driver.Parent{{Index: 0, On: driver.OnSucceeded}}
	resumed.Parents = []driver.Parent{{Index: 0, On: driver.OnDeleted}}
	then.AfterBatch = b
	res, err := s.Insert(ctx, []driver.InsertParams{job("p"), doomed, resumed, then})
	if err != nil {
		t.Fatal(err)
	}

	s.begin()
	s.unindex(s.jobs[res[0].ID])
	delete(s.jobs, res[0].ID)
	delete(s.batches, b)
	s.end()

	if n, err := s.Sweep(ctx, 100); err != nil || n != 3 {
		t.Fatalf("sweep changed %d rows, %v, want 3", n, err)
	}
	want := map[int64]driver.State{res[1].ID: driver.Deleted, res[2].ID: driver.Enqueued, res[3].ID: driver.Enqueued}
	for id, st := range want {
		if got := s.jobs[id].state; got != st {
			t.Fatalf("job %d is %s, want %s", id, got, st)
		}
	}
	h := s.jobs[res[1].ID].history
	if reason := fmt.Sprintf("parent %d pruned", res[0].ID); len(h) != 1 || h[0].Reason != reason {
		t.Fatalf("doomed history %+v, want %q", h, reason)
	}
	if n, _ := s.Sweep(ctx, 100); n != 0 {
		t.Fatalf("second sweep changed %d rows", n)
	}
}

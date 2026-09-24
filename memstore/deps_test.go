package memstore_test

import (
	"slices"
	"sync"
	"testing"

	"github.com/rafaelaugustos/kiln/driver"
)

func TestDoomCascade(t *testing.T) {
	t.Parallel()
	s, _ := open(t)
	got := ids(t, s,
		params("a"),
		params("b", needs(0, driver.OnSucceeded)),
		params("c", needs(1, driver.OnSucceeded)),
		params("d", needs(1, driver.OnDeleted)),
	)
	if n, err := s.Delete(ctx, driver.Filter{IDs: got[:1]}); err != nil || n != 1 {
		t.Fatalf("delete = %d, %v", n, err)
	}
	expectState(t, s, got[1], driver.Deleted)
	expectState(t, s, got[2], driver.Deleted)
	expectState(t, s, got[3], driver.Enqueued)
	if h := record(t, s, got[2]).History; len(h) != 1 || h[0].Reason != "parent 2 deleted" {
		t.Fatalf("grandchild history = %+v", h)
	}
}

func TestFailedParent(t *testing.T) {
	t.Parallel()
	s, _ := open(t)
	got := ids(t, s,
		params("a"),
		params("b", needs(0, driver.OnSucceeded)),
		params("c", needs(0, driver.OnFinished)),
		params("d", needs(0, driver.OnFailed)),
	)
	finish(t, s, done(claimOne(t, s), driver.Failed))
	expectState(t, s, got[1], driver.Awaiting)
	expectState(t, s, got[2], driver.Enqueued)
	expectState(t, s, got[3], driver.Enqueued)

	if n, err := s.Requeue(ctx, driver.Filter{IDs: got[:1]}); err != nil || n != 1 {
		t.Fatalf("requeue = %d, %v", n, err)
	}
	js := claim(t, s, 10)
	for _, j := range js {
		if j.ID == got[0] {
			finish(t, s, done(j, driver.Succeeded))
		}
	}
	expectState(t, s, got[1], driver.Enqueued)
}

func TestIndexRefs(t *testing.T) {
	t.Parallel()
	s, _ := open(t)
	res := insert(t, s,
		params("child", needs(1, driver.OnSucceeded), needs(2, driver.OnSucceeded)),
		params("left"),
		params("right", priority(-1)),
	)
	child := res[0].ID
	if res[0].State != driver.Awaiting || res[1].State != driver.Enqueued {
		t.Fatalf("states = %+v", res)
	}
	r := record(t, s, child)
	if !slices.Equal(r.Parents, []int64{res[1].ID, res[2].ID}) || r.PendingDeps != 2 {
		t.Fatalf("parents %v pending %d", r.Parents, r.PendingDeps)
	}
	if c := record(t, s, res[1].ID).Children; !slices.Equal(c, []int64{child}) {
		t.Fatalf("children = %v", c)
	}
	finish(t, s, done(claimOne(t, s), driver.Succeeded))
	if r := record(t, s, child); r.State != driver.Awaiting || r.PendingDeps != 1 {
		t.Fatalf("after one parent: %s pending %d", r.State, r.PendingDeps)
	}
	finish(t, s, done(claimOne(t, s), driver.Succeeded))
	expectState(t, s, child, driver.Enqueued)
}

func TestFanIn(t *testing.T) {
	t.Parallel()
	s, _ := open(t)
	const n = 64
	ps := make([]driver.InsertParams, n+1)
	for i := range n {
		ps[i] = params("leaf")
		ps[n].Parents = append(ps[n].Parents, driver.Parent{Index: i, On: driver.OnSucceeded})
	}
	ps[n].Kind, ps[n].Queue, ps[n].Args, ps[n].MaxAttempts = "join", "join", []byte(`{}`), 1
	child := insert(t, s, ps...)[n].ID

	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for {
				js, err := s.Claim(ctx, driver.ClaimQuery{Queues: []string{"default"}, Limit: 1})
				if err != nil || len(js) == 0 {
					return
				}
				if _, err := s.Finish(ctx, "s", []driver.Outcome{done(js[0], driver.Succeeded)}); err != nil {
					t.Error(err)
				}
			}
		})
	}
	wg.Wait()
	js := claim(t, s, 10, "join")
	if len(js) != 1 || js[0].ID != child {
		t.Fatalf("join claimed %+v", js)
	}
	if more := claim(t, s, 10, "join"); len(more) != 0 {
		t.Fatalf("join enqueued twice")
	}
}

func TestDuplicateParent(t *testing.T) {
	t.Parallel()
	s, _ := open(t)
	res := insert(t, s,
		params("a", unique("k", 0)),
		params("a", unique("k", 0)),
		params("b", needs(1, driver.OnSucceeded)),
	)
	if !res[1].Duplicate || res[1].ID != res[0].ID {
		t.Fatalf("in-call duplicate = %+v", res[1])
	}
	if p := record(t, s, res[2].ID).Parents; !slices.Equal(p, []int64{res[0].ID}) {
		t.Fatalf("parent through duplicate = %v", p)
	}
	finish(t, s, done(claimOne(t, s), driver.Succeeded))
	expectState(t, s, res[2].ID, driver.Enqueued)
}

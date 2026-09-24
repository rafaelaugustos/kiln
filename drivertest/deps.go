package drivertest

import (
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

var depsTests = []test{
	{"Continuation", testDepsContinuation},
	{"Masks", testDepsMasks},
	{"FailedParent", testDepsFailedParent},
	{"FinalParent", testDepsFinalParent},
	{"Chain", testDepsChain},
	{"Cascade", testDepsCascade},
	{"FanIn", testDepsFanIn},
	{"Index", testDepsIndex},
	{"Invalid", testDepsInvalid},
	{"MissingParent", testDepsMissingParent},
	{"Resume", testDepsResume},
	{"DeleteParent", testDepsDeleteParent},
	{"PrunedParent", testDepsPrunedParent},
}

func testDepsContinuation(t *testing.T, s driver.Store) {
	parent := add(t, s, task("p"))
	ins := insert(t, s, after("c", driver.OnSucceeded, parent), after("c", driver.OnSucceeded, parent))
	children := insertedIDs(ins)
	for _, in := range ins {
		if in.State != driver.Awaiting {
			t.Fatalf("child inserted as %s, want awaiting", in.State)
		}
		r := record(t, s, in.ID)
		if r.State != driver.Awaiting || r.PendingDeps != 1 || !slices.Equal(r.Parents, []int64{parent}) {
			t.Fatalf("child: state %s pending %d parents %v", r.State, r.PendingDeps, r.Parents)
		}
	}
	if got := sorted(record(t, s, parent).Children); !slices.Equal(got, children) {
		t.Fatalf("children %v, want %v", got, children)
	}
	if js := claim(t, s, 10, "c"); len(js) != 0 {
		t.Fatalf("claimed awaiting children %v", jobIDs(js))
	}
	apply(t, s, outcome(claimOne(t, s, "p", parent), driver.Succeeded))
	for _, id := range children {
		if r := record(t, s, id); r.State != driver.Enqueued || r.PendingDeps != 0 {
			t.Fatalf("child %d: state %s pending %d, want enqueued with none pending", id, r.State, r.PendingDeps)
		}
	}
	if got := sorted(jobIDs(claim(t, s, 10, "c"))); !slices.Equal(got, children) {
		t.Fatalf("claimed %v, want %v", got, children)
	}
}

func testDepsMasks(t *testing.T, s driver.Store) {
	cases := []struct {
		on     driver.Mask
		parent driver.State
		want   driver.State
	}{
		{driver.OnSucceeded, driver.Succeeded, driver.Enqueued},
		{driver.OnSucceeded, driver.Failed, driver.Awaiting},
		{driver.OnSucceeded, driver.Deleted, driver.Deleted},
		{driver.OnFailed, driver.Succeeded, driver.Deleted},
		{driver.OnFailed, driver.Failed, driver.Enqueued},
		{driver.OnFailed, driver.Deleted, driver.Deleted},
		{driver.OnDeleted, driver.Succeeded, driver.Deleted},
		{driver.OnDeleted, driver.Failed, driver.Awaiting},
		{driver.OnDeleted, driver.Deleted, driver.Enqueued},
		{driver.OnFinished, driver.Succeeded, driver.Enqueued},
		{driver.OnFinished, driver.Failed, driver.Enqueued},
		{driver.OnFinished, driver.Deleted, driver.Enqueued},
		{driver.OnSucceeded | driver.OnFailed, driver.Failed, driver.Enqueued},
	}
	for i, c := range cases {
		q := fmt.Sprintf("p%d", i)
		parent := add(t, s, task(q))
		child := add(t, s, after("c", c.on, parent))
		apply(t, s, outcome(claimOne(t, s, q, parent), c.parent))
		if got := stateOf(t, s, child); got != c.want {
			t.Fatalf("mask %03b, parent %s: child %s, want %s", c.on, c.parent, got, c.want)
		}
		if c.want == driver.Deleted {
			wantReason(t, s, child, fmt.Sprintf("parent %d %s", parent, c.parent))
		}
	}
}

func testDepsFailedParent(t *testing.T, s driver.Store) {
	parent := add(t, s, task("p"))
	child := add(t, s, after("c", driver.OnSucceeded, parent))
	apply(t, s, outcome(claimOne(t, s, "p", parent), driver.Failed))
	wantState(t, s, driver.Awaiting, child)
	sweep(t, s)
	wantState(t, s, driver.Awaiting, child)
	if n := requeueIDs(t, s, parent); n != 1 {
		t.Fatalf("requeued %d, want 1", n)
	}
	apply(t, s, outcome(claimOne(t, s, "p", parent), driver.Succeeded))
	wantState(t, s, driver.Enqueued, child)
}

func testDepsFinalParent(t *testing.T, s driver.Store) {
	finished := func(q string, st driver.State) int64 {
		j := start(t, s, task(q))
		apply(t, s, outcome(j, st))
		return j.ID
	}
	succeeded := finished("a", driver.Succeeded)
	deleted := finished("b", driver.Deleted)
	failed := finished("c", driver.Failed)
	pending := add(t, s, task("d"))

	cases := []struct {
		p    driver.InsertParams
		want driver.State
		deps int
	}{
		{after("q", driver.OnSucceeded, succeeded), driver.Enqueued, 0},
		{after("q", driver.OnSucceeded, deleted), driver.Deleted, 0},
		{after("q", driver.OnSucceeded, failed), driver.Awaiting, 1},
		{after("q", driver.OnFailed, failed), driver.Enqueued, 0},
		{after("q", driver.OnFailed, succeeded), driver.Deleted, 0},
		{after("q", driver.OnFinished, succeeded, deleted, failed), driver.Enqueued, 0},
		{after("q", driver.OnSucceeded, succeeded, pending), driver.Awaiting, 1},
		{after("q", driver.OnFinished, pending, failed), driver.Awaiting, 1},
		{after("q", driver.OnSucceeded, failed, pending), driver.Awaiting, 2},
	}
	for i, c := range cases {
		in := insert(t, s, c.p)[0]
		if in.State != c.want {
			t.Fatalf("case %d: inserted as %s, want %s", i, in.State, c.want)
		}
		r := record(t, s, in.ID)
		if r.State != c.want || (c.want != driver.Deleted && r.PendingDeps != c.deps) {
			t.Fatalf("case %d: state %s pending %d, want %s with %d pending", i, r.State, r.PendingDeps, c.want, c.deps)
		}
	}
}

func testDepsChain(t *testing.T, s driver.Store) {
	a, b, c := task("a"), task("b"), task("c")
	b.Parents = []driver.Parent{{Index: 0, On: driver.OnSucceeded}}
	c.Parents = []driver.Parent{{Index: 1, On: driver.OnSucceeded}}
	ids := insertedIDs(insert(t, s, a, b, c))
	wantState(t, s, driver.Awaiting, ids[1], ids[2])
	apply(t, s, outcome(claimOne(t, s, "a", ids[0]), driver.Succeeded))
	wantState(t, s, driver.Enqueued, ids[1])
	wantState(t, s, driver.Awaiting, ids[2])
	apply(t, s, outcome(claimOne(t, s, "b", ids[1]), driver.Succeeded))
	wantState(t, s, driver.Enqueued, ids[2])
}

func testDepsCascade(t *testing.T, s driver.Store) {
	ps := tasks(6, "chain")
	for i := 1; i < len(ps); i++ {
		ps[i].Queue = "later"
		ps[i].Parents = []driver.Parent{{Index: i - 1, On: driver.OnSucceeded}}
	}
	ps = append(ps, task("side"))
	ps[len(ps)-1].Parents = []driver.Parent{{Index: 2, On: driver.OnDeleted}}
	ids := insertedIDs(insert(t, s, ps...))
	apply(t, s, outcome(claimOne(t, s, "chain", ids[0]), driver.Deleted))
	chain := ids[1:6]
	for range 10 {
		if !slices.ContainsFunc(chain, func(id int64) bool { return stateOf(t, s, id) != driver.Deleted }) {
			break
		}
		sweep(t, s)
	}
	wantState(t, s, driver.Deleted, chain...)
	wantState(t, s, driver.Enqueued, ids[6])
	for i, id := range chain {
		wantReason(t, s, id, fmt.Sprintf("parent %d deleted", ids[i]))
	}
}

func testDepsFanIn(t *testing.T, s driver.Store) {
	const fans, width = 4, 16
	var children []int64
	for range fans {
		ps := tasks(width+1, "parents")
		child := &ps[width]
		child.Queue = "child"
		for i := range width {
			child.Parents = append(child.Parents, driver.Parent{Index: i, On: driver.OnSucceeded})
		}
		ins := insert(t, s, ps...)
		children = append(children, ins[width].ID)
	}
	js := claim(t, s, fans*width, "parents")
	if len(js) != fans*width {
		t.Fatalf("claimed %d parents, want %d", len(js), fans*width)
	}
	ctx := t.Context()
	deadline := time.Now().Add(3 * time.Second)
	var wg sync.WaitGroup
	for _, j := range js {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				rs, err := s.Finish(ctx, server, []driver.Outcome{outcome(j, driver.Succeeded)})
				if err != nil || len(rs) != 1 {
					t.Errorf("finish parent %d: %v %v", j.ID, rs, err)
					return
				}
				if rs[0] != driver.Busy {
					if rs[0] != driver.Applied {
						t.Errorf("finish parent %d: %d, want applied", j.ID, rs[0])
					}
					return
				}
				time.Sleep(time.Millisecond)
			}
			t.Errorf("parent %d still busy after 3s", j.ID)
		}()
	}
	wg.Wait()
	if t.Failed() {
		return
	}
	for _, id := range children {
		if r := record(t, s, id); r.State != driver.Enqueued || r.PendingDeps != 0 {
			t.Fatalf("child %d: state %s pending %d, want enqueued with none pending", id, r.State, r.PendingDeps)
		}
	}
	sweep(t, s)
	if got := sorted(jobIDs(claim(t, s, 100, "child"))); !slices.Equal(got, children) {
		t.Fatalf("claimed children %v, want %v", got, children)
	}
	sweep(t, s)
	if js := claim(t, s, 100, "child"); len(js) != 0 {
		t.Fatalf("children enqueued again: %v", jobIDs(js))
	}
	if c := counts(t, s); c.Enqueued != 0 || c.Awaiting != 0 {
		t.Fatalf("enqueued %d awaiting %d after fan-in, want 0", c.Enqueued, c.Awaiting)
	}
}

func testDepsIndex(t *testing.T, s driver.Store) {
	existing := add(t, s, task("p"))
	child, parent, grandchild := task("c"), task("p"), task("c")
	child.Parents = []driver.Parent{{Index: 1, On: driver.OnSucceeded}}
	grandchild.Parents = []driver.Parent{{Index: 0, On: driver.OnSucceeded}, {ID: existing, On: driver.OnSucceeded}}
	ins := insert(t, s, child, parent, grandchild)
	if ins[0].State != driver.Awaiting || ins[1].State != driver.Enqueued || ins[2].State != driver.Awaiting {
		t.Fatalf("inserted %+v", ins)
	}
	if ins[0].ID <= existing || ins[1].ID <= ins[0].ID || ins[2].ID <= ins[1].ID {
		t.Fatalf("ids %v not increasing in slice order", insertedIDs(ins))
	}
	if got := record(t, s, ins[0].ID).Parents; !slices.Equal(got, []int64{ins[1].ID}) {
		t.Fatalf("child parents %v, want [%d]", got, ins[1].ID)
	}
	r := record(t, s, ins[2].ID)
	if got, want := sorted(r.Parents), sorted([]int64{ins[0].ID, existing}); !slices.Equal(got, want) || r.PendingDeps != 2 {
		t.Fatalf("grandchild parents %v pending %d, want %v and 2", got, r.PendingDeps, want)
	}
	apply(t, s, outcome(claimOne(t, s, "p", existing), driver.Succeeded))
	if r := record(t, s, ins[2].ID); r.PendingDeps != 1 || r.State != driver.Awaiting {
		t.Fatalf("grandchild: state %s pending %d, want awaiting with 1", r.State, r.PendingDeps)
	}
	apply(t, s, outcome(claimOne(t, s, "p", ins[1].ID), driver.Succeeded))
	apply(t, s, outcome(claimOne(t, s, "c", ins[0].ID), driver.Succeeded))
	wantState(t, s, driver.Enqueued, ins[2].ID)
}

func testDepsInvalid(t *testing.T, s driver.Store) {
	ref := func(p driver.InsertParams, idx ...int) driver.InsertParams {
		for _, i := range idx {
			p.Parents = append(p.Parents, driver.Parent{Index: i, On: driver.OnSucceeded})
		}
		return p
	}
	cases := map[string][]driver.InsertParams{
		"self":         {task("q"), ref(task("q"), 1)},
		"two cycle":    {ref(task("q"), 1), ref(task("q"), 0)},
		"three cycle":  {ref(task("q"), 2), ref(task("q"), 0), ref(task("q"), 1)},
		"out of range": {task("q"), ref(task("q"), 2)},
		"negative":     {task("q"), ref(task("q"), -1)},
	}
	for name, ps := range cases {
		_, err := s.Insert(t.Context(), ps)
		if err == nil {
			t.Fatalf("%s: insert succeeded", name)
		}
		wantErr(t, err, driver.ErrInvalid)
		wantEmpty(t, s)
	}
}

func testDepsMissingParent(t *testing.T, s driver.Store) {
	parent := add(t, s, task("p"))
	_, err := s.Insert(t.Context(), []driver.InsertParams{task("q"), after("q", driver.OnSucceeded, parent, parent+1<<40)})
	wantErr(t, err, driver.ErrNotFound)
	if c := counts(t, s); c.Enqueued != 1 || c.Awaiting != 0 {
		t.Fatalf("enqueued %d awaiting %d, want only the parent", c.Enqueued, c.Awaiting)
	}
}

func testDepsResume(t *testing.T, s driver.Store) {
	parent := add(t, s, task("p"))
	delayed := after("q", driver.OnSucceeded, parent)
	delayed.Delay = time.Hour
	past := after("q", driver.OnSucceeded, parent)
	past.RunAt = now(t, s).Add(-time.Hour)
	blocker := add(t, s, limited("other", "k", 1))
	throttled := after("q", driver.OnSucceeded, parent)
	throttled.LimitKey, throttled.LimitMax = "k", 1
	admitted := after("q", driver.OnSucceeded, parent)
	admitted.LimitKey, admitted.LimitMax = "free", 1
	ids := insertedIDs(insert(t, s, delayed, past, throttled, admitted))
	wantState(t, s, driver.Awaiting, ids...)
	wantState(t, s, driver.Enqueued, blocker)

	apply(t, s, outcome(claimOne(t, s, "p", parent), driver.Succeeded))
	wantState(t, s, driver.Scheduled, ids[0])
	wantState(t, s, driver.Enqueued, ids[1])
	wantState(t, s, driver.Throttled, ids[2])
	wantState(t, s, driver.Enqueued, ids[3])
}

func testDepsPrunedParent(t *testing.T, s driver.Store) {
	bid := openBatch(t, s)
	parent := start(t, s, members("p", bid, 1)[0])
	next := add(t, s, afterBatch("next", bid))
	seal(t, s, bid)
	doomed := add(t, s, after("c", driver.OnSucceeded, parent.ID))
	resumed := add(t, s, after("c", driver.OnDeleted, parent.ID))
	lp := after("c", driver.OnDeleted, parent.ID)
	lp.LimitKey, lp.LimitMax = "k", 1
	throttled := add(t, s, lp)
	failed := add(t, s, after("c", driver.OnFailed, parent.ID))
	apply(t, s, outcome(parent, driver.Failed))
	wantState(t, s, driver.Enqueued, failed)
	wantState(t, s, driver.Awaiting, doomed, resumed, throttled, next)
	wantFinished(t, s, bid, false)
	deleted := counts(t, s).Deleted

	time.Sleep(20 * time.Millisecond)
	pp := keep()
	pp.Failed = 5 * time.Millisecond
	prune(t, s, pp)
	if !gone(t, s, parent.ID) {
		t.Fatalf("failed parent %d not pruned", parent.ID)
	}
	wantState(t, s, driver.Deleted, doomed)
	wantReason(t, s, doomed, fmt.Sprintf("parent %d pruned", parent.ID))
	wantState(t, s, driver.Enqueued, resumed, throttled, failed, next)
	if b, err := s.Batch(t.Context(), bid); err != nil {
		wantErr(t, err, driver.ErrNotFound)
	} else if b.FinishedAt.IsZero() {
		t.Fatalf("batch %d open after its only member was pruned", bid)
	}
	if c := counts(t, s); c.Deleted != deleted+1 || c.Awaiting != 0 {
		t.Fatalf("counts %+v, want one more deleted and nothing awaiting", c)
	}
}

func testDepsDeleteParent(t *testing.T, s driver.Store) {
	parent := add(t, s, task("p"))
	doomed := add(t, s, after("c", driver.OnSucceeded, parent))
	resumed := add(t, s, after("c", driver.OnDeleted, parent))
	if n := deleteIDs(t, s, parent); n != 1 {
		t.Fatalf("deleted %d, want 1", n)
	}
	wantState(t, s, driver.Deleted, parent, doomed)
	wantState(t, s, driver.Enqueued, resumed)
	wantReason(t, s, doomed, fmt.Sprintf("parent %d deleted", parent))

	other := add(t, s, task("p"))
	child := add(t, s, after("c", driver.OnSucceeded, other))
	if n := deleteIDs(t, s, child); n != 1 {
		t.Fatalf("deleted %d, want 1", n)
	}
	apply(t, s, outcome(claimOne(t, s, "p", other), driver.Succeeded))
	wantState(t, s, driver.Deleted, child)
}

package drivertest

import (
	"fmt"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

var uniqueTests = []test{
	{"Held", testUniqueHeld},
	{"Released", testUniqueReleased},
	{"Window", testUniqueWindow},
	{"InCall", testUniqueInCall},
	{"Requeue", testUniqueRequeue},
}

func uniq(queue, k string) driver.InsertParams {
	p := task(queue)
	p.UniqueKey = key(k)
	return p
}

func wantDuplicate(t *testing.T, s driver.Store, p driver.InsertParams, holder int64) {
	t.Helper()
	in := insert(t, s, p)[0]
	if !in.Duplicate || in.ID != holder {
		t.Fatalf("insert %x: %+v, want duplicate of %d", p.UniqueKey, in, holder)
	}
	if in.State != "" {
		if st := stateOf(t, s, holder); in.State != st {
			t.Fatalf("duplicate reports holder state %s, holder is %s", in.State, st)
		}
	}
}

func wantFresh(t *testing.T, s driver.Store, p driver.InsertParams) int64 {
	t.Helper()
	in := insert(t, s, p)[0]
	if in.Duplicate {
		t.Fatalf("insert %x: duplicate of %d, want a new job", p.UniqueKey, in.ID)
	}
	return in.ID
}

func testUniqueHeld(t *testing.T, s driver.Store) {
	p := uniq("q", "a")
	holder := wantFresh(t, s, p)
	wantDuplicate(t, s, p, holder)
	wantFresh(t, s, uniq("q", "b"))
	if c := counts(t, s); c.Enqueued != 2 {
		t.Fatalf("enqueued %d, want 2", c.Enqueued)
	}

	j := claimOne(t, s, "q", holder)
	wantDuplicate(t, s, p, holder)
	retry := outcome(j, driver.Scheduled)
	retry.Delay, retry.Reason = time.Hour, "retry"
	apply(t, s, retry)
	wantDuplicate(t, s, p, holder)

	parent := add(t, s, task("parents"))
	child := after("q", driver.OnSucceeded, parent)
	child.UniqueKey = key("c")
	awaiting := wantFresh(t, s, child)
	wantDuplicate(t, s, uniq("q", "c"), awaiting)

	add(t, s, limited("other", "k", 1))
	throttled := uniq("q", "d")
	throttled.LimitKey, throttled.LimitMax = "k", 1
	id := wantFresh(t, s, throttled)
	wantState(t, s, driver.Throttled, id)
	wantDuplicate(t, s, uniq("q", "d"), id)
}

func testUniqueReleased(t *testing.T, s driver.Store) {
	exits := []struct {
		name string
		exit func(q string, id int64)
	}{
		{"succeeded", func(q string, id int64) { apply(t, s, outcome(claimOne(t, s, q, id), driver.Succeeded)) }},
		{"failed", func(q string, id int64) { apply(t, s, outcome(claimOne(t, s, q, id), driver.Failed)) }},
		{"deleted", func(q string, id int64) { apply(t, s, outcome(claimOne(t, s, q, id), driver.Deleted)) }},
		{"admin delete", func(q string, id int64) { deleteIDs(t, s, id) }},
		{"canceled", func(q string, id int64) {
			j := claimOne(t, s, q, id)
			deleteIDs(t, s, id)
			apply(t, s, outcome(j, driver.Failed))
		}},
	}
	for i, e := range exits {
		q := fmt.Sprintf("q%d", i)
		p := uniq(q, e.name)
		first := wantFresh(t, s, p)
		e.exit(q, first)
		second := wantFresh(t, s, p)
		if second == first {
			t.Fatalf("%s: reinsert returned the same id", e.name)
		}
		wantDuplicate(t, s, p, second)
	}

	parent := add(t, s, task("parents"))
	child := after("c", driver.OnSucceeded, parent)
	child.UniqueKey = key("doomed")
	doomed := wantFresh(t, s, child)
	deleteIDs(t, s, parent)
	wantState(t, s, driver.Deleted, doomed)
	wantFresh(t, s, uniq("c", "doomed"))

	child.UniqueKey = key("born doomed")
	if in := insert(t, s, child)[0]; in.Duplicate || in.State != driver.Deleted {
		t.Fatalf("child of a deleted parent inserted as %+v, want deleted", in)
	}
	wantFresh(t, s, uniq("c", "born doomed"))
}

func testUniqueWindow(t *testing.T, s driver.Store) {
	exits := []struct {
		name string
		exit func(q string, id int64)
	}{
		{"succeeded", func(q string, id int64) { apply(t, s, outcome(claimOne(t, s, q, id), driver.Succeeded)) }},
		{"failed", func(q string, id int64) { apply(t, s, outcome(claimOne(t, s, q, id), driver.Failed)) }},
		{"deleted", func(q string, id int64) { apply(t, s, outcome(claimOne(t, s, q, id), driver.Deleted)) }},
		{"admin delete", func(q string, id int64) { deleteIDs(t, s, id) }},
	}
	for i, e := range exits {
		q := fmt.Sprintf("q%d", i)
		p := uniq(q, e.name)
		p.UniqueFor = time.Hour
		id := wantFresh(t, s, p)
		e.exit(q, id)
		wantDuplicate(t, s, p, id)
	}

	const window = 100 * time.Millisecond
	p := uniq("w", "window")
	p.UniqueFor = window
	began := time.Now()
	first := wantFresh(t, s, p)
	if in := insert(t, s, p)[0]; time.Since(began) < window && (!in.Duplicate || in.ID != first) {
		t.Fatalf("insert inside the window: %+v, want duplicate of %d", in, first)
	}
	eventually(t, time.Second, func() bool { return !insert(t, s, p)[0].Duplicate })
	if el := time.Since(began); el < window-5*time.Millisecond {
		t.Fatalf("key released after %v, want at least %v", el, window)
	}
}

func testUniqueInCall(t *testing.T, s driver.Store) {
	ins := insert(t, s, uniq("q", "a"), uniq("q", "a"), uniq("q", "b"), uniq("q", "a"), task("q"))
	switch {
	case ins[0].Duplicate || ins[2].Duplicate || ins[4].Duplicate:
		t.Fatalf("first occurrences marked duplicate: %+v", ins)
	case !ins[1].Duplicate || ins[1].ID != ins[0].ID || !ins[3].Duplicate || ins[3].ID != ins[0].ID:
		t.Fatalf("repeated key not duplicate of first: %+v", ins)
	case ins[2].ID <= ins[0].ID || ins[4].ID <= ins[2].ID:
		t.Fatalf("ids not increasing: %+v", ins)
	}
	if c := counts(t, s); c.Enqueued != 3 {
		t.Fatalf("enqueued %d, want 3", c.Enqueued)
	}

	b := ins[2].ID
	ins = insert(t, s, uniq("q", "b"), uniq("q", "c"))
	if !ins[0].Duplicate || ins[0].ID != b || ins[1].Duplicate {
		t.Fatalf("mixed insert %+v", ins)
	}
	if c := counts(t, s); c.Enqueued != 4 {
		t.Fatalf("enqueued %d, want 4", c.Enqueued)
	}
}

func testUniqueRequeue(t *testing.T, s driver.Store) {
	p := uniq("q", "r")
	first := wantFresh(t, s, p)
	apply(t, s, outcome(claimOne(t, s, "q", first), driver.Failed))
	second := wantFresh(t, s, p)
	if n := requeueIDs(t, s, first); n != 0 {
		t.Fatalf("requeued %d while key is held by %d, want 0", n, second)
	}
	wantState(t, s, driver.Failed, first)

	apply(t, s, outcome(claimOne(t, s, "q", second), driver.Succeeded))
	if n := requeueIDs(t, s, first); n != 1 {
		t.Fatalf("requeued %d, want 1", n)
	}
	wantState(t, s, driver.Enqueued, first)
	wantDuplicate(t, s, p, first)
}

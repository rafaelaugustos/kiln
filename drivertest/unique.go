package drivertest

import (
	"fmt"
	"maps"
	"slices"
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
	{"Replace", testUniqueReplace},
	{"ReplaceStates", testUniqueReplaceStates},
	{"ReplaceLimited", testUniqueReplaceLimited},
	{"Debounce", testUniqueDebounce},
	{"DebounceStates", testUniqueDebounceStates},
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

func replacing(queue, k, args string) driver.InsertParams {
	p := uniq(queue, k)
	p.UniqueReplace, p.Args = true, []byte(args)
	return p
}

func debounced(queue, k, args string, d time.Duration) driver.InsertParams {
	p := uniq(queue, k)
	p.UniqueDebounce, p.Args = d, []byte(args)
	return p
}

func wantReplaced(t *testing.T, s driver.Store, p driver.InsertParams, holder int64, st driver.State) driver.Record {
	t.Helper()
	in := insert(t, s, p)[0]
	if !in.Duplicate || !in.Replaced || in.ID != holder || in.State != st {
		t.Fatalf("insert %x: %+v, want holder %d replaced and %s", p.UniqueKey, in, holder, st)
	}
	r := record(t, s, holder)
	switch {
	case r.State != st:
		t.Fatalf("replaced holder %d is %s, want %s", holder, r.State, st)
	case !sameJSON(r.Args, p.Args) || !maps.Equal(r.Meta, p.Meta) || !slices.Equal(r.Tags, p.Tags):
		t.Fatalf("replaced holder %d has args %s meta %v tags %v, want %s %v %v", holder, r.Args, r.Meta, r.Tags, p.Args, p.Meta, p.Tags)
	case r.Title != p.Title || r.Priority != p.Priority:
		t.Fatalf("replaced holder %d has title %q priority %d, want %q %d", holder, r.Title, r.Priority, p.Title, p.Priority)
	}
	return r
}

func wantKept(t *testing.T, s driver.Store, p driver.InsertParams, holder int64) {
	t.Helper()
	before := record(t, s, holder)
	in := insert(t, s, p)[0]
	if !in.Duplicate || in.Replaced || in.ID != holder {
		t.Fatalf("insert %x: %+v, want a plain duplicate of %d", p.UniqueKey, in, holder)
	}
	after := record(t, s, holder)
	if after.State != before.State || !sameJSON(after.Args, before.Args) || !after.RunAt.Equal(before.RunAt) {
		t.Fatalf("plain duplicate changed holder %d from %s %s %v to %s %s %v", holder,
			before.State, before.Args, before.RunAt, after.State, after.Args, after.RunAt)
	}
}

func testUniqueReplace(t *testing.T, s driver.Store) {
	first := replacing("q", "r", `{"v":1}`)
	first.Delay = time.Hour
	holder := wantFresh(t, s, first)
	wantState(t, s, driver.Scheduled, holder)

	next := replacing("q", "r", `{"v":2}`)
	next.Meta, next.Tags, next.Title, next.Priority = map[string]string{"m": "2"}, []string{"b", "a"}, "second", 4
	next.RunAt = now(t, s).Add(2 * time.Hour).Truncate(time.Second)
	if r := wantReplaced(t, s, next, holder, driver.Scheduled); !r.RunAt.Equal(next.RunAt) {
		t.Fatalf("replaced run at %v, want %v", r.RunAt, next.RunAt)
	}

	n0 := now(t, s)
	r := wantReplaced(t, s, replacing("q", "r", `{"v":3}`), holder, driver.Scheduled)
	within(t, "run at replaced by now", r.RunAt, n0, now(t, s))
	promote(t, s, 100)
	wantState(t, s, driver.Enqueued, holder)

	enqueued := record(t, s, holder).RunAt
	if r := wantReplaced(t, s, replacing("q", "r", `{"v":4}`), holder, driver.Enqueued); !r.RunAt.Equal(enqueued) {
		t.Fatalf("enqueued holder run at %v, want it kept at %v", r.RunAt, enqueued)
	}
	later := replacing("q", "r", `{"v":5}`)
	later.Delay = time.Hour
	n0 = now(t, s)
	r = wantReplaced(t, s, later, holder, driver.Scheduled)
	within(t, "run at of a holder sent back to scheduled", r.RunAt, n0.Add(time.Hour), now(t, s).Add(time.Hour))

	ins := insert(t, s, replacing("q", "c", `{"v":"a"}`), replacing("q", "c", `{"v":"b"}`))
	if ins[0].Duplicate || !ins[1].Duplicate || ins[1].Replaced || ins[1].ID != ins[0].ID {
		t.Fatalf("repeated key in one call: %+v, want a plain duplicate of the first job", ins)
	}
	if r := record(t, s, ins[0].ID); !sameJSON(r.Args, []byte(`{"v":"a"}`)) {
		t.Fatalf("repeated key in one call replaced args with %s", r.Args)
	}

	w := replacing("w", "window", `{"v":1}`)
	w.UniqueFor = time.Hour
	done := wantFresh(t, s, w)
	apply(t, s, outcome(claimOne(t, s, "w", done), driver.Succeeded))
	w.Args = []byte(`{"v":2}`)
	wantKept(t, s, w, done)
}

func testUniqueReplaceStates(t *testing.T, s driver.Store) {
	parent := add(t, s, task("parents"))
	child := replacing("q", "a", `{"v":1}`)
	child.Parents = []driver.Parent{{ID: parent, On: driver.OnSucceeded}}
	awaiting := wantFresh(t, s, child)
	p := replacing("q", "a", `{"v":2}`)
	p.Delay, p.Parents = time.Hour, child.Parents
	n0 := now(t, s)
	r := wantReplaced(t, s, p, awaiting, driver.Awaiting)
	within(t, "awaiting holder run at", r.RunAt, n0.Add(time.Hour), now(t, s).Add(time.Hour))

	add(t, s, limited("blocker", "k", 1))
	throttle := func(args string) driver.InsertParams {
		p := replacing("q", "t", args)
		p.LimitKey, p.LimitMax = "k", 1
		return p
	}
	throttled := wantFresh(t, s, throttle(`{"v":1}`))
	wantState(t, s, driver.Throttled, throttled)
	wantReplaced(t, s, throttle(`{"v":2}`), throttled, driver.Throttled)
	p = throttle(`{"v":3}`)
	p.Delay = time.Hour
	wantReplaced(t, s, p, throttled, driver.Scheduled)

	busy := wantFresh(t, s, replacing("busy", "p", `{"v":1}`))
	claimOne(t, s, "busy", busy)
	wantKept(t, s, replacing("busy", "p", `{"v":2}`), busy)
}

func testUniqueReplaceLimited(t *testing.T, s driver.Store) {
	rate := func(args string) driver.InsertParams {
		p := rated("r", "k", 1, time.Hour, 1)
		p.UniqueKey, p.UniqueReplace, p.Args = key("granted"), true, []byte(args)
		return p
	}
	first := rated("r", "k", 1, time.Hour, 1)
	first.UniqueKey, first.UniqueReplace = key("admitted"), true
	ins := insert(t, s, first, rate(`{"v":1}`))
	wantState(t, s, driver.Enqueued, ins[0].ID)
	wantState(t, s, driver.Scheduled, ins[1].ID)
	reserved := record(t, s, ins[1].ID).RunAt

	if r := wantReplaced(t, s, rate(`{"v":2}`), ins[1].ID, driver.Scheduled); !r.RunAt.Equal(reserved) {
		t.Fatalf("granted holder run at %v, want its reserved start %v", r.RunAt, reserved)
	}
	admitted := record(t, s, ins[0].ID).RunAt
	p := rated("r", "k", 1, time.Hour, 1)
	p.UniqueKey, p.UniqueReplace, p.Args, p.Delay = key("admitted"), true, []byte(`{"v":2}`), time.Hour
	if r := wantReplaced(t, s, p, ins[0].ID, driver.Enqueued); !r.RunAt.Equal(admitted) {
		t.Fatalf("admitted holder run at %v, want it kept at %v", r.RunAt, admitted)
	}
}

func testUniqueDebounce(t *testing.T, s driver.Store) {
	p := debounced("q", "d", `{"v":1}`, time.Hour)
	p.RunAt = now(t, s).Add(-time.Hour)
	n0 := now(t, s)
	in := insert(t, s, p)[0]
	if in.Duplicate || in.State != driver.Scheduled {
		t.Fatalf("first debounced insert: %+v, want a new scheduled job", in)
	}
	first := record(t, s, in.ID).RunAt
	within(t, "debounced run at", first, n0.Add(time.Hour), now(t, s).Add(time.Hour))

	time.Sleep(5 * time.Millisecond)
	next := debounced("q", "d", `{"v":2}`, time.Hour)
	next.Tags, next.Title = []string{"x"}, "pushed"
	n0 = now(t, s)
	r := wantReplaced(t, s, next, in.ID, driver.Scheduled)
	within(t, "pushed run at", r.RunAt, n0.Add(time.Hour), now(t, s).Add(time.Hour))
	if !r.RunAt.After(first) {
		t.Fatalf("pushed run at %v, want after %v", r.RunAt, first)
	}

	requeueIDs(t, s, in.ID)
	wantState(t, s, driver.Enqueued, in.ID)
	wantKept(t, s, debounced("q", "d", `{"v":3}`, time.Hour), in.ID)
	claimOne(t, s, "q", in.ID)
	wantKept(t, s, debounced("q", "d", `{"v":4}`, time.Hour), in.ID)
}

func testUniqueDebounceStates(t *testing.T, s driver.Store) {
	parent := add(t, s, task("parents"))
	child := uniq("q", "a")
	child.Parents = []driver.Parent{{ID: parent, On: driver.OnSucceeded}}
	awaiting := wantFresh(t, s, child)
	wantKept(t, s, debounced("q", "a", `{"v":2}`, time.Hour), awaiting)

	add(t, s, limited("blocker", "k", 1))
	th := uniq("q", "t")
	th.LimitKey, th.LimitMax = "k", 1
	throttled := wantFresh(t, s, th)
	wantState(t, s, driver.Throttled, throttled)
	wantKept(t, s, debounced("q", "t", `{"v":2}`, time.Hour), throttled)

	g := rated("r", "g", 1, time.Hour, 1)
	g.UniqueKey = key("granted")
	ins := insert(t, s, rated("r", "g", 1, time.Hour, 1), g)
	wantState(t, s, driver.Scheduled, ins[1].ID)
	wantKept(t, s, debounced("r", "granted", `{"v":2}`, time.Hour), ins[1].ID)
}

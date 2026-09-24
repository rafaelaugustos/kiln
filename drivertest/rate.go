package drivertest

import (
	"cmp"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

var rateTests = []test{
	{"Spacing", testRateSpacing},
	{"Burst", testRateBurst},
	{"Mutex", testRateMutex},
	{"Unlimited", testRateUnlimited},
	{"Stall", testRateStall},
	{"Retry", testRateRetry},
	{"Requeue", testRateRequeue},
	{"Update", testRateUpdate},
	{"Sweep", testRateSweep},
	{"Prune", testRatePrune},
	{"Invalid", testRateInvalid},
}

func rated(queue, key string, rate int, per time.Duration, burst int) driver.InsertParams {
	p := task(queue)
	p.LimitKey, p.LimitRate, p.LimitPer, p.LimitBurst = key, rate, per, burst
	return p
}

func repeat(n int, p driver.InsertParams) []driver.InsertParams {
	ps := make([]driver.InsertParams, n)
	for i := range ps {
		ps[i] = p
	}
	return ps
}

func reach(t *testing.T, s driver.Store, at time.Time) {
	t.Helper()
	eventually(t, time.Second, func() bool { return !now(t, s).Before(at) })
}

func wantSlot(t *testing.T, s driver.Store, id int64, lo, hi time.Time, gap time.Duration) time.Time {
	t.Helper()
	r := record(t, s, id)
	if r.State != driver.Scheduled {
		t.Fatalf("job %d state = %s, want scheduled in a reserved slot", id, r.State)
	}
	within(t, fmt.Sprintf("job %d run at", id), r.RunAt, lo.Add(gap-5*time.Millisecond), hi.Add(gap+10*time.Millisecond))
	return r.RunAt
}

func wantConsistent(t *testing.T, s driver.Store) {
	t.Helper()
	if n, err := s.Sweep(t.Context(), 1000); err != nil || n != 0 {
		t.Fatalf("sweep changed %d rows, %v, want nothing to repair", n, err)
	}
}

func testRateSpacing(t *testing.T, s driver.Store) {
	const gap = 50 * time.Millisecond
	p := rated("s", "k", 20, time.Second, 1)
	n0 := now(t, s)
	ids := insertedIDs(insert(t, s, repeat(5, p)...))
	n1 := now(t, s)
	wantState(t, s, driver.Enqueued, ids[0])
	at := wantSlot(t, s, ids[1], n0, n1, gap)
	for _, id := range ids[2:] {
		at = wantSlot(t, s, id, at, at, gap)
	}
	wantConsistent(t, s)

	reach(t, s, at)
	promote(t, s, 100)
	wantState(t, s, driver.Enqueued, ids...)
	next := add(t, s, p)
	if stateOf(t, s, next) == driver.Enqueued {
		if n := now(t, s); n.Before(at.Add(gap - 5*time.Millisecond)) {
			t.Fatalf("job %d enqueued at %v, before the slot after %v", next, n, at)
		}
		return
	}
	wantSlot(t, s, next, at, at, gap)
}

func testRateBurst(t *testing.T, s driver.Store) {
	const gap = 100 * time.Millisecond
	n0 := now(t, s)
	ids := insertedIDs(insert(t, s, repeat(5, rated("b", "k", 10, time.Second, 3))...))
	n1 := now(t, s)
	wantState(t, s, driver.Enqueued, ids[:3]...)
	at := wantSlot(t, s, ids[3], n0, n1, gap)
	wantSlot(t, s, ids[4], at, at, gap)
}

func testRateMutex(t *testing.T, s driver.Store) {
	job := func(prio int16) driver.InsertParams {
		p := rated("m", "k", 1000, time.Second, 1000)
		p.LimitMax, p.Priority = 1, prio
		return p
	}
	cur := start(t, s, job(0))
	prios := []int16{0, 0, 5, 0, 5}
	ids := make([]int64, len(prios))
	for i, p := range prios {
		ids[i] = add(t, s, job(p))
	}
	wantState(t, s, driver.Throttled, ids...)
	for _, i := range []int{2, 4, 0, 1, 3} {
		apply(t, s, outcome(cur, driver.Succeeded))
		cur = claimOne(t, s, "m", ids[i])
		if c := counts(t, s); c.Enqueued != 0 || c.Scheduled != 0 {
			t.Fatalf("counts %+v while job %d runs under a limit of 1, want nothing else enqueued or scheduled", c, cur.ID)
		}
	}
	apply(t, s, outcome(cur, driver.Succeeded))
}

func testRateUnlimited(t *testing.T, s driver.Store) {
	p := rated("u", "k", 1000, time.Second, 1000)
	insert(t, s, repeat(100, p)...)
	if c := counts(t, s); c.Enqueued != 100 || c.Throttled != 0 || c.Scheduled != 0 {
		t.Fatalf("counts %+v, want all 100 jobs enqueued", c)
	}
	claimN(t, s, 100, "u")
	insert(t, s, repeat(50, p)...)
	if c := counts(t, s); c.Enqueued != 50 || c.Processing != 100 || c.Throttled != 0 || c.Scheduled != 0 {
		t.Fatalf("counts %+v, want 50 more enqueued while 100 run", c)
	}
}

func testRateStall(t *testing.T, s driver.Store) {
	const gap = 50 * time.Millisecond
	p := rated("st", "k", 20, time.Second, 1)
	p.LimitMax = 1
	n0 := now(t, s)
	ids := insertedIDs(insert(t, s, repeat(3, p)...))
	n1 := now(t, s)
	wantState(t, s, driver.Enqueued, ids[0])
	at := wantSlot(t, s, ids[1], n0, n1, gap)
	at = wantSlot(t, s, ids[2], at, at, gap)
	running := claimOne(t, s, "st", ids[0])

	reach(t, s, at)
	promote(t, s, 100)
	wantState(t, s, driver.Throttled, ids[1:]...)
	wantConsistent(t, s)
	wantState(t, s, driver.Throttled, ids[1:]...)

	apply(t, s, outcome(running, driver.Succeeded))
	wantState(t, s, driver.Enqueued, ids[1])
	wantState(t, s, driver.Throttled, ids[2])
	apply(t, s, outcome(claimOne(t, s, "st", ids[1]), driver.Succeeded))
	wantState(t, s, driver.Enqueued, ids[2])
}

func testRateRetry(t *testing.T, s driver.Store) {
	const gap = 50 * time.Millisecond
	ids := insertedIDs(insert(t, s, repeat(65, rated("r", "k", 20, time.Second, 1))...))
	granted := ids[1:5]
	wantState(t, s, driver.Scheduled, ids[1:]...)
	reach(t, s, record(t, s, granted[3]).RunAt)
	promote(t, s, 100)
	js := claimN(t, s, 5, "r")
	slices.SortFunc(js, func(a, b driver.Job) int { return cmp.Compare(a.ID, b.ID) })
	if got := jobIDs(js); !slices.Equal(got, ids[:5]) {
		t.Fatalf("claimed %v, want the first job and the granted ones %v", got, ids[:5])
	}

	apply(t, s,
		outcome(js[0], driver.Succeeded),
		driver.Outcome{Ref: js[1].Ref, State: driver.Scheduled, Reason: "retry"},
		driver.Outcome{Ref: js[2].Ref, State: driver.Enqueued, Refund: true, Reason: "shutdown"},
		driver.Outcome{Ref: js[3].Ref, State: driver.Enqueued, Reason: "lost"},
		driver.Outcome{Ref: js[4].Ref, State: driver.Scheduled, Delay: 20 * time.Millisecond, Refund: true, Reason: "snoozed"},
	)
	at := record(t, s, ids[len(ids)-1]).RunAt
	for _, id := range granted[:3] {
		at = wantSlot(t, s, id, at, at, gap)
	}
	reach(t, s, record(t, s, granted[3]).RunAt)
	promote(t, s, 100)
	wantSlot(t, s, granted[3], at, at, gap)
}

func testRateRequeue(t *testing.T, s driver.Store) {
	p := rated("rq", "k", 1, time.Minute, 1)
	n0 := now(t, s)
	ids := insertedIDs(insert(t, s, p, p))
	n1 := now(t, s)
	wantState(t, s, driver.Enqueued, ids[0])
	at := wantSlot(t, s, ids[1], n0, n1, time.Minute)
	if n := requeueIDs(t, s, ids[1]); n != 1 {
		t.Fatalf("requeued %d, want 1", n)
	}
	at = wantSlot(t, s, ids[1], at, at, time.Minute)

	apply(t, s, outcome(claimOne(t, s, "rq", ids[0]), driver.Failed))
	if n := requeueIDs(t, s, ids[0]); n != 1 {
		t.Fatalf("requeued %d, want 1", n)
	}
	wantSlot(t, s, ids[0], at, at, time.Minute)
}

func testRateUpdate(t *testing.T, s driver.Store) {
	const slow, slower = 6 * time.Second, 12 * time.Second
	n0 := now(t, s)
	ids := insertedIDs(insert(t, s, repeat(3, rated("up", "k", 10, time.Minute, 1))...))
	n1 := now(t, s)
	wantState(t, s, driver.Enqueued, ids[0])
	kept := []time.Time{wantSlot(t, s, ids[1], n0, n1, slow)}
	kept = append(kept, wantSlot(t, s, ids[2], kept[0], kept[0], slow))

	more := insertedIDs(insert(t, s, repeat(2, rated("up", "k", 5, time.Minute, 1))...))
	for i, want := range kept {
		if r := record(t, s, ids[i+1]); r.State != driver.Scheduled || !r.RunAt.Equal(want) {
			t.Fatalf("job %d %s at %v after the rate changed, want its slot at %v kept", r.ID, r.State, r.RunAt, want)
		}
	}
	at := wantSlot(t, s, more[0], kept[1], kept[1], slow)
	wantSlot(t, s, more[1], at, at, slower)
}

func testRateSweep(t *testing.T, s driver.Store) {
	const gap = 50 * time.Millisecond
	var ids []int64
	n0 := now(t, s)
	inTx(t, s, func(w driver.Writer) error {
		ids = insertedIDs(insert(t, w, repeat(3, rated("sw", "k", 20, time.Second, 1))...))
		return nil
	})
	for range 3 {
		sweep(t, s)
	}
	n1 := now(t, s)
	wantState(t, s, driver.Enqueued, ids[0])
	at := wantSlot(t, s, ids[1], n0, n1, gap)
	wantSlot(t, s, ids[2], at, at, gap)
	wantConsistent(t, s)
}

func testRatePrune(t *testing.T, s driver.Store) {
	slow := rated("pr", "slow", 1, time.Hour, 1)
	n0 := now(t, s)
	j := start(t, s, slow)
	n1 := now(t, s)
	apply(t, s, outcome(j, driver.Succeeded))
	apply(t, s, outcome(start(t, s, rated("pr", "fast", 100, time.Second, 1)), driver.Succeeded))
	time.Sleep(30 * time.Millisecond)
	pp := keep()
	pp.Succeeded = 5 * time.Millisecond
	n := 0
	for range 3 {
		n += prune(t, s, pp)
	}
	if n != 3 {
		t.Fatalf("pruned %d rows, want both jobs and only the limit whose slots are all past", n)
	}
	wantSlot(t, s, add(t, s, slow), n0, n1, time.Hour)
}

func testRateInvalid(t *testing.T, s driver.Store) {
	cases := []struct {
		name string
		edit func(p *driver.InsertParams)
	}{
		{"neither max nor rate", func(p *driver.InsertParams) { p.LimitRate = 0 }},
		{"rate without period", func(p *driver.InsertParams) { p.LimitPer = 0 }},
		{"rate without burst", func(p *driver.InsertParams) { p.LimitBurst = 0 }},
		{"negative max", func(p *driver.InsertParams) { p.LimitMax = -1 }},
		{"negative rate", func(p *driver.InsertParams) { p.LimitMax, p.LimitRate = 1, -1 }},
		{"negative period", func(p *driver.InsertParams) { p.LimitPer = -time.Second }},
		{"negative burst", func(p *driver.InsertParams) { p.LimitBurst = -1 }},
		{"negative period without rate", func(p *driver.InsertParams) { p.LimitMax, p.LimitRate, p.LimitPer = 1, 0, -time.Second }},
		{"rate without key or period", func(p *driver.InsertParams) { p.LimitKey, p.LimitPer = "", 0 }},
	}
	for _, c := range cases {
		bad := rated("q", "k", 5, time.Second, 5)
		c.edit(&bad)
		_, err := s.Insert(t.Context(), []driver.InsertParams{task("q"), bad})
		if err == nil {
			t.Fatalf("%s: insert succeeded", c.name)
		}
		wantErr(t, err, driver.ErrInvalid)
		wantEmpty(t, s)
	}
	both := rated("q", "k", 5, time.Second, 1)
	both.LimitMax = 2
	insert(t, s, rated("q", "k", 5, time.Second, 5), both)
}

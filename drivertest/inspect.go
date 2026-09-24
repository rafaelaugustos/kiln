package drivertest

import (
	"slices"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

var inspectTests = []test{
	{"JobNotFound", testJobNotFound},
	{"JobsOrder", testJobsOrder},
	{"JobsFinalOrder", testJobsFinalOrder},
	{"JobsFilter", testJobsFilter},
	{"JobsPages", testJobsPages},
	{"JobsLimit", testJobsLimit},
	{"Counts", testCounts},
	{"Series", testSeries},
	{"DeletedStats", testDeletedStats},
	{"Queues", testQueues},
}

func list(t *testing.T, s driver.Store, q driver.JobQuery) []int64 {
	t.Helper()
	limit := q.Limit
	if limit <= 0 {
		limit = 20
	}
	limit = min(limit, 500)
	var ids []int64
	seen := make(map[int64]bool)
	for range 1000 {
		p, err := s.Jobs(t.Context(), q)
		if err != nil {
			t.Fatalf("jobs %+v: %v", q, err)
		}
		if len(p.Records) > limit {
			t.Fatalf("jobs %+v: page of %d records", q, len(p.Records))
		}
		for _, r := range p.Records {
			if seen[r.ID] {
				t.Fatalf("jobs %+v: job %d listed twice", q, r.ID)
			}
			if r.State != q.State {
				t.Fatalf("jobs %+v: job %d is %s", q, r.ID, r.State)
			}
			seen[r.ID] = true
			ids = append(ids, r.ID)
		}
		if p.Next == "" {
			return ids
		}
		q.Cursor = p.Next
	}
	t.Fatalf("jobs %+v: pagination did not end", q)
	return nil
}

func wantOrder(t *testing.T, s driver.Store, q driver.JobQuery, want ...int64) {
	t.Helper()
	if got := list(t, s, q); !slices.Equal(got, want) {
		t.Fatalf("jobs %s in %q: %v, want %v", q.State, q.Queue, got, want)
	}
}

func desc(ids []int64) []int64 {
	ids = sorted(ids)
	slices.Reverse(ids)
	return ids
}

func testJobNotFound(t *testing.T, s driver.Store) {
	_, err := s.Job(t.Context(), 1<<40)
	wantErr(t, err, driver.ErrNotFound)
}

func testJobsOrder(t *testing.T, s driver.Store) {
	n := now(t, s).Truncate(time.Second)
	ps := tasks(4, "scheduled")
	for i, d := range []time.Duration{3 * time.Hour, time.Hour, 2 * time.Hour, time.Hour} {
		ps[i].RunAt = n.Add(d)
	}
	ids := insertedIDs(insert(t, s, ps...))
	wantOrder(t, s, driver.JobQuery{State: driver.Scheduled, Limit: 3}, ids[1], ids[3], ids[2], ids[0])

	ps = tasks(4, "enqueued")
	for i, p := range []int16{0, 5, 0, 5} {
		ps[i].Priority = p
	}
	enqueued := insertedIDs(insert(t, s, ps...))
	wantOrder(t, s, driver.JobQuery{State: driver.Enqueued, Queue: "enqueued", Limit: 3}, enqueued[1], enqueued[3], enqueued[0], enqueued[2])

	add(t, s, limited("blocker", "k", 1))
	ps = make([]driver.InsertParams, 4)
	for i, p := range []int16{1, 3, 1, 3} {
		ps[i] = limited("throttled", "k", 1)
		ps[i].Priority = p
	}
	ids = insertedIDs(insert(t, s, ps...))
	wantOrder(t, s, driver.JobQuery{State: driver.Throttled, Limit: 3}, ids[1], ids[3], ids[0], ids[2])

	parent := add(t, s, task("parent"))
	ids = insertedIDs(insert(t, s, after("c", driver.OnSucceeded, parent), after("c", driver.OnSucceeded, parent), after("c", driver.OnSucceeded, parent)))
	wantOrder(t, s, driver.JobQuery{State: driver.Awaiting, Limit: 2}, desc(ids)...)

	claim(t, s, 4, "enqueued")
	wantOrder(t, s, driver.JobQuery{State: driver.Processing, Limit: 3}, desc(enqueued)...)
}

func testJobsFinalOrder(t *testing.T, s driver.Store) {
	for _, st := range []driver.State{driver.Succeeded, driver.Failed, driver.Deleted} {
		q := string(st)
		ids := insertedIDs(insert(t, s, tasks(5, q)...))
		js := claim(t, s, 5, q)
		byID := make(map[int64]driver.Job, len(js))
		for _, j := range js {
			byID[j.ID] = j
		}
		for _, group := range [][]int{{2}, {0}, {1, 4}, {3}} {
			outs := make([]driver.Outcome, len(group))
			for i, k := range group {
				outs[i] = outcome(byID[ids[k]], st)
			}
			apply(t, s, outs...)
			time.Sleep(5 * time.Millisecond)
		}
		wantOrder(t, s, driver.JobQuery{State: st, Queue: q, Limit: 2}, ids[3], ids[4], ids[1], ids[0], ids[2])
	}
}

func testJobsFilter(t *testing.T, s driver.Store) {
	_, err := s.Jobs(t.Context(), driver.JobQuery{Limit: 10})
	wantErr(t, err, driver.ErrInvalid)

	bid := openBatch(t, s)
	job := func(q, kind string, batch int64) driver.InsertParams {
		p := task(q)
		p.Kind, p.BatchID = kind, batch
		return p
	}
	ids := insertedIDs(insert(t, s, job("a", "x", 0), job("a", "x", 0), job("a", "y", 0), job("b", "x", 0), job("b", "y", bid)))
	cases := []struct {
		q    driver.JobQuery
		want []int64
	}{
		{driver.JobQuery{}, ids},
		{driver.JobQuery{Queue: "a"}, ids[:3]},
		{driver.JobQuery{Kind: "x"}, []int64{ids[0], ids[1], ids[3]}},
		{driver.JobQuery{Queue: "a", Kind: "x"}, ids[:2]},
		{driver.JobQuery{BatchID: bid}, ids[4:]},
		{driver.JobQuery{Queue: "c"}, nil},
	}
	for _, c := range cases {
		c.q.State = driver.Enqueued
		if got := list(t, s, c.q); !slices.Equal(got, c.want) {
			t.Fatalf("jobs %+v: %v, want %v", c.q, got, c.want)
		}
	}
	if got := list(t, s, driver.JobQuery{State: driver.Scheduled}); len(got) != 0 {
		t.Fatalf("scheduled jobs %v, want none", got)
	}
}

func testJobsPages(t *testing.T, s driver.Store) {
	ps := tasks(7, "q")
	for i, p := range []int16{2, 0, 2, 1, 0, 2, 1} {
		ps[i].Priority = p
	}
	ids := insertedIDs(insert(t, s, ps...))
	want := []int64{ids[0], ids[2], ids[5], ids[3], ids[6], ids[1], ids[4]}
	for _, limit := range []int{1, 3, 7, 100} {
		wantOrder(t, s, driver.JobQuery{State: driver.Enqueued, Limit: limit}, want...)
	}
}

func testJobsLimit(t *testing.T, s driver.Store) {
	const n = 510
	for range 3 {
		insert(t, s, tasks(n/3, "q")...)
	}
	for _, c := range []struct{ limit, max int }{{0, 20}, {1000, 500}} {
		p, err := s.Jobs(t.Context(), driver.JobQuery{State: driver.Enqueued, Limit: c.limit})
		if err != nil {
			t.Fatal(err)
		}
		if len(p.Records) == 0 || len(p.Records) > c.max || p.Next == "" {
			t.Fatalf("limit %d: %d records next %q, want at most %d and more to come", c.limit, len(p.Records), p.Next, c.max)
		}
	}
	if got := list(t, s, driver.JobQuery{State: driver.Enqueued, Limit: 1000}); len(got) != n {
		t.Fatalf("listed %d, want %d", len(got), n)
	}
}

func testCounts(t *testing.T, s driver.Store) {
	wantEmpty(t, s)
	finished := func(q string, out driver.Outcome) {
		j := start(t, s, task(q))
		out.Ref = j.Ref
		apply(t, s, out)
	}
	parent := add(t, s, task("p"))
	add(t, s, after("c", driver.OnSucceeded, parent))
	p := task("s")
	p.Delay = time.Hour
	add(t, s, p)
	finished("r", driver.Outcome{State: driver.Scheduled, Delay: time.Hour, Reason: "retry"})
	add(t, s, limited("l", "k", 1))
	add(t, s, limited("l", "k", 1))
	add(t, s, task("e"))
	start(t, s, task("x"))
	finished("f", driver.Outcome{State: driver.Failed, Reason: "exhausted"})
	finished("s1", driver.Outcome{State: driver.Succeeded})
	finished("s2", driver.Outcome{State: driver.Succeeded})
	finished("d", driver.Outcome{State: driver.Deleted, Reason: "canceled"})

	want := driver.Counts{Awaiting: 1, Scheduled: 2, Throttled: 1, Enqueued: 3, Processing: 1, Failed: 1, Retries: 1, Succeeded: 2, Deleted: 1}
	if c := counts(t, s); c != want {
		t.Fatalf("counts %+v, want %+v", c, want)
	}
}

func testSeries(t *testing.T, s driver.Store) {
	finished := func(q string, out driver.Outcome) {
		j := start(t, s, task(q))
		out.Ref = j.Ref
		apply(t, s, out)
	}
	n0 := now(t, s)
	finished("a", driver.Outcome{State: driver.Succeeded})
	finished("b", driver.Outcome{State: driver.Succeeded})
	finished("c", driver.Outcome{State: driver.Failed, Reason: "exhausted"})
	finished("d", driver.Outcome{State: driver.Deleted, Reason: "canceled"})
	finished("e", driver.Outcome{State: driver.Scheduled, Delay: time.Hour, Reason: "retry"})
	finished("f", driver.Outcome{State: driver.Scheduled, Delay: time.Hour, Refund: true, Reason: "snoozed"})
	n1 := now(t, s)

	for _, step := range []time.Duration{time.Minute, 5 * time.Minute} {
		ps, err := s.Series(t.Context(), n0.Add(-10*time.Minute), n1.Add(10*time.Minute), step)
		if err != nil {
			t.Fatal(err)
		}
		var sum driver.Point
		for i, p := range ps {
			if p.At.Unix()%int64(step/time.Second) != 0 || p.At.Nanosecond() != 0 {
				t.Fatalf("step %v: bucket %v not aligned", step, p.At)
			}
			if i > 0 && !p.At.After(ps[i-1].At) {
				t.Fatalf("step %v: bucket %v after %v", step, p.At, ps[i-1].At)
			}
			if p.Succeeded+p.Failed+p.Deleted+p.Retried == 0 {
				t.Fatalf("step %v: empty bucket %v returned", step, p.At)
			}
			sum.Succeeded += p.Succeeded
			sum.Failed += p.Failed
			sum.Deleted += p.Deleted
			sum.Retried += p.Retried
		}
		if sum.Succeeded != 2 || sum.Failed != 1 || sum.Deleted != 1 || sum.Retried != 1 {
			t.Fatalf("step %v: totals %+v, want 2 succeeded, 1 failed, 1 deleted, 1 retried", step, sum)
		}
	}
	ps, err := s.Series(t.Context(), n1.Add(time.Hour), n1.Add(2*time.Hour), time.Minute)
	if err != nil || len(ps) != 0 {
		t.Fatalf("future series %+v %v, want none", ps, err)
	}
}

func testDeletedStats(t *testing.T, s driver.Store) {
	n0 := now(t, s)
	parent := add(t, s, task("p"))
	add(t, s, after("c", driver.OnSucceeded, parent))
	enqueued := add(t, s, task("q"))
	running := start(t, s, task("r"))
	if n := deleteIDs(t, s, parent, enqueued, running.ID); n != 3 {
		t.Fatalf("deleted %d, want 3", n)
	}
	deleteIDs(t, s, running.ID)
	apply(t, s, outcome(running, driver.Failed))
	fin := start(t, s, task("f"))
	add(t, s, after("c", driver.OnFailed, fin.ID))
	apply(t, s, outcome(fin, driver.Succeeded))
	if in := insert(t, s, after("c", driver.OnFailed, fin.ID))[0]; in.State != driver.Deleted {
		t.Fatalf("child of succeeded parent inserted as %s, want deleted", in.State)
	}
	n1 := now(t, s)

	const want = 5
	if c := counts(t, s); c.Deleted != want {
		t.Fatalf("all-time deleted %d, want %d", c.Deleted, want)
	}
	ps, err := s.Series(t.Context(), n0.Add(-10*time.Minute), n1.Add(10*time.Minute), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	var sum int64
	for _, p := range ps {
		sum += p.Deleted
	}
	if sum != want {
		t.Fatalf("series deleted %d, want %d", sum, want)
	}
}

func testQueues(t *testing.T, s driver.Store) {
	ctx := t.Context()
	insert(t, s, tasks(3, "q1")...)
	claim(t, s, 1, "q1")
	p := task("q1")
	p.Delay = time.Hour
	add(t, s, p)
	add(t, s, limited("other", "k", 1))
	add(t, s, limited("q1", "k", 1))
	if err := s.PauseQueue(ctx, "q2", true); err != nil {
		t.Fatal(err)
	}
	heartbeat(t, s, driver.ServerInfo{ID: server, Queues: []string{"q3"}})
	time.Sleep(50 * time.Millisecond)

	qs, err := s.Queues(ctx)
	if err != nil {
		t.Fatal(err)
	}
	byName := make(map[string]driver.QueueInfo, len(qs))
	for _, q := range qs {
		if _, dup := byName[q.Name]; dup {
			t.Fatalf("queue %q listed twice", q.Name)
		}
		byName[q.Name] = q
	}
	for _, name := range []string{"q1", "q2", "q3", "other"} {
		if _, ok := byName[name]; !ok {
			t.Fatalf("queues %+v missing %q", qs, name)
		}
	}
	q1 := byName["q1"]
	if q1.Paused || q1.Enqueued != 2 || q1.Processing != 1 || q1.Scheduled != 1 || q1.Throttled != 1 {
		t.Fatalf("q1 %+v", q1)
	}
	if q1.Latency < 45*time.Millisecond || q1.Latency > 10*time.Second {
		t.Fatalf("q1 latency %v, want about 50ms", q1.Latency)
	}
	if q2 := byName["q2"]; !q2.Paused || q2.Enqueued != 0 {
		t.Fatalf("q2 %+v", q2)
	}
}

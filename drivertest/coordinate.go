package drivertest

import (
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

var coordinateTests = []test{
	{"Now", testNow},
	{"Lead", testLead},
	{"Resign", testResign},
	{"Promote", testPromote},
	{"PromoteLimit", testPromoteLimit},
	{"PromoteConcurrent", testPromoteConcurrent},
	{"Heartbeat", testHeartbeat},
	{"HeartbeatStarted", testHeartbeatStarted},
	{"Unregister", testUnregister},
	{"Orphans", testOrphans},
	{"Sweep", testSweep},
	{"Prune", testPrune},
	{"PruneLimit", testPruneLimit},
	{"PruneServers", testPruneServers},
	{"PruneStats", testPruneStats},
	{"PruneBatches", testPruneBatches},
}

func lead(t *testing.T, s driver.Store, name, holder string, ttl time.Duration) (time.Duration, bool) {
	t.Helper()
	held, ok, err := s.Lead(t.Context(), name, holder, ttl)
	if err != nil {
		t.Fatalf("lead %s as %s: %v", name, holder, err)
	}
	return held, ok
}

func orphans(t *testing.T, s driver.Store, deadAfter time.Duration, limit int) []driver.Orphan {
	t.Helper()
	os, err := s.Orphans(t.Context(), deadAfter, limit)
	if err != nil {
		t.Fatalf("orphans: %v", err)
	}
	return os
}

func orphanIDs(os []driver.Orphan) []int64 {
	ids := make([]int64, len(os))
	for i, o := range os {
		ids[i] = o.ID
	}
	return sorted(ids)
}

func prune(t *testing.T, s driver.Store, p driver.PruneParams) int {
	t.Helper()
	n, err := s.Prune(t.Context(), p)
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	return n
}

func keep() driver.PruneParams {
	return driver.PruneParams{
		Retention: driver.Retention{Succeeded: time.Hour, Deleted: time.Hour, Failed: -1},
		Servers:   time.Hour,
		Stats:     24 * time.Hour,
		Limit:     1000,
	}
}

func gone(t *testing.T, s driver.Store, id int64) bool {
	t.Helper()
	_, err := s.Job(t.Context(), id)
	if err != nil {
		wantErr(t, err, driver.ErrNotFound)
		return true
	}
	return false
}

func testNow(t *testing.T, s driver.Store) {
	a := now(t, s)
	time.Sleep(20 * time.Millisecond)
	b := now(t, s)
	if d := b.Sub(a); d < 15*time.Millisecond || d > time.Second {
		t.Fatalf("store clock advanced %v over a 20ms sleep", d)
	}
}

func testLead(t *testing.T, s driver.Store) {
	if held, ok := lead(t, s, "leader", "a", time.Minute); !ok || held != 0 {
		t.Fatalf("first lead: held %v ok %v, want 0 true", held, ok)
	}
	time.Sleep(30 * time.Millisecond)
	held, ok := lead(t, s, "leader", "a", time.Minute)
	if !ok || held < 25*time.Millisecond {
		t.Fatalf("renew: held %v ok %v, want >= 25ms true", held, ok)
	}
	time.Sleep(20 * time.Millisecond)
	if again, ok := lead(t, s, "leader", "a", time.Minute); !ok || again <= held {
		t.Fatalf("second renew: held %v ok %v, want > %v", again, ok, held)
	}
	if _, ok := lead(t, s, "leader", "b", time.Minute); ok {
		t.Fatal("b acquired a lease held by a")
	}
	if held, ok := lead(t, s, "other", "b", time.Minute); !ok || held != 0 {
		t.Fatalf("b on other name: held %v ok %v", held, ok)
	}

	const ttl = 100 * time.Millisecond
	began := time.Now()
	if _, ok := lead(t, s, "leader", "a", ttl); !ok {
		t.Fatal("a lost the lease while renewing it")
	}
	var took time.Duration
	eventually(t, time.Second, func() bool {
		held, ok := lead(t, s, "leader", "b", time.Minute)
		took = held
		return ok
	})
	if el := time.Since(began); el < ttl-5*time.Millisecond {
		t.Fatalf("b took the lease %v after a renewed it for %v", el, ttl)
	}
	if took != 0 {
		t.Fatalf("b acquired expired lease with held %v, want 0", took)
	}
	if _, ok := lead(t, s, "leader", "a", time.Minute); ok {
		t.Fatal("a renewed a lease now held by b")
	}
}

func testResign(t *testing.T, s driver.Store) {
	ctx := t.Context()
	const ttl = time.Minute
	lead(t, s, "leader", "a", ttl)
	if err := s.Resign(ctx, "leader", "b"); err != nil {
		t.Fatal(err)
	}
	if _, ok := lead(t, s, "leader", "b", ttl); ok {
		t.Fatal("resign by non-holder released the lease")
	}
	if err := s.Resign(ctx, "leader", "a"); err != nil {
		t.Fatal(err)
	}
	if held, ok := lead(t, s, "leader", "b", ttl); !ok || held != 0 {
		t.Fatalf("lead after resign: held %v ok %v, want 0 true", held, ok)
	}
	if err := s.Resign(ctx, "nothing", "x"); err != nil {
		t.Fatal(err)
	}
}

func testPromote(t *testing.T, s driver.Store) {
	if p := promote(t, s, 100); p.Count != 0 || p.Next != 0 || len(p.Queues) != 0 {
		t.Fatalf("promote on empty store: %+v", p)
	}
	const delay = 150 * time.Millisecond
	ps := []driver.InsertParams{task("x"), task("x"), task("y"), task("z")}
	for i := range ps {
		ps[i].Delay = delay
	}
	ps[3].Delay = time.Hour
	began := time.Now()
	ids := insertedIDs(insert(t, s, ps...))

	total := 0
	queues := make(map[string]bool)
	collect := func(p driver.Promoted) {
		total += p.Count
		if distinct := slices.Compact(slices.Sorted(slices.Values(p.Queues))); len(distinct) != len(p.Queues) {
			t.Fatalf("promoted queues %v repeat a queue", p.Queues)
		}
		for _, q := range p.Queues {
			queues[q] = true
		}
	}
	p := promote(t, s, 100)
	if time.Since(began) < delay && (p.Count != 0 || p.Next <= 0 || p.Next > delay) {
		t.Fatalf("early promote %+v, want nothing and next in (0, %v]", p, delay)
	}
	collect(p)
	eventually(t, time.Second, func() bool {
		if total < 3 {
			p = promote(t, s, 100)
			collect(p)
		}
		return total >= 3
	})
	if total != 3 {
		t.Fatalf("promoted %d, want 3", total)
	}
	if !queues["x"] || !queues["y"] || queues["z"] || len(queues) != 2 {
		t.Fatalf("promoted queues %v, want x and y", queues)
	}
	if p.Next <= delay || p.Next > time.Hour {
		t.Fatalf("next %v, want about an hour", p.Next)
	}
	wantState(t, s, driver.Enqueued, ids[:3]...)
	wantState(t, s, driver.Scheduled, ids[3])
	deleteIDs(t, s, ids[3])
	if p := promote(t, s, 100); p.Count != 0 || p.Next != 0 {
		t.Fatalf("promote with nothing scheduled: %+v", p)
	}
}

func testPromoteLimit(t *testing.T, s driver.Store) {
	ps := tasks(6, "q")
	for i := range ps {
		ps[i].Delay = 20 * time.Millisecond
	}
	insert(t, s, ps...)
	time.Sleep(40 * time.Millisecond)
	total := 0
	for range 10 {
		p := promote(t, s, 2)
		if p.Count > 2 {
			t.Fatalf("promoted %d with limit 2", p.Count)
		}
		if total += p.Count; total == 6 {
			break
		}
		if p.Next <= 0 || p.Next > 10*time.Millisecond {
			t.Fatalf("next %v with %d due jobs left, want a small positive duration", p.Next, 6-total)
		}
	}
	if c := counts(t, s); total != 6 || c.Enqueued != 6 || c.Scheduled != 0 {
		t.Fatalf("promoted %d, counts %+v", total, c)
	}
}

func testPromoteConcurrent(t *testing.T, s driver.Store) {
	const n, workers = 300, 8
	ps := tasks(n, "q")
	for i := range ps {
		ps[i].Delay = 20 * time.Millisecond
	}
	insert(t, s, ps...)
	time.Sleep(40 * time.Millisecond)

	ctx := t.Context()
	deadline := time.Now().Add(3 * time.Second)
	var (
		total atomic.Int64
		wg    sync.WaitGroup
	)
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for total.Load() < n && time.Now().Before(deadline) {
				p, err := s.Promote(ctx, 25)
				if err != nil {
					t.Error(err)
					return
				}
				total.Add(int64(p.Count))
			}
		}()
	}
	wg.Wait()
	if total.Load() != n {
		t.Fatalf("promoted %d, want %d", total.Load(), n)
	}
	if c := counts(t, s); c.Enqueued != n || c.Scheduled != 0 {
		t.Fatalf("counts %+v", c)
	}
}

func testHeartbeat(t *testing.T, s driver.Store) {
	ctx := t.Context()
	info := driver.ServerInfo{
		ID: server, Host: "host-1", PID: 42, Version: "v1.2.3",
		Queues: []string{"a", "b"}, Kinds: []string{"task"}, Workers: 4, Running: 1,
		StartedAt: now(t, s).Add(-time.Hour).Truncate(time.Second),
	}
	if d := heartbeat(t, s, info); len(d.Leases) != 0 || len(d.Paused) != 0 {
		t.Fatalf("directives %+v on empty store", d)
	}

	j := start(t, s, task("a"))
	add(t, s, task("b"))
	claimAs(t, s, "s2", 1, "b")
	time.Sleep(40 * time.Millisecond)
	d := heartbeat(t, s, info)
	if len(d.Leases) != 1 || d.Leases[0].Ref != j.Ref || d.Leases[0].Cancel {
		t.Fatalf("leases %+v, want only %+v", d.Leases, j.Ref)
	}
	if age := d.Leases[0].Age; age < 35*time.Millisecond || age > 5*time.Second {
		t.Fatalf("lease age %v, want about 40ms", age)
	}

	deleteIDs(t, s, j.ID)
	if err := s.PauseQueue(ctx, "b", true); err != nil {
		t.Fatal(err)
	}
	d = heartbeat(t, s, info)
	if len(d.Leases) != 1 || !d.Leases[0].Cancel {
		t.Fatalf("leases %+v, want cancel requested", d.Leases)
	}
	if !slices.Equal(d.Paused, []string{"b"}) {
		t.Fatalf("paused %v, want [b]", d.Paused)
	}

	info.Running = 3
	n0 := now(t, s)
	heartbeat(t, s, info)
	n1 := now(t, s)
	srvs, err := s.Servers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	i := slices.IndexFunc(srvs, func(v driver.ServerInfo) bool { return v.ID == server })
	if i < 0 || len(srvs) != 1 {
		t.Fatalf("servers %+v, want only %s", srvs, server)
	}
	got := srvs[i]
	switch {
	case got.Host != info.Host || got.PID != info.PID || got.Version != info.Version:
		t.Fatalf("server %+v", got)
	case !slices.Equal(got.Queues, info.Queues) || !slices.Equal(got.Kinds, info.Kinds):
		t.Fatalf("server queues %v kinds %v", got.Queues, got.Kinds)
	case got.Workers != 4 || got.Running != 3:
		t.Fatalf("server workers %d running %d", got.Workers, got.Running)
	}
	if !got.StartedAt.Equal(info.StartedAt) {
		t.Fatalf("started at %v, want %v as given", got.StartedAt, info.StartedAt)
	}
	within(t, "heartbeat at", got.HeartbeatAt, n0, n1)

	apply(t, s, outcome(j, driver.Succeeded))
	if d := heartbeat(t, s, info); len(d.Leases) != 0 {
		t.Fatalf("leases %+v after finish", d.Leases)
	}
}

func testHeartbeatStarted(t *testing.T, s driver.Store) {
	given := now(t, s).Add(-time.Hour).Truncate(time.Second)
	heartbeat(t, s, driver.ServerInfo{ID: "given", StartedAt: given})
	b0 := now(t, s)
	heartbeat(t, s, driver.ServerInfo{ID: "stamped"})
	b1 := now(t, s)
	time.Sleep(20 * time.Millisecond)
	heartbeat(t, s, driver.ServerInfo{ID: "given", StartedAt: given})
	heartbeat(t, s, driver.ServerInfo{ID: "stamped"})
	srvs, err := s.Servers(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(srvs) != 2 {
		t.Fatalf("servers %+v, want given and stamped", srvs)
	}
	for _, v := range srvs {
		switch v.ID {
		case "given":
			if !v.StartedAt.Equal(given) {
				t.Fatalf("started at %v, want %v as given", v.StartedAt, given)
			}
		case "stamped":
			within(t, "stamped started at", v.StartedAt, b0, b1)
		}
	}
}

func testUnregister(t *testing.T, s driver.Store) {
	ctx := t.Context()
	heartbeat(t, s, driver.ServerInfo{ID: server})
	heartbeat(t, s, driver.ServerInfo{ID: "s2"})
	j := start(t, s, task("q"))
	if os := orphans(t, s, time.Hour, 10); len(os) != 0 {
		t.Fatalf("orphans %+v with live server", os)
	}
	if err := s.Unregister(ctx, server); err != nil {
		t.Fatal(err)
	}
	srvs, err := s.Servers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(srvs) != 1 || srvs[0].ID != "s2" {
		t.Fatalf("servers %+v, want only s2", srvs)
	}
	if got := orphanIDs(orphans(t, s, time.Hour, 10)); !slices.Equal(got, []int64{j.ID}) {
		t.Fatalf("orphans %v, want [%d]", got, j.ID)
	}
}

func testOrphans(t *testing.T, s driver.Store) {
	p := task("a")
	p.MaxAttempts = 7
	heartbeat(t, s, driver.ServerInfo{ID: server})
	alive := start(t, s, p)
	add(t, s, task("b"))
	js := claimAs(t, s, "ghost", 1, "b")
	if len(js) != 1 {
		t.Fatalf("ghost claimed %d jobs, want 1", len(js))
	}
	ghost := js[0]

	os := orphans(t, s, time.Hour, 10)
	if len(os) != 1 {
		t.Fatalf("orphans %+v, want only job %d", os, ghost.ID)
	}
	want := driver.Orphan{Ref: ghost.Ref, Kind: "task", Queue: "b", Attempt: 1, MaxAttempts: 5, Server: "ghost"}
	if os[0] != want {
		t.Fatalf("orphan %+v, want %+v", os[0], want)
	}

	time.Sleep(60 * time.Millisecond)
	if got := orphanIDs(orphans(t, s, 30*time.Millisecond, 10)); !slices.Equal(got, sorted([]int64{alive.ID, ghost.ID})) {
		t.Fatalf("orphans %v, want %d and %d", got, alive.ID, ghost.ID)
	}
	if os := orphans(t, s, 30*time.Millisecond, 1); len(os) != 1 {
		t.Fatalf("%d orphans with limit 1", len(os))
	}
	for _, o := range orphans(t, s, 30*time.Millisecond, 10) {
		if o.ID == alive.ID && (o.MaxAttempts != 7 || o.Server != server) {
			t.Fatalf("orphan %+v", o)
		}
	}
	wantState(t, s, driver.Processing, alive.ID, ghost.ID)

	beat := time.Now()
	heartbeat(t, s, driver.ServerInfo{ID: server})
	os = orphans(t, s, 30*time.Millisecond, 10)
	if time.Since(beat) < 30*time.Millisecond && slices.ContainsFunc(os, func(o driver.Orphan) bool { return o.ID == alive.ID }) {
		t.Fatalf("orphans %v include job %d right after its server heartbeat", orphanIDs(os), alive.ID)
	}
	deleteIDs(t, s, ghost.ID)
	os = orphans(t, s, time.Hour, 10)
	if len(os) != 1 || os[0].ID != ghost.ID || !os[0].Cancel {
		t.Fatalf("orphans %+v, want job %d with cancel", os, ghost.ID)
	}
	apply(t, s, driver.Outcome{Ref: os[0].Ref, State: driver.Scheduled, Delay: time.Hour, Reason: "orphaned"})
	wantState(t, s, driver.Deleted, ghost.ID)
	if os := orphans(t, s, time.Hour, 10); len(os) != 0 {
		t.Fatalf("orphans %+v after rescue", os)
	}
}

func testSweep(t *testing.T, s driver.Store) {
	parent := add(t, s, task("p"))
	awaiting := add(t, s, after("c", driver.OnSucceeded, parent))
	p := task("s")
	p.Delay = time.Hour
	scheduled := add(t, s, p)
	blocker := add(t, s, limited("l", "k", 1))
	throttled := add(t, s, limited("l", "k", 1))
	failed := start(t, s, task("f"))
	apply(t, s, outcome(failed, driver.Failed))
	running := start(t, s, task("r"))
	bid := openBatch(t, s)
	member := add(t, s, members("m", bid, 1)[0])
	seal(t, s, bid)

	for range 3 {
		if n, err := s.Sweep(t.Context(), 1000); err != nil || n != 0 {
			t.Fatalf("sweep of a consistent store changed %d rows, %v", n, err)
		}
	}
	wantState(t, s, driver.Enqueued, parent, blocker, member)
	wantState(t, s, driver.Awaiting, awaiting)
	wantState(t, s, driver.Scheduled, scheduled)
	wantState(t, s, driver.Throttled, throttled)
	wantState(t, s, driver.Failed, failed.ID)
	wantState(t, s, driver.Processing, running.ID)
	wantFinished(t, s, bid, false)
}

func testPrune(t *testing.T, s driver.Store) {
	finished := func(q string, st driver.State) int64 {
		j := start(t, s, task(q))
		apply(t, s, outcome(j, st))
		return j.ID
	}
	succeeded := finished("a", driver.Succeeded)
	deleted := finished("b", driver.Deleted)
	failed := finished("c", driver.Failed)
	enqueued := add(t, s, task("d"))
	p := task("e")
	p.Delay = time.Hour
	scheduled := add(t, s, p)

	if n := prune(t, s, keep()); n != 0 {
		t.Fatalf("pruned %d within retention", n)
	}
	time.Sleep(30 * time.Millisecond)

	steps := []struct {
		edit func(*driver.PruneParams)
		id   int64
	}{
		{func(p *driver.PruneParams) { p.Succeeded = 10 * time.Millisecond }, succeeded},
		{func(p *driver.PruneParams) { p.Deleted = 10 * time.Millisecond }, deleted},
		{func(p *driver.PruneParams) { p.Failed = 10 * time.Millisecond }, failed},
	}
	for i, step := range steps {
		pp := keep()
		step.edit(&pp)
		if n := prune(t, s, pp); n < 1 {
			t.Fatalf("step %d pruned %d rows", i, n)
		}
		for j, other := range steps {
			if got := gone(t, s, other.id); got != (j <= i) {
				t.Fatalf("after step %d job %d gone = %v", i, other.id, got)
			}
		}
	}
	wantState(t, s, driver.Enqueued, enqueued)
	wantState(t, s, driver.Scheduled, scheduled)
	if c := counts(t, s); c.Succeeded != 1 || c.Deleted != 1 {
		t.Fatalf("all-time totals %+v changed by pruning jobs", c)
	}
	_, err := s.Insert(t.Context(), []driver.InsertParams{after("q", driver.OnFinished, succeeded)})
	wantErr(t, err, driver.ErrNotFound)
}

func testPruneLimit(t *testing.T, s driver.Store) {
	insert(t, s, tasks(5, "q")...)
	js := claimN(t, s, 5, "q")
	for _, j := range js {
		apply(t, s, outcome(j, driver.Succeeded))
	}
	time.Sleep(20 * time.Millisecond)
	pp := keep()
	pp.Succeeded, pp.Limit = 5*time.Millisecond, 2
	left := len(js)
	for range 10 {
		n := prune(t, s, pp)
		if n > 2 {
			t.Fatalf("pruned %d rows with limit 2", n)
		}
		left -= n
		remaining := 0
		for _, j := range js {
			if !gone(t, s, j.ID) {
				remaining++
			}
		}
		if remaining != left {
			t.Fatalf("prune reported %d, %d jobs remain", n, remaining)
		}
		if left == 0 {
			return
		}
	}
	t.Fatalf("%d jobs left after 10 prunes", left)
}

func testPruneServers(t *testing.T, s driver.Store) {
	heartbeat(t, s, driver.ServerInfo{ID: "old"})
	heartbeat(t, s, driver.ServerInfo{ID: "new"})
	time.Sleep(150 * time.Millisecond)
	if n := prune(t, s, keep()); n != 0 {
		t.Fatalf("pruned %d with servers retention of an hour", n)
	}
	pp := keep()
	pp.Servers = 100 * time.Millisecond
	beat := time.Now()
	heartbeat(t, s, driver.ServerInfo{ID: "new"})
	n := prune(t, s, pp)
	fresh := time.Since(beat) < pp.Servers
	srvs, err := s.Servers(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if slices.ContainsFunc(srvs, func(v driver.ServerInfo) bool { return v.ID == "old" }) {
		t.Fatalf("servers %+v, want old pruned", srvs)
	}
	if fresh && (n != 1 || len(srvs) != 1 || srvs[0].ID != "new") {
		t.Fatalf("pruned %d, servers %+v, want only old pruned", n, srvs)
	}
}

func testPruneStats(t *testing.T, s driver.Store) {
	insert(t, s, tasks(3, "q")...)
	js := claimN(t, s, 3, "q")
	apply(t, s, outcome(js[0], driver.Succeeded), outcome(js[1], driver.Succeeded), outcome(js[2], driver.Deleted))
	before := counts(t, s)
	if before.Succeeded != 2 || before.Deleted != 1 {
		t.Fatalf("counts %+v", before)
	}
	time.Sleep(5 * time.Millisecond)
	pp := keep()
	pp.Stats = time.Millisecond
	prune(t, s, pp)
	prune(t, s, pp)
	if c := counts(t, s); c != before {
		t.Fatalf("counts %+v after folding stats, want %+v", c, before)
	}
	pp.Succeeded, pp.Deleted = time.Millisecond, time.Millisecond
	prune(t, s, pp)
	if c := counts(t, s); c.Succeeded != 2 || c.Deleted != 1 {
		t.Fatalf("counts %+v after pruning jobs and stats", c)
	}
}

func testPruneBatches(t *testing.T, s driver.Store) {
	done := openBatch(t, s)
	m := add(t, s, members("m", done, 1)[0])
	apply(t, s, outcome(claimOne(t, s, "m", m), driver.Succeeded))
	seal(t, s, done)
	wantFinished(t, s, done, true)
	open := openBatch(t, s)
	busy := openBatch(t, s)
	add(t, s, members("m", busy, 1)[0])
	seal(t, s, busy)
	time.Sleep(20 * time.Millisecond)

	pp := keep()
	pp.Succeeded = 5 * time.Millisecond
	for range 3 {
		prune(t, s, pp)
	}
	if !gone(t, s, m) {
		t.Fatalf("member %d not pruned", m)
	}
	_, err := s.Batch(t.Context(), done)
	wantErr(t, err, driver.ErrNotFound)
	batch(t, s, open)
	batch(t, s, busy)
}

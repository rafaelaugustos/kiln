package memstore_test

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

func TestLead(t *testing.T) {
	t.Parallel()
	s, c := open(t)
	lead := func(holder string) (time.Duration, bool) {
		held, ok, err := s.Lead(ctx, "leader", holder, 15*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		return held, ok
	}
	if held, ok := lead("a"); !ok || held != 0 {
		t.Fatalf("acquire = %v %v", held, ok)
	}
	c.add(5 * time.Second)
	if held, ok := lead("a"); !ok || held != 5*time.Second {
		t.Fatalf("renew = %v %v", held, ok)
	}
	if _, ok := lead("b"); ok {
		t.Fatalf("second holder acquired a held lease")
	}
	c.add(16 * time.Second)
	if held, ok := lead("b"); !ok || held != 0 {
		t.Fatalf("takeover = %v %v", held, ok)
	}
	if err := s.Resign(ctx, "leader", "a"); err != nil {
		t.Fatal(err)
	}
	if _, ok := lead("a"); ok {
		t.Fatalf("resign by a non-holder released the lease")
	}
	if err := s.Resign(ctx, "leader", "b"); err != nil {
		t.Fatal(err)
	}
	if _, ok := lead("a"); !ok {
		t.Fatalf("lease not released")
	}
}

func TestHeartbeatAndOrphans(t *testing.T) {
	t.Parallel()
	s, c := open(t)
	got := ids(t, s, params("a"), params("a"), params("a"))
	js := claim(t, s, 2)
	info := driver.ServerInfo{ID: "s1", Host: "h", Queues: []string{"default", "idle"}}
	beat(t, s, info)
	c.add(3 * time.Second)
	if _, err := s.Delete(ctx, driver.Filter{IDs: got[1:2]}); err != nil {
		t.Fatal(err)
	}
	if err := s.PauseQueue(ctx, "idle", true); err != nil {
		t.Fatal(err)
	}
	d := beat(t, s, info)
	want := []driver.Lease{
		{Ref: js[0].Ref, Age: 3 * time.Second},
		{Ref: js[1].Ref, Cancel: true, Age: 3 * time.Second},
	}
	if !slices.Equal(d.Leases, want) || !slices.Equal(d.Paused, []string{"idle"}) {
		t.Fatalf("directives = %+v", d)
	}
	servers, _ := s.Servers(ctx)
	if len(servers) != 1 || !servers[0].StartedAt.Equal(epoch) || !servers[0].HeartbeatAt.Equal(c.now()) {
		t.Fatalf("servers = %+v", servers)
	}

	orphans := func() []int64 {
		os, err := s.Orphans(ctx, time.Minute, 10)
		if err != nil {
			t.Fatal(err)
		}
		var out []int64
		for _, o := range os {
			out = append(out, o.ID)
		}
		return out
	}
	if o := orphans(); len(o) != 0 {
		t.Fatalf("live server has orphans %v", o)
	}
	c.add(61 * time.Second)
	if o := orphans(); !slices.Equal(o, got[:2]) {
		t.Fatalf("orphans = %v", o)
	}
	beat(t, s, info)
	if o := orphans(); len(o) != 0 {
		t.Fatalf("orphans after heartbeat %v", o)
	}
	if err := s.Unregister(ctx, "s1"); err != nil {
		t.Fatal(err)
	}
	if o := orphans(); !slices.Equal(o, got[:2]) {
		t.Fatalf("orphans after unregister = %v", o)
	}
}

func TestPromote(t *testing.T) {
	t.Parallel()
	s, c := open(t)
	got := ids(t, s,
		params("a", delay(time.Minute)),
		params("a", queue("b"), delay(2*time.Minute)),
		params("a", queue("c"), delay(time.Minute), limited("k", 1)),
	)
	p, err := s.Promote(ctx, 100)
	if err != nil || p.Count != 0 || p.Next != time.Minute {
		t.Fatalf("promote early = %+v, %v", p, err)
	}
	c.add(90 * time.Second)
	p, err = s.Promote(ctx, 100)
	if err != nil || p.Count != 2 || p.Next != 30*time.Second || !slices.Equal(p.Queues, []string{"c", "default"}) {
		t.Fatalf("promote = %+v, %v", p, err)
	}
	expectState(t, s, got[0], driver.Enqueued)
	expectState(t, s, got[2], driver.Enqueued)
	c.add(time.Minute)
	if p, _ = s.Promote(ctx, 100); p.Count != 1 || p.Next != 0 {
		t.Fatalf("promote last = %+v", p)
	}
}

func TestPrune(t *testing.T) {
	t.Parallel()
	s, c := open(t)
	old := ids(t, s, params("a"), params("a"), params("a"))
	finish(t, s, done(claimOne(t, s), driver.Succeeded))
	finish(t, s, done(claimOne(t, s), driver.Failed))
	if _, err := s.Delete(ctx, driver.Filter{IDs: old[2:]}); err != nil {
		t.Fatal(err)
	}
	beat(t, s, driver.ServerInfo{ID: "gone"})
	insert(t, s, params("w", queue("w"), unique("w", time.Minute)))
	c.add(25 * time.Hour)
	fresh := ids(t, s, params("b"))
	finish(t, s, done(claimOne(t, s), driver.Succeeded))
	beat(t, s, driver.ServerInfo{ID: "alive"})

	n, err := s.Prune(ctx, driver.PruneParams{Succeeded: 24 * time.Hour, Deleted: 24 * time.Hour, Failed: -1})
	if err != nil {
		t.Fatal(err)
	}
	if n < 4 {
		t.Fatalf("pruned %d rows", n)
	}
	for _, id := range []int64{old[0], old[2]} {
		if _, err := s.Job(ctx, id); !errors.Is(err, driver.ErrNotFound) {
			t.Fatalf("job %d survived prune: %v", id, err)
		}
	}
	expectState(t, s, old[1], driver.Failed)
	expectState(t, s, fresh[0], driver.Succeeded)
	if servers, _ := s.Servers(ctx); len(servers) != 1 || servers[0].ID != "alive" {
		t.Fatalf("servers after prune = %+v", servers)
	}
	if cs, _ := s.Counts(ctx); cs.Succeeded != 2 || cs.Deleted != 1 || cs.Failed != 1 {
		t.Fatalf("totals changed by prune: %+v", cs)
	}
	if n, _ := s.Prune(ctx, driver.PruneParams{Failed: 0}); n == 0 {
		t.Fatalf("failed retention did not prune")
	}
	if _, err := s.Job(ctx, old[1]); !errors.Is(err, driver.ErrNotFound) {
		t.Fatalf("failed job survived: %v", err)
	}
}

func TestRecurring(t *testing.T) {
	t.Parallel()
	s, c := open(t)
	r := driver.Recurring{
		ID:        "nightly",
		Spec:      "* * * * *",
		Location:  "UTC",
		Template:  params("cron", unique("cron", 0)),
		NextRunAt: epoch.Add(time.Minute),
	}
	if err := s.PutRecurring(ctx, r); err != nil {
		t.Fatal(err)
	}
	if err := s.PutRecurring(ctx, r); !errors.Is(err, driver.ErrConflict) {
		t.Fatalf("create twice: %v", err)
	}
	cur, err := s.Recurring(ctx, "nightly")
	if err != nil || cur.Version != 1 || !cur.CreatedAt.Equal(epoch) {
		t.Fatalf("recurring = %+v, %v", cur, err)
	}
	cur.Template.Priority = 3
	if err := s.PutRecurring(ctx, cur); err != nil {
		t.Fatal(err)
	}
	if err := s.PutRecurring(ctx, cur); !errors.Is(err, driver.ErrConflict) {
		t.Fatalf("stale update: %v", err)
	}

	due, now, err := s.Due(ctx, 10)
	if err != nil || len(due) != 0 || !now.Equal(epoch) {
		t.Fatalf("due early = %v %v %v", due, now, err)
	}
	c.add(time.Minute)
	due, _, _ = s.Due(ctx, 10)
	if len(due) != 1 || due[0].Version != 2 || due[0].Template.Priority != 3 {
		t.Fatalf("due = %+v", due)
	}
	f := driver.Fire{ID: "nightly", Version: 1, NextRunAt: epoch.Add(2 * time.Minute), LastRunAt: epoch.Add(time.Minute), Jobs: []driver.InsertParams{due[0].Template}}
	if _, err := s.Fire(ctx, f); !errors.Is(err, driver.ErrConflict) {
		t.Fatalf("stale fire: %v", err)
	}
	f.Version = 2
	res, err := s.Fire(ctx, f)
	if err != nil || len(res) != 1 || res[0].Duplicate {
		t.Fatalf("fire = %+v, %v", res, err)
	}
	cur, _ = s.Recurring(ctx, "nightly")
	if cur.Version != 3 || cur.LastJobID != res[0].ID || !cur.NextRunAt.Equal(f.NextRunAt) || !cur.LastRunAt.Equal(f.LastRunAt) {
		t.Fatalf("after fire = %+v", cur)
	}
	if due, _, _ = s.Due(ctx, 10); len(due) != 0 {
		t.Fatalf("still due after fire")
	}
	f.Version = 3
	if res, _ := s.Fire(ctx, f); len(res) != 1 || !res[0].Duplicate {
		t.Fatalf("overlapping fire = %+v", res)
	}
	if err := s.RemoveRecurring(ctx, "nightly"); err != nil {
		t.Fatal(err)
	}
	if err := s.RemoveRecurring(ctx, "nightly"); !errors.Is(err, driver.ErrNotFound) {
		t.Fatalf("remove twice: %v", err)
	}
}

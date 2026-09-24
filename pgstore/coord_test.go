package pgstore

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

func TestClaimOrder(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	q := func(name string) func(*driver.InsertParams) { return func(p *driver.InsertParams) { p.Queue = name } }
	res := insert(t, s, job("a", q("low")), job("a", q("high")), job("b", q("high")), job("a", q("low")), job("a", q("mid")))
	if err := s.PauseQueue(ctx, "mid", true); err != nil {
		t.Fatal(err)
	}
	got, err := s.Claim(ctx, driver.ClaimQuery{Queues: []string{"high", "mid", "low"}, Limit: 3, Server: "srv"})
	if err != nil {
		t.Fatal(err)
	}
	ids := []int64{}
	for _, j := range got {
		ids = append(ids, j.ID)
	}
	if want := []int64{res[1].ID, res[2].ID, res[0].ID}; !reflect.DeepEqual(ids, want) {
		t.Fatalf("claimed %v, want %v", ids, want)
	}
	got, err = s.Claim(ctx, driver.ClaimQuery{Queues: []string{"mid", "low"}, Kinds: []string{"b"}, Limit: 5, Server: "srv"})
	if err != nil || len(got) != 0 {
		t.Fatalf("kinds filter %v %v", got, err)
	}
	if err := s.PauseQueue(ctx, "mid", false); err != nil {
		t.Fatal(err)
	}
	got, err = s.Claim(ctx, driver.ClaimQuery{Queues: []string{"mid", "low"}, Limit: 5, Server: "srv"})
	if err != nil || len(got) != 2 || got[0].ID != res[4].ID || got[1].ID != res[3].ID {
		t.Fatalf("after resume %+v %v", got, err)
	}
}

func TestPromote(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	delay := func(d time.Duration) func(*driver.InsertParams) {
		return func(p *driver.InsertParams) { p.Delay = d }
	}
	queue := func(q string) func(*driver.InsertParams) { return func(p *driver.InsertParams) { p.Queue = q } }
	res := insert(t, s,
		job("a", delay(50*time.Millisecond), queue("q1")),
		job("a", delay(400*time.Millisecond), queue("q2")),
		job("a", delay(50*time.Millisecond), limited("k", 1)),
		job("a", delay(time.Hour)),
	)
	for _, r := range res {
		if r.State != driver.Scheduled {
			t.Fatalf("insert %+v", res)
		}
	}
	time.Sleep(80 * time.Millisecond)
	p, err := s.Promote(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	if p.Count != 2 || !reflect.DeepEqual(sorted(p.Queues), []string{"default", "q1"}) || p.Next <= 0 || p.Next > 350*time.Millisecond {
		t.Fatalf("promoted %+v", p)
	}
	if r := record(t, s, res[2].ID); r.State != driver.Enqueued {
		t.Fatalf("limited job not admitted: %s", r.State)
	}
	time.Sleep(p.Next)
	p, err = s.Promote(ctx, 100)
	if err != nil || p.Count != 1 || p.Next < 50*time.Minute {
		t.Fatalf("second promote %+v %v", p, err)
	}
}

func sorted(ss []string) []string {
	out := append([]string(nil), ss...)
	for i := range out {
		for j := i + 1; j < len(out); j++ {
			if out[j] < out[i] {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out
}

func TestLeaseAndHeartbeat(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	held, ok, err := s.Lead(ctx, "leader", "a", 200*time.Millisecond)
	if err != nil || !ok || held != 0 {
		t.Fatalf("acquire %v %v %v", held, ok, err)
	}
	if _, ok, _ := s.Lead(ctx, "leader", "b", time.Second); ok {
		t.Fatal("stolen lease")
	}
	time.Sleep(50 * time.Millisecond)
	held, ok, _ = s.Lead(ctx, "leader", "a", 200*time.Millisecond)
	if !ok || held < 40*time.Millisecond {
		t.Fatalf("renew held %v", held)
	}
	if err := s.Resign(ctx, "leader", "a"); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := s.Lead(ctx, "leader", "b", time.Second); !ok {
		t.Fatal("lease not released")
	}

	insert(t, s, job("a"), job("a"))
	jobs := claim(t, s, 2)
	if _, err := s.Delete(ctx, driver.Filter{IDs: []int64{jobs[1].ID}}); err != nil {
		t.Fatal(err)
	}
	if err := s.PauseQueue(ctx, "paused", true); err != nil {
		t.Fatal(err)
	}
	d, err := s.Heartbeat(ctx, driver.ServerInfo{ID: "srv", Host: "h", PID: 7, Queues: []string{"default"}, Workers: 4, StartedAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	if len(d.Leases) != 2 || !reflect.DeepEqual(d.Paused, []string{"paused"}) {
		t.Fatalf("directives %+v", d)
	}
	for _, l := range d.Leases {
		if l.Cancel != (l.ID == jobs[1].ID) || l.Age < 0 {
			t.Fatalf("lease %+v", l)
		}
	}
	servers, err := s.Servers(ctx)
	if err != nil || len(servers) != 1 || servers[0].PID != 7 || servers[0].Workers != 4 {
		t.Fatalf("servers %+v %v", servers, err)
	}
	orphans, err := s.Orphans(ctx, time.Minute, 10)
	if err != nil || len(orphans) != 0 {
		t.Fatalf("orphans while alive %+v %v", orphans, err)
	}
	if err := s.Unregister(ctx, "srv"); err != nil {
		t.Fatal(err)
	}
	orphans, err = s.Orphans(ctx, time.Minute, 10)
	if err != nil || len(orphans) != 2 || orphans[0].Server != "srv" || !orphans[1].Cancel {
		t.Fatalf("orphans %+v %v", orphans, err)
	}
}

func TestRecurring(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	if _, err := s.Recurring(ctx, "r"); !errors.Is(err, driver.ErrNotFound) {
		t.Fatalf("missing: %v", err)
	}
	now, _ := s.Now(ctx)
	tmpl := job("report", func(p *driver.InsertParams) {
		p.Args = []byte(`{"b": 1, "a": 2}`)
		p.Meta = map[string]string{"x": "y"}
		p.RecurringID = "r"
	})
	r := driver.Recurring{ID: "r", Spec: "* * * * *", Location: "UTC", Template: tmpl, Misfire: driver.MisfireAll, NextRunAt: now.Add(-time.Minute)}
	if err := s.PutRecurring(ctx, r); err != nil {
		t.Fatal(err)
	}
	if err := s.PutRecurring(ctx, r); !errors.Is(err, driver.ErrConflict) {
		t.Fatalf("duplicate create: %v", err)
	}
	got, err := s.Recurring(ctx, "r")
	if err != nil {
		t.Fatal(err)
	}
	if got.Version != 1 || !reflect.DeepEqual(got.Template, tmpl) || got.Misfire != driver.MisfireAll {
		t.Fatalf("round trip %+v", got)
	}
	due, at, err := s.Due(ctx, 10)
	if err != nil || len(due) != 1 || at.IsZero() {
		t.Fatalf("due %+v %v", due, err)
	}
	fire := driver.Fire{ID: "r", Version: 1, NextRunAt: at.Add(time.Minute), LastRunAt: at, Jobs: []driver.InsertParams{tmpl, tmpl}}
	res, err := s.Fire(ctx, fire)
	if err != nil || len(res) != 2 {
		t.Fatalf("fire %+v %v", res, err)
	}
	if _, err := s.Fire(ctx, fire); !errors.Is(err, driver.ErrConflict) {
		t.Fatalf("stale fire: %v", err)
	}
	got, _ = s.Recurring(ctx, "r")
	if got.Version != 2 || got.LastJobID != res[1].ID || !got.NextRunAt.Equal(fire.NextRunAt.Truncate(time.Microsecond)) {
		t.Fatalf("after fire %+v", got)
	}
	if due, _, _ := s.Due(ctx, 10); len(due) != 0 {
		t.Fatalf("still due %+v", due)
	}
	got.Paused = true
	if err := s.PutRecurring(ctx, got); err != nil {
		t.Fatal(err)
	}
	if err := s.PutRecurring(ctx, got); !errors.Is(err, driver.ErrConflict) {
		t.Fatalf("stale put: %v", err)
	}
	all, err := s.Recurrings(ctx)
	if err != nil || len(all) != 1 || !all[0].Paused || all[0].Version != 3 {
		t.Fatalf("recurrings %+v %v", all, err)
	}
	if err := s.RemoveRecurring(ctx, "r"); err != nil {
		t.Fatal(err)
	}
	if err := s.RemoveRecurring(ctx, "r"); !errors.Is(err, driver.ErrNotFound) {
		t.Fatalf("remove twice: %v", err)
	}
}

func TestClaimTolerantDecoding(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	res := insert(t, s, job("a"), job("a"), job("a"))
	_, err := s.pool.Exec(ctx,
		"UPDATE "+s.schema+".jobs SET meta = '{\"n\": 1}', tags = ARRAY['x', NULL], parents = ARRAY[NULL::bigint] WHERE id = $1", res[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.pool.Exec(ctx, "UPDATE "+s.schema+".jobs SET run_at = 'infinity', created_at = '-infinity' WHERE id = $1", res[1].ID)
	if err != nil {
		t.Fatal(err)
	}
	jobs := claim(t, s, 3)
	if len(jobs) != 3 || jobs[0].Meta != nil || !reflect.DeepEqual(jobs[0].Tags, []string{"x"}) {
		t.Fatalf("claimed %+v", jobs)
	}
	if j := jobs[1]; j.ID != res[1].ID || !j.RunAt.IsZero() || !j.CreatedAt.IsZero() || j.AttemptedAt.IsZero() {
		t.Fatalf("job with infinite timestamps claimed as %+v", j)
	}
}

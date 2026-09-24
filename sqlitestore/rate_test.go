package sqlitestore

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

func rated(key string, rate int, per time.Duration, burst int) func(*driver.InsertParams) {
	return func(p *driver.InsertParams) {
		p.LimitKey, p.LimitRate, p.LimitPer, p.LimitBurst = key, rate, per, burst
	}
}

func counts(t *testing.T, s *Store) driver.Counts {
	t.Helper()
	c, err := s.Counts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestRateBound(t *testing.T) {
	t.Parallel()
	s := open(t)
	jobs := make([]driver.InsertParams, 1500)
	for i := range jobs {
		jobs[i] = job("a", rated("k", 10, time.Second, 1))
	}
	res := insert(t, s, jobs...)
	if c := counts(t, s); c.Enqueued != 1 || c.Scheduled != 999 || c.Throttled != 500 {
		t.Fatalf("after insert %+v, want 1000 jobs through one admission", c)
	}
	if n, err := s.Sweep(context.Background(), 100); err != nil || n != 500 {
		t.Fatalf("sweep = %d, %v, want the other 500 reserved", n, err)
	}
	first := record(t, s, res[0].ID).RunAt
	for _, i := range []int{1, 999, 1000, 1499} {
		want := first.Add(time.Duration(i) * 100 * time.Millisecond)
		if r := record(t, s, res[i].ID); r.State != driver.Scheduled || !r.RunAt.Equal(want) {
			t.Fatalf("job %d is %s at %v, want scheduled at %v", i, r.State, r.RunAt, want)
		}
	}

	for i := range jobs {
		jobs[i] = job("b", limited("m", 2000))
	}
	insert(t, s, jobs...)
	if c := counts(t, s); c.Enqueued != 1501 || c.Throttled != 0 {
		t.Fatalf("after insert %+v, want every job under a plain max admitted at once", c)
	}
}

func TestRateInsertStates(t *testing.T) {
	t.Parallel()
	s := open(t)
	p := job("a", rated("k", 1, time.Minute, 1))
	res := insert(t, s, p, p)
	if res[0].State != driver.Enqueued || res[1].State != driver.Scheduled {
		t.Fatalf("inserted %+v, want the first enqueued and the second in a reserved slot", res)
	}
	if st := insert(t, s, p)[0].State; st != driver.Scheduled {
		t.Fatalf("third job inserted as %s, want scheduled", st)
	}
}

func TestPromoteAdmitsThrottledGrant(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	stmt := strings.NewReplacer("{p}", s.prefix, "{now}", clock).Replace
	exec := func(q string, args ...any) {
		t.Helper()
		if _, err := s.db.ExecContext(ctx, stmt(q), args...); err != nil {
			t.Fatal(err)
		}
	}
	reserved := func() (tat int64, granted int) {
		t.Helper()
		q := stmt("SELECT tat, (SELECT count(*) FROM {p}jobs WHERE granted = 1) FROM {p}limits WHERE limit_key = 'k'")
		if err := s.db.QueryRowContext(ctx, q).Scan(&tat, &granted); err != nil {
			t.Fatal(err)
		}
		return tat, granted
	}
	wantState := func(id int64, want driver.State) {
		t.Helper()
		if st := record(t, s, id).State; st != want {
			t.Fatalf("job %d is %s, want %s", id, st, want)
		}
	}
	slow := rated("k", 1, time.Minute, 1)
	late := func(p *driver.InsertParams) { p.Queue = "late" }
	res := insert(t, s, job("a", slow), job("a", slow, late), job("a", slow))
	running, due, later := res[0].ID, res[1].ID, res[2].ID
	wantState(running, driver.Enqueued)
	wantState(due, driver.Scheduled)
	wantState(later, driver.Scheduled)
	slot := record(t, s, later).RunAt
	tat, granted := reserved()
	if granted != 2 {
		t.Fatalf("%d granted jobs, want both reservations", granted)
	}

	exec("UPDATE {p}jobs SET run_at = {now} WHERE id = ?", due)
	exec("UPDATE {p}jobs SET state = 'throttled' WHERE state = 'scheduled' AND run_at <= {now}")
	wantState(due, driver.Throttled)

	p, err := s.Promote(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	if p.Count != 0 || !slices.Equal(p.Queues, []string{"late"}) {
		t.Fatalf("promote %+v, want nothing promoted and the queue of the granted job woken", p)
	}
	wantState(due, driver.Enqueued)
	wantState(running, driver.Enqueued)
	if r := record(t, s, later); r.State != driver.Scheduled || !r.RunAt.Equal(slot) {
		t.Fatalf("next reservation is %s at %v, want scheduled at %v", r.State, r.RunAt, slot)
	}
	if after, granted := reserved(); after != tat || granted != 1 {
		t.Fatalf("tat %d with %d granted jobs, want tat %d and only the later reservation granted", after, granted, tat)
	}
	if n, err := s.Sweep(ctx, 100); err != nil || n != 0 {
		t.Fatalf("sweep changed %d rows, %v, want nothing to repair", n, err)
	}
}

func TestPendingPlan(t *testing.T) {
	t.Parallel()
	s := open(t)
	rows, err := s.db.QueryContext(context.Background(), "EXPLAIN QUERY PLAN "+s.q.pending)
	var plan []string
	err = each(rows, err, func() error {
		var (
			id, parent, unused int
			detail             string
		)
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			return err
		}
		plan = append(plan, detail)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"USING COVERING INDEX kiln_jobs_due", "USING COVERING INDEX kiln_jobs_granted"} {
		if !slices.ContainsFunc(plan, func(d string) bool { return strings.HasSuffix(d, want) }) {
			t.Errorf("pending plan lacks %q: %q", want, plan)
		}
	}
}

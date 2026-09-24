package mysqlstore

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

func rated(key string, n int, per time.Duration, burst int) func(*driver.InsertParams) {
	return func(p *driver.InsertParams) {
		p.LimitKey, p.LimitRate, p.LimitPer, p.LimitBurst = key, n, per, burst
	}
}

func insertedIDs(res []driver.Inserted) []int64 {
	ids := make([]int64, len(res))
	for i, r := range res {
		ids[i] = r.ID
	}
	return ids
}

func TestRateBound(t *testing.T) {
	t.Parallel()
	for _, limit := range []int{0, 2} {
		t.Run(fmt.Sprint("max=", limit), func(t *testing.T) {
			t.Parallel()
			s := open(t)
			ctx := context.Background()
			ps := make([]driver.InsertParams, 1500)
			for i := range ps {
				ps[i] = job("a", rated("k", 10, time.Second, 1), func(p *driver.InsertParams) { p.LimitMax = limit })
			}
			ids := insertedIDs(insert(t, s, ps...))
			c, err := s.Counts(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if c.Enqueued != 1 || c.Scheduled != 999 || c.Throttled != 500 {
				t.Fatalf("after insert %+v, want 1000 jobs through one admission", c)
			}
			if n, err := s.Sweep(ctx, 100); err != nil || n != 500 {
				t.Fatalf("sweep = %d, %v, want the other 500 reserved", n, err)
			}
			first := record(t, s, ids[1]).RunAt
			for _, i := range []int{2, 999, 1000, 1499} {
				r := record(t, s, ids[i])
				if want := first.Add(time.Duration(i-1) * 100 * time.Millisecond); r.State != driver.Scheduled || !r.RunAt.Equal(want) {
					t.Fatalf("job %d is %s at %v, want scheduled at %v", i, r.State, r.RunAt, want)
				}
			}
			if n, err := s.Sweep(ctx, 100); err != nil || n != 0 {
				t.Fatalf("second sweep = %d, %v, want nothing left to reserve", n, err)
			}
		})
	}
}

func TestRateAdmitsBehindLockedLimit(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	p := job("a", rated("k", 1, time.Minute, 1))
	first := insert(t, s, p)[0]
	tx := begin(t, s)
	if _, err := tx.ExecContext(ctx, "SELECT limit_key FROM kiln_limits WHERE limit_key = 'k' FOR UPDATE"); err != nil {
		t.Fatal(err)
	}
	done := make(chan []driver.Inserted, 1)
	go func() {
		res, err := s.Insert(ctx, []driver.InsertParams{p, p})
		if err != nil {
			t.Error(err)
		}
		done <- res
	}()
	time.Sleep(200 * time.Millisecond)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	res := <-done
	if t.Failed() {
		return
	}
	start := record(t, s, first.ID)
	for i, r := range res {
		got := record(t, s, r.ID)
		if want := start.RunAt.Add(time.Duration(i+1) * time.Minute); got.State != driver.Scheduled || got.RunAt.Sub(want).Abs() > time.Second {
			t.Fatalf("job %d is %s at %v, want scheduled at %v", r.ID, got.State, got.RunAt, want)
		}
	}
	if gap := record(t, s, res[1].ID).RunAt.Sub(record(t, s, res[0].ID).RunAt); gap != time.Minute {
		t.Fatalf("slots %v apart, want a minute", gap)
	}
}

func TestPromoteAdmitsThrottledGrant(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	exec := func(q string, args ...any) {
		t.Helper()
		if _, err := s.db.ExecContext(ctx, render(q, args...)); err != nil {
			t.Fatal(err)
		}
	}
	reserved := func() (time.Time, int) {
		t.Helper()
		var tat stamp
		if err := s.db.QueryRowContext(ctx, "SELECT tat FROM kiln_limits WHERE limit_key = 'k'").Scan(&tat); err != nil {
			t.Fatal(err)
		}
		return tat.Time, count(t, s, "SELECT COUNT(*) FROM kiln_jobs WHERE granted")
	}
	wantState := func(id int64, want driver.State) {
		t.Helper()
		if st := record(t, s, id).State; st != want {
			t.Fatalf("job %d is %s, want %s", id, st, want)
		}
	}
	slow := rated("k", 1, time.Minute, 1)
	late := func(p *driver.InsertParams) { p.Queue = "late" }
	ids := insertedIDs(insert(t, s, job("a", slow), job("a", slow, late), job("a", slow)))
	running, due, later := ids[0], ids[1], ids[2]
	wantState(running, driver.Enqueued)
	wantState(due, driver.Scheduled)
	wantState(later, driver.Scheduled)
	slot := record(t, s, later).RunAt
	tat, granted := reserved()
	if granted != 2 {
		t.Fatalf("%d granted jobs, want both reservations", granted)
	}

	exec("UPDATE kiln_jobs SET run_at = UTC_TIMESTAMP(6) WHERE id = ?", due)
	exec("UPDATE kiln_jobs SET state = 'throttled' WHERE state = 'scheduled' AND run_at <= UTC_TIMESTAMP(6)")
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
	if after, granted := reserved(); !after.Equal(tat) || granted != 1 {
		t.Fatalf("tat %v with %d granted jobs, want tat %v and only the later reservation granted", after, granted, tat)
	}
	if n, err := s.Sweep(ctx, 100); err != nil || n != 0 {
		t.Fatalf("sweep changed %d rows, %v, want nothing to repair", n, err)
	}
}

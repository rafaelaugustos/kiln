package pgstore

import (
	"context"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

func limited(key string, n int) func(*driver.InsertParams) {
	return func(p *driver.InsertParams) { p.LimitKey, p.LimitMax = key, n }
}

func TestLimitAdmission(t *testing.T) {
	t.Parallel()
	limitAdmission(t, open(t))
}

func limitAdmission(t *testing.T, s *Store) {
	t.Helper()
	ctx := context.Background()
	res := insert(t, s,
		job("a", limited("k", 2)),
		job("a", limited("k", 2)),
		job("a", limited("k", 2), func(p *driver.InsertParams) { p.Priority = 5 }),
		job("a", limited("k", 2)),
	)
	states := map[driver.State]int{}
	for _, r := range res {
		states[r.State]++
	}
	if states[driver.Throttled] != 4 {
		t.Fatalf("insert states %+v", res)
	}
	jobs := claim(t, s, 10)
	if len(jobs) != 2 || jobs[0].ID != res[2].ID || jobs[1].ID != res[0].ID {
		t.Fatalf("first admission %+v", jobs)
	}
	finish(t, s, driver.Outcome{Ref: jobs[0].Ref, State: driver.Failed, Reason: "permanent"})
	next := claim(t, s, 10)
	if len(next) != 1 || next[0].ID != res[1].ID {
		t.Fatalf("fifo admission %+v", next)
	}
	if n, err := s.Delete(ctx, driver.Filter{IDs: []int64{res[3].ID}}); err != nil || n != 1 {
		t.Fatalf("delete throttled: %d %v", n, err)
	}
	finish(t, s, driver.Outcome{Ref: next[0].Ref, State: driver.Scheduled, Reason: "retry"})
	again := claim(t, s, 10)
	if len(again) != 1 || again[0].ID != res[1].ID {
		t.Fatalf("readmission %+v", again)
	}
	wantLimit(t, s, "k", 2, 2)
}

func wantLimit(t *testing.T, s *Store, key string, max, active int) {
	t.Helper()
	var (
		gotMax, gotActive, rate, burst int
		per                            int64
		used                           bool
	)
	q := "SELECT max, active, rate, per_us, burst, tat IS NOT NULL FROM " + s.schema + ".limits WHERE key = $1"
	if err := s.pool.QueryRow(context.Background(), q, key).Scan(&gotMax, &gotActive, &rate, &per, &burst, &used); err != nil {
		t.Fatalf("limit %s: %v", key, err)
	}
	if gotMax != max || gotActive != active || rate != 0 || per != 0 || burst != 0 || used {
		t.Fatalf("limit %s: max %d active %d rate %d per %d burst %d tat set %v, want max %d active %d and no rate",
			key, gotMax, gotActive, rate, per, burst, used, max, active)
	}
}

func TestLimitUnderConcurrency(t *testing.T) {
	t.Parallel()
	s := open(t, MaxConns(16))
	ctx := context.Background()
	const (
		max  = 3
		jobs = 300
	)
	ps := make([]driver.InsertParams, jobs)
	for i := range ps {
		ps[i] = job("a", limited("shared", max))
	}
	insert(t, s, ps...)
	var (
		running, peak, done atomic.Int64
		wg                  sync.WaitGroup
	)
	for range 8 {
		wg.Go(func() {
			for done.Load() < jobs {
				got, err := s.Claim(ctx, driver.ClaimQuery{Queues: []string{"default"}, Limit: 2, Server: "srv"})
				if err != nil {
					t.Error(err)
					return
				}
				n := running.Add(int64(len(got)))
				for {
					p := peak.Load()
					if n <= p || peak.CompareAndSwap(p, n) {
						break
					}
				}
				outs := make([]driver.Outcome, len(got))
				for i, j := range got {
					outs[i] = driver.Outcome{Ref: j.Ref, State: driver.Succeeded}
				}
				running.Add(-int64(len(got)))
				for len(outs) > 0 {
					res, err := s.Finish(ctx, "srv", outs)
					if err != nil {
						t.Error(err)
						return
					}
					var busy []driver.Outcome
					for i, r := range res {
						switch r {
						case driver.Applied:
							done.Add(1)
						case driver.Busy:
							busy = append(busy, outs[i])
						}
					}
					outs = busy
				}
			}
		})
	}
	wg.Wait()
	if p := peak.Load(); p > max {
		t.Fatalf("peak running %d exceeds %d", p, max)
	}
	c, err := s.Counts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if c.Succeeded != jobs || c.Throttled != 0 || c.Enqueued != 0 {
		t.Fatalf("counts %+v", c)
	}
}

func rated(key string, rate int, per time.Duration) func(*driver.InsertParams) {
	return func(p *driver.InsertParams) {
		p.LimitKey, p.LimitRate, p.LimitPer, p.LimitBurst = key, rate, per, 1
	}
}

func into(queue string) func(*driver.InsertParams) {
	return func(p *driver.InsertParams) { p.Queue = queue }
}

func TestRateNotifies(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	events := make(chan driver.Event, 64)
	sub, stop := context.WithCancel(ctx)
	defer stop()
	go s.Subscribe(sub, func(e driver.Event) {
		select {
		case events <- e:
		default:
		}
	})
	await := func(kind driver.EventKind, queue string, within time.Duration) bool {
		t.Helper()
		timeout := time.After(within)
		for {
			select {
			case e := <-events:
				if e.Kind == kind && e.Queue == queue {
					return true
				}
			case <-timeout:
				return false
			}
		}
	}
	ready := func(queue string) {
		t.Helper()
		if !await(driver.JobsReady, queue, 3*time.Second) {
			t.Fatalf("no jobs ready event for %s", queue)
		}
	}
	if !await(driver.Resync, "", 3*time.Second) {
		t.Fatal("not subscribed")
	}

	slow := rated("k", 1, time.Minute)
	first := insert(t, s, job("a", slow, into("first")))[0].ID
	ready("first")
	reserved := insert(t, s, job("a", slow, into("reserved")))[0].ID
	ready("reserved")

	finish(t, s, driver.Outcome{Ref: claim(t, s, 1, "first")[0].Ref, State: driver.Scheduled, Reason: "retry"})
	ready("first")

	delayed := job("a", slow, into("delayed"))
	delayed.Delay = 20 * time.Millisecond
	late := insert(t, s, delayed)[0].ID
	time.Sleep(30 * time.Millisecond)
	if p, err := s.Promote(ctx, 10); err != nil || p.Count != 1 {
		t.Fatalf("promote %+v, %v", p, err)
	}
	ready("delayed")

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	w := s.Tx(tx)
	var ids []int64
	for _, rate := range []int{2, 3} {
		res, err := w.Insert(ctx, []driver.InsertParams{job("a", rated("k", rate, time.Minute), into("tx"))})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, res[0].ID)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if await(driver.JobsReady, "tx", 200*time.Millisecond) {
		t.Fatal("jobs ready for tx before Notify")
	}
	if err := w.Notify(ctx); err != nil {
		t.Fatal(err)
	}
	ready("tx")

	slots := make([]time.Time, 0, 5)
	for _, id := range append([]int64{reserved, first, late}, ids...) {
		r := record(t, s, id)
		if r.State != driver.Scheduled {
			t.Fatalf("job %d is %s, want scheduled in a reserved slot", id, r.State)
		}
		slots = append(slots, r.RunAt)
	}
	for i, gap := range []time.Duration{time.Minute, time.Minute, time.Minute, 20 * time.Second} {
		if d := slots[i+1].Sub(slots[i]); d != gap {
			t.Fatalf("slot %d is %v after the previous one, want %v (slots %v)", i+1, d, gap, slots)
		}
	}
}

func TestPromoteAdmitsThrottledGrant(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := s.pool.Exec(ctx, strings.ReplaceAll(sql, "{s}", s.schema), args...); err != nil {
			t.Fatal(err)
		}
	}
	reserved := func() (tat time.Time, granted int) {
		t.Helper()
		q := strings.ReplaceAll(`SELECT tat, (SELECT count(*) FROM {s}.jobs WHERE granted) FROM {s}.limits WHERE key = 'k'`, "{s}", s.schema)
		if err := s.pool.QueryRow(ctx, q).Scan(&tat, &granted); err != nil {
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
	slow := rated("k", 1, time.Minute)
	res := insert(t, s, job("a", slow), job("a", slow, into("late")), job("a", slow))
	running, due, later := res[0].ID, res[1].ID, res[2].ID
	wantState(running, driver.Enqueued)
	wantState(due, driver.Scheduled)
	wantState(later, driver.Scheduled)
	slot := record(t, s, later).RunAt
	tat, granted := reserved()
	if granted != 2 {
		t.Fatalf("%d granted jobs, want both reservations", granted)
	}

	exec(`UPDATE {s}.jobs SET run_at = now() WHERE id = $1`, due)
	exec(`UPDATE {s}.jobs SET state = 'throttled' WHERE state = 'scheduled' AND run_at <= now()`)
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

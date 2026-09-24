package memstore_test

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

func page(t *testing.T, s driver.Store, q driver.JobQuery) []int64 {
	t.Helper()
	var out []int64
	for {
		p, err := s.Jobs(ctx, q)
		if err != nil {
			t.Fatal(err)
		}
		if len(p.Records) > q.Limit {
			t.Fatalf("page of %d records with limit %d", len(p.Records), q.Limit)
		}
		for _, r := range p.Records {
			if r.State != q.State {
				t.Fatalf("record %d is %s in a %s page", r.ID, r.State, q.State)
			}
			out = append(out, r.ID)
		}
		if p.Next == "" {
			return out
		}
		q.Cursor = p.Next
	}
}

func TestJobsOrder(t *testing.T) {
	t.Parallel()
	s, c := open(t)
	var ps []driver.InsertParams
	for i := range 45 {
		ps = append(ps, params("a", priority(int16(i%3))))
	}
	got := ids(t, s, ps...)
	var want []int64
	for p := 2; p >= 0; p-- {
		for i, id := range got {
			if i%3 == p {
				want = append(want, id)
			}
		}
	}
	if order := page(t, s, driver.JobQuery{State: driver.Enqueued, Limit: 20}); !slices.Equal(order, want) {
		t.Fatalf("enqueued order = %v, want %v", order, want)
	}

	sched := ids(t, s, params("s", delay(3*time.Minute)), params("s", delay(time.Minute)), params("s", delay(time.Minute)))
	if order := page(t, s, driver.JobQuery{State: driver.Scheduled, Limit: 2}); !slices.Equal(order, []int64{sched[1], sched[2], sched[0]}) {
		t.Fatalf("scheduled order = %v", order)
	}

	var finished []int64
	for range 3 {
		j := claimOne(t, s)
		finish(t, s, done(j, driver.Failed))
		finished = append(finished, j.ID)
		c.add(time.Second)
	}
	slices.Reverse(finished)
	if order := page(t, s, driver.JobQuery{State: driver.Failed, Limit: 1}); !slices.Equal(order, finished) {
		t.Fatalf("failed order = %v, want %v", order, finished)
	}
	if order := page(t, s, driver.JobQuery{State: driver.Enqueued, Kind: "none", Limit: 5}); len(order) != 0 {
		t.Fatalf("kind filter = %v", order)
	}
	if _, err := s.Jobs(ctx, driver.JobQuery{}); !errors.Is(err, driver.ErrInvalid) {
		t.Fatalf("missing state: %v", err)
	}
	if _, err := s.Jobs(ctx, driver.JobQuery{State: driver.Enqueued, Cursor: "x"}); !errors.Is(err, driver.ErrInvalid) {
		t.Fatalf("bad cursor: %v", err)
	}
}

func TestCountsAndSeries(t *testing.T) {
	t.Parallel()
	s, c := open(t)
	insert(t, s, params("a"), params("a"), params("a"), params("a"), params("r"), params("d"))
	for _, j := range claim(t, s, 6) {
		switch j.Kind {
		case "r":
			finish(t, s, driver.Outcome{Ref: j.Ref, State: driver.Scheduled, Delay: time.Hour})
		case "d":
			finish(t, s, done(j, driver.Failed))
		default:
			finish(t, s, done(j, driver.Succeeded))
		}
	}
	c.add(time.Minute + 5*time.Second)
	insert(t, s, params("b"), params("b", delay(time.Hour)))
	finish(t, s, done(claimOne(t, s), driver.Succeeded))

	cs, err := s.Counts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if want := (driver.Counts{Scheduled: 2, Failed: 1, Retries: 1, Succeeded: 5}); cs != want {
		t.Fatalf("counts = %+v, want %+v", cs, want)
	}

	pts, err := s.Series(ctx, epoch.Add(-time.Hour), c.now(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	want := []driver.Point{
		{At: epoch, Succeeded: 4, Failed: 1, Retried: 1},
		{At: epoch.Add(time.Minute), Succeeded: 1},
	}
	if !slices.Equal(pts, want) {
		t.Fatalf("series = %+v", pts)
	}
	pts, _ = s.Series(ctx, epoch, c.now(), 2*time.Minute)
	if len(pts) != 1 || pts[0].Succeeded != 5 || !pts[0].At.Equal(epoch) {
		t.Fatalf("2m series = %+v", pts)
	}
	if _, err := s.Series(ctx, epoch, c.now(), 30*time.Second); !errors.Is(err, driver.ErrInvalid) {
		t.Fatalf("sub-minute step: %v", err)
	}
}

func TestQueues(t *testing.T) {
	t.Parallel()
	s, c := open(t)
	insert(t, s, params("a"), params("a"), params("a", queue("later"), delay(time.Hour)))
	claim(t, s, 1)
	must(t, s.PauseQueue(ctx, "paused", true))
	beat(t, s, driver.ServerInfo{ID: "s", Queues: []string{"served"}})
	c.add(10 * time.Second)
	qs, err := s.Queues(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := []driver.QueueInfo{
		{Name: "default", Enqueued: 1, Processing: 1, Latency: 10 * time.Second},
		{Name: "later", Scheduled: 1},
		{Name: "paused", Paused: true},
		{Name: "served"},
	}
	if !slices.Equal(qs, want) {
		t.Fatalf("queues = %+v", qs)
	}
}

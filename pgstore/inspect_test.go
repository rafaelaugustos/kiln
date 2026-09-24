package pgstore

import (
	"context"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

func TestJobsPagination(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	var ps []driver.InsertParams
	for i := 0; i < 7; i++ {
		prio := int16(i % 3)
		ps = append(ps, job("a", func(p *driver.InsertParams) { p.Priority = prio }))
		ps = append(ps, job("a", func(p *driver.InsertParams) { p.Delay = time.Duration(10-i) * time.Hour }))
	}
	insert(t, s, ps...)
	for _, j := range claim(t, s, 3) {
		finish(t, s, driver.Outcome{Ref: j.Ref, State: driver.Succeeded})
	}
	cases := []struct {
		state driver.State
		n     int
		less  func(a, b driver.Record) bool
	}{
		{driver.Enqueued, 4, func(a, b driver.Record) bool {
			return a.Priority > b.Priority || a.Priority == b.Priority && a.ID < b.ID
		}},
		{driver.Scheduled, 7, func(a, b driver.Record) bool {
			return a.RunAt.Before(b.RunAt) || a.RunAt.Equal(b.RunAt) && a.ID < b.ID
		}},
		{driver.Succeeded, 3, func(a, b driver.Record) bool {
			return a.FinalizedAt.After(b.FinalizedAt) || a.FinalizedAt.Equal(b.FinalizedAt) && a.ID > b.ID
		}},
	}
	for _, c := range cases {
		var (
			all    []driver.Record
			cursor string
			pages  int
		)
		for {
			p, err := s.Jobs(ctx, driver.JobQuery{State: c.state, Limit: 2, Cursor: cursor})
			if err != nil {
				t.Fatal(err)
			}
			pages++
			all = append(all, p.Records...)
			if p.Next == "" {
				break
			}
			cursor = p.Next
		}
		if len(all) != c.n || pages != (c.n+1)/2 {
			t.Fatalf("%s: %d records in %d pages", c.state, len(all), pages)
		}
		for i := 1; i < len(all); i++ {
			if !c.less(all[i-1], all[i]) {
				t.Fatalf("%s: order broken at %d: %+v then %+v", c.state, i, all[i-1], all[i])
			}
		}
	}
	queues, err := s.Queues(ctx)
	if err != nil || len(queues) != 1 || queues[0].Enqueued != 4 || queues[0].Scheduled != 7 || queues[0].Latency < 0 {
		t.Fatalf("queues %+v %v", queues, err)
	}
	now, _ := s.Now(ctx)
	pts, err := s.Series(ctx, now.Add(-time.Hour), now.Add(time.Minute), 5*time.Minute)
	if err != nil || len(pts) != 1 || pts[0].Succeeded != 3 || pts[0].At.Unix()%300 != 0 {
		t.Fatalf("series %+v %v", pts, err)
	}
	if _, err := s.Series(ctx, now, now, 90*time.Second); err == nil {
		t.Fatal("series accepted a non-minute step")
	}
}

func TestPruneAndSweep(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	res := insert(t, s, job("a", unique("p", 0)), job("a"), job("a", limited("k", 1)))
	for _, j := range claim(t, s, 10) {
		st := driver.Succeeded
		if j.ID == res[1].ID {
			st = driver.Failed
		}
		finish(t, s, driver.Outcome{Ref: j.Ref, State: st, Reason: "x"})
	}
	if _, err := s.pool.Exec(ctx, "UPDATE "+s.schema+".limits SET active = 5"); err != nil {
		t.Fatal(err)
	}
	insert(t, s, job("a", limited("k", 1)))
	if n, err := s.Sweep(ctx, 100); err != nil || n == 0 {
		t.Fatalf("sweep %d %v", n, err)
	}
	if got := claim(t, s, 10); len(got) != 1 {
		t.Fatalf("reconciled limit admitted %d", len(got))
	}
	n, err := s.Prune(ctx, driver.PruneParams{Retention: driver.Retention{Failed: -1}})
	if err != nil {
		t.Fatal(err)
	}
	if n < 2 {
		t.Fatalf("pruned %d", n)
	}
	if _, err := s.Job(ctx, res[0].ID); err == nil {
		t.Fatal("succeeded job survived zero retention")
	}
	if r := record(t, s, res[1].ID); r.State != driver.Failed {
		t.Fatalf("failed job pruned: %s", r.State)
	}
	c, err := s.Counts(ctx)
	if err != nil || c.Succeeded != 2 {
		t.Fatalf("totals lost after prune: %+v %v", c, err)
	}
}

func TestBulkAdmin(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	ps := make([]driver.InsertParams, 2500)
	for i := range ps {
		ps[i] = job("bulk")
	}
	insert(t, s, ps...)
	n, err := s.Delete(ctx, driver.Filter{State: driver.Enqueued, Kind: "bulk"})
	if err != nil || n != 2500 {
		t.Fatalf("delete %d %v", n, err)
	}
	n, err = s.Requeue(ctx, driver.Filter{State: driver.Deleted})
	if err != nil || n != 2500 {
		t.Fatalf("requeue %d %v", n, err)
	}
	c, err := s.Counts(ctx)
	if err != nil || c.Enqueued != 2500 || c.Deleted != 2500 {
		t.Fatalf("counts %+v %v", c, err)
	}
	if _, err := s.Delete(ctx, driver.Filter{Queue: "default"}); err == nil {
		t.Fatal("unbounded delete accepted")
	}
}

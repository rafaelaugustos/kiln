package memstore_test

import (
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

func rated(key string, n int, per time.Duration, burst int) opt {
	return func(p *driver.InsertParams) {
		p.LimitKey, p.LimitRate, p.LimitPer, p.LimitBurst = key, n, per, burst
	}
}

type slot struct {
	st driver.State
	at time.Duration
}

func expectSlots(t *testing.T, s driver.Store, ids []int64, want ...slot) {
	t.Helper()
	for i, id := range ids {
		if r := record(t, s, id); r.State != want[i].st || !r.RunAt.Equal(epoch.Add(want[i].at)) {
			t.Fatalf("job %d is %s at %v, want %s at %v", id, r.State, r.RunAt.Sub(epoch), want[i].st, want[i].at)
		}
	}
}

func TestRateSlots(t *testing.T) {
	t.Parallel()
	s, c := open(t)
	const ms = time.Millisecond
	enq, sch := driver.Enqueued, driver.Scheduled
	job := params("a", rated("k", 4, time.Second, 2))

	got := ids(t, s, job, job, job, job, job, job)
	expectSlots(t, s, got, slot{enq, 0}, slot{enq, 0}, slot{sch, 250 * ms}, slot{sch, 500 * ms}, slot{sch, 750 * ms}, slot{sch, time.Second})

	c.add(time.Second)
	if p, err := s.Promote(ctx, 100); err != nil || p.Count != 4 || p.Next != 0 {
		t.Fatalf("promote = %+v, %v", p, err)
	}
	expectSlots(t, s, got[2:], slot{enq, 250 * ms}, slot{enq, 500 * ms}, slot{enq, 750 * ms}, slot{enq, time.Second})
	expectSlots(t, s, ids(t, s, job, job), slot{sch, 1250 * ms}, slot{sch, 1500 * ms})

	c.add(10 * time.Second)
	expectSlots(t, s, ids(t, s, job, job, job), slot{enq, 11 * time.Second}, slot{enq, 11 * time.Second}, slot{sch, 11250 * ms})
}

func TestRateBound(t *testing.T) {
	t.Parallel()
	s, _ := open(t)
	ps := make([]driver.InsertParams, 1500)
	for i := range ps {
		ps[i] = params("a", rated("k", 10, time.Second, 1))
	}
	got := ids(t, s, ps...)
	if cs, _ := s.Counts(ctx); cs.Enqueued != 1 || cs.Scheduled != 999 || cs.Throttled != 500 {
		t.Fatalf("after insert %+v, want 1000 jobs through one admission", cs)
	}
	if n, err := s.Sweep(ctx, 100); err != nil || n != 500 {
		t.Fatalf("sweep = %d, %v, want the other 500 reserved", n, err)
	}
	for _, i := range []int{999, 1000, 1499} {
		expectSlots(t, s, got[i:i+1], slot{driver.Scheduled, time.Duration(i) * 100 * time.Millisecond})
	}

	for i := range ps {
		ps[i] = params("b", limited("m", 2000))
	}
	insert(t, s, ps...)
	if cs, _ := s.Counts(ctx); cs.Enqueued != 1501 || cs.Throttled != 0 {
		t.Fatalf("after insert %+v, want every job under a plain max admitted at once", cs)
	}
}

package pgstore

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/rafaelaugustos/kiln/driver"
)

func TestLifecycle(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	res := insert(t, s,
		job("a", func(p *driver.InsertParams) {
			p.Priority = 1
			p.Tags = []string{"x", `q"t`}
			p.Meta = map[string]string{"k": "v"}
		}),
		job("b", func(p *driver.InsertParams) { p.Delay = time.Hour }),
		job("c"),
	)
	if res[0].State != driver.Enqueued || res[1].State != driver.Scheduled || res[2].State != driver.Enqueued {
		t.Fatalf("states %+v", res)
	}
	if !(res[0].ID < res[1].ID && res[1].ID < res[2].ID) {
		t.Fatalf("ids not increasing: %+v", res)
	}
	jobs := claim(t, s, 10)
	if len(jobs) != 2 || jobs[0].ID != res[0].ID || jobs[1].ID != res[2].ID {
		t.Fatalf("claimed %+v", jobs)
	}
	j := jobs[0]
	if j.Attempt != 1 || j.Claim != 1 || j.Meta["k"] != "v" || len(j.Tags) != 2 || j.Tags[1] != `q"t` || j.AttemptedAt.IsZero() {
		t.Fatalf("job %+v", j)
	}
	got := finish(t, s,
		driver.Outcome{Ref: j.Ref, State: driver.Succeeded, Output: []byte(`{"ok":true}`)},
		driver.Outcome{Ref: jobs[1].Ref, State: driver.Scheduled, Delay: time.Hour, Reason: "retry", Error: "boom"},
	)
	if got[0] != driver.Applied || got[1] != driver.Applied {
		t.Fatalf("results %v", got)
	}
	again := finish(t, s, driver.Outcome{Ref: j.Ref, State: driver.Succeeded})
	if again[0] != driver.Stale {
		t.Fatalf("resend: %v", again)
	}
	r := record(t, s, j.ID)
	if r.State != driver.Succeeded || string(r.Output) != `{"ok":true}` || r.FinalizedAt.IsZero() || r.Server != "srv" {
		t.Fatalf("record %+v", r)
	}
	r = record(t, s, jobs[1].ID)
	if r.State != driver.Scheduled || len(r.History) != 1 || r.History[0].Reason != "retry" || r.History[0].Error != "boom" {
		t.Fatalf("retry record %+v", r)
	}
	if d := time.Until(r.RunAt); d < 50*time.Minute {
		t.Fatalf("run at %v", r.RunAt)
	}
	c, err := s.Counts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if c.Scheduled != 2 || c.Retries != 1 || c.Succeeded != 1 || c.Enqueued != 0 {
		t.Fatalf("counts %+v", c)
	}
	if _, err := s.Job(ctx, 1<<40); !errors.Is(err, driver.ErrNotFound) {
		t.Fatalf("missing job: %v", err)
	}
}

func TestFinishOutcomes(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	ids := insert(t, s, job("a"), job("a"), job("a"), job("a"), job("a"), job("a"))
	jobs := claim(t, s, 10)
	if len(jobs) != 6 {
		t.Fatalf("claimed %d", len(jobs))
	}
	if _, err := s.Delete(ctx, driver.Filter{IDs: []int64{jobs[2].ID}}); err != nil {
		t.Fatal(err)
	}
	res := finish(t, s,
		driver.Outcome{Ref: jobs[0].Ref, State: driver.Failed, Reason: "permanent", Error: "bad"},
		driver.Outcome{Ref: jobs[1].Ref, State: driver.Scheduled, Refund: true, Delay: time.Minute, Reason: "snoozed"},
		driver.Outcome{Ref: jobs[2].Ref, State: driver.Scheduled, Delay: time.Minute, Reason: "retry"},
		driver.Outcome{Ref: jobs[3].Ref, State: driver.Enqueued, Refund: true, Reason: "shutdown"},
		driver.Outcome{Ref: jobs[4].Ref, State: driver.Scheduled, Reason: "retry"},
		driver.Outcome{ID: jobs[5].ID, Claim: 99, State: driver.Succeeded},
		driver.Outcome{Ref: jobs[5].Ref, State: driver.Awaiting},
	)
	want := []driver.Result{driver.Applied, driver.Applied, driver.Applied, driver.Applied, driver.Applied, driver.Stale, driver.Rejected}
	for i := range want {
		if res[i] != want[i] {
			t.Fatalf("result %d = %v, want %v (%v)", i, res[i], want[i], res)
		}
	}
	cases := []struct {
		id      int64
		state   driver.State
		attempt int
		reason  string
	}{
		{ids[0].ID, driver.Failed, 1, "permanent"},
		{ids[1].ID, driver.Scheduled, 0, "snoozed"},
		{ids[2].ID, driver.Deleted, 1, "canceled"},
		{ids[3].ID, driver.Enqueued, 0, "shutdown"},
		{ids[4].ID, driver.Enqueued, 1, "retry"},
	}
	for _, c := range cases {
		r := record(t, s, c.id)
		if r.State != c.state || r.Attempt != c.attempt || len(r.History) == 0 || r.History[len(r.History)-1].Reason != c.reason {
			t.Errorf("job %d: state %s attempt %d history %+v, want %s %d %s", c.id, r.State, r.Attempt, r.History, c.state, c.attempt, c.reason)
		}
	}
	if err := s.SetMeta(ctx, jobs[0].Ref, map[string]string{"a": "b"}); !errors.Is(err, driver.ErrLost) {
		t.Fatalf("set meta on finished job: %v", err)
	}
	if err := s.SetMeta(ctx, jobs[5].Ref, map[string]string{"a": "b"}); err != nil {
		t.Fatalf("set meta: %v", err)
	}
	if r := record(t, s, jobs[5].ID); r.Meta["a"] != "b" {
		t.Fatalf("meta %v", r.Meta)
	}
}

func TestFinishRejectsBadOutput(t *testing.T) {
	t.Parallel()
	s := open(t)
	insert(t, s, job("a"), job("a"), job("a"))
	jobs := claim(t, s, 3)
	res := finish(t, s,
		driver.Outcome{Ref: jobs[0].Ref, State: driver.Succeeded, Output: []byte(`{"ok":1}`)},
		driver.Outcome{Ref: jobs[1].Ref, State: driver.Succeeded, Output: []byte(`{bad`)},
		driver.Outcome{Ref: jobs[2].Ref, State: driver.Failed, Error: "nul\x00byte", Reason: "exhausted"},
	)
	if res[0] != driver.Applied || res[1] != driver.Rejected || res[2] != driver.Applied {
		t.Fatalf("results %v", res)
	}
	if r := record(t, s, jobs[2].ID); r.History[0].Error != "nulbyte" {
		t.Fatalf("nul not stripped: %q", r.History[0].Error)
	}
	if r := record(t, s, jobs[1].ID); r.State != driver.Processing {
		t.Fatalf("rejected job state %s", r.State)
	}
}

func TestFinishBusy(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	insert(t, s, job("a"), job("a"))
	jobs := claim(t, s, 2)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, "SELECT 1 FROM "+s.schema+".jobs WHERE id = $1 FOR UPDATE", jobs[0].ID); err != nil {
		t.Fatal(err)
	}
	res := finish(t, s,
		driver.Outcome{Ref: jobs[0].Ref, State: driver.Succeeded},
		driver.Outcome{Ref: jobs[1].Ref, State: driver.Succeeded},
	)
	if res[0] != driver.Busy || res[1] != driver.Applied {
		t.Fatalf("results %v", res)
	}
	tx.Rollback(ctx)
	if res := finish(t, s, driver.Outcome{Ref: jobs[0].Ref, State: driver.Succeeded}); res[0] != driver.Applied {
		t.Fatalf("after unlock %v", res)
	}
}

func TestFinishMixedClaims(t *testing.T) {
	t.Parallel()
	s := open(t)
	insert(t, s, job("a"))
	j := claim(t, s, 1)[0]
	res := finish(t, s,
		driver.Outcome{ID: j.ID, Claim: j.Claim + 1, State: driver.Succeeded},
		driver.Outcome{Ref: j.Ref, State: driver.Failed, Reason: "permanent"},
		driver.Outcome{Ref: j.Ref, State: driver.Succeeded},
	)
	if res[0] != driver.Stale || res[1] != driver.Applied || res[2] != driver.Stale {
		t.Fatalf("results %v", res)
	}
	if r := record(t, s, j.ID); r.State != driver.Failed {
		t.Fatalf("state %s, want the first matching outcome", r.State)
	}
}

func TestHistoryCap(t *testing.T) {
	t.Parallel()
	s := open(t)
	id := insert(t, s, job("a", func(p *driver.InsertParams) { p.MaxAttempts = 100 }))[0].ID
	long := strings.Repeat("é", 10<<10)
	for range 20 {
		j := claim(t, s, 1)[0]
		finish(t, s, driver.Outcome{Ref: j.Ref, State: driver.Scheduled, Reason: "retry", Error: long, Trace: long})
	}
	r := record(t, s, id)
	if len(r.History) != 16 || r.History[15].Attempt != 20 || r.History[0].Attempt != 5 {
		t.Fatalf("history len %d first %+v", len(r.History), r.History[0].Attempt)
	}
	e := r.History[15]
	if len(e.Error) > 2<<10 || len(e.Trace) > 8<<10 || !utf8.ValidString(e.Error) || !utf8.ValidString(e.Trace) {
		t.Fatalf("entry sizes error %d trace %d", len(e.Error), len(e.Trace))
	}
	c, err := s.Counts(context.Background())
	if err != nil || c.Retries != 0 || c.Enqueued != 1 {
		t.Fatalf("counts %+v %v", c, err)
	}
	pts, err := s.Series(context.Background(), time.Now().Add(-time.Hour), time.Now().Add(time.Hour), time.Minute)
	if err != nil || len(pts) == 0 || pts[len(pts)-1].Retried == 0 {
		t.Fatalf("series %+v %v", pts, err)
	}
}

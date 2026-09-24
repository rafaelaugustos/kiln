package drivertest

import (
	"fmt"
	"maps"
	"strings"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

var finishTests = []test{
	{"Succeeded", testFinishSucceeded},
	{"Deleted", testFinishDeleted},
	{"Failed", testFinishFailed},
	{"Stale", testFinishStale},
	{"Resend", testFinishResend},
	{"Duplicate", testFinishDuplicate},
	{"Retry", testFinishRetry},
	{"RetryNow", testFinishRetryNow},
	{"Refund", testFinishRefund},
	{"Cancel", testFinishCancel},
	{"History", testFinishHistory},
	{"Truncate", testFinishTruncate},
	{"Rejected", testFinishRejected},
	{"SetMeta", testSetMeta},
}

func testFinishSucceeded(t *testing.T, s driver.Store) {
	j := start(t, s, task("q"))
	out := outcome(j, driver.Succeeded)
	out.Output = []byte(`{"ok":true,"n":[1,2]}`)
	n0 := now(t, s)
	apply(t, s, out)
	n1 := now(t, s)

	r := record(t, s, j.ID)
	if r.State != driver.Succeeded || r.Attempt != 1 {
		t.Fatalf("state %s attempt %d, want succeeded after attempt 1", r.State, r.Attempt)
	}
	within(t, "finalized at", r.FinalizedAt, n0, n1)
	if !sameJSON(r.Output, out.Output) {
		t.Fatalf("output %s, want %s", r.Output, out.Output)
	}
	if len(r.History) != 0 {
		t.Fatalf("history %+v, want none for succeeded without reason", r.History)
	}
	if js := claim(t, s, 10, "q"); len(js) != 0 {
		t.Fatalf("claimed %v after success", jobIDs(js))
	}
}

func testFinishDeleted(t *testing.T, s driver.Store) {
	j := start(t, s, task("q"))
	out := outcome(j, driver.Deleted)
	out.Reason, out.Error = "canceled", "stop"
	n0 := now(t, s)
	apply(t, s, out)
	n1 := now(t, s)

	r := record(t, s, j.ID)
	if r.State != driver.Deleted {
		t.Fatalf("state %s, want deleted", r.State)
	}
	within(t, "finalized at", r.FinalizedAt, n0, n1)
	if e := lastEntry(t, r); e.Reason != "canceled" || e.Error != "stop" {
		t.Fatalf("history entry %+v", e)
	}
}

func testFinishFailed(t *testing.T, s driver.Store) {
	j := start(t, s, task("q"))
	out := outcome(j, driver.Failed)
	out.Reason, out.Error, out.Trace = "exhausted", "boom", "main.go:1"
	n0 := now(t, s)
	apply(t, s, out)
	n1 := now(t, s)

	r := record(t, s, j.ID)
	if r.State != driver.Failed || r.Attempt != 1 {
		t.Fatalf("state %s attempt %d, want failed after attempt 1", r.State, r.Attempt)
	}
	within(t, "finalized at", r.FinalizedAt, n0, n1)
	e := lastEntry(t, r)
	if e.State != driver.Failed || e.Reason != "exhausted" || e.Error != "boom" || e.Trace != "main.go:1" || e.Attempt != 1 || e.Server != server {
		t.Fatalf("history entry %+v", e)
	}
	within(t, "history at", e.At, n0, n1)
	if js := claim(t, s, 10, "q"); len(js) != 0 {
		t.Fatalf("claimed %v after failure", jobIDs(js))
	}
}

func testFinishStale(t *testing.T, s driver.Store) {
	j := start(t, s, task("a"))
	idle := add(t, s, task("idle"))
	rs := finish(t, s,
		driver.Outcome{ID: j.ID, Claim: j.Claim + 1, State: driver.Succeeded},
		driver.Outcome{ID: j.ID + 1000, Claim: 1, State: driver.Succeeded},
		outcome(j, driver.Succeeded),
		driver.Outcome{ID: idle, Claim: 1, State: driver.Succeeded},
		driver.Outcome{ID: idle, State: driver.Failed},
	)
	want := []driver.Result{driver.Stale, driver.Stale, driver.Applied, driver.Stale, driver.Stale}
	for i := range want {
		if rs[i] != want[i] {
			t.Fatalf("results %v, want %v", rs, want)
		}
	}
	wantState(t, s, driver.Succeeded, j.ID)
	wantState(t, s, driver.Enqueued, idle)
	if rs := finish(t, s, outcome(j, driver.Failed)); rs[0] != driver.Stale {
		t.Fatalf("finish of finished job: %d, want stale", rs[0])
	}

	old := start(t, s, task("b"))
	retry := outcome(old, driver.Scheduled)
	retry.Reason = "retry"
	apply(t, s, retry)
	cur := claimOne(t, s, "b", old.ID)
	if cur.Claim <= old.Claim {
		t.Fatalf("claim %d after %d, want increasing", cur.Claim, old.Claim)
	}
	if rs := finish(t, s, outcome(old, driver.Succeeded)); rs[0] != driver.Stale {
		t.Fatalf("finish with previous claim: %d, want stale", rs[0])
	}
	wantState(t, s, driver.Processing, cur.ID)
	apply(t, s, outcome(cur, driver.Succeeded))
}

func testFinishDuplicate(t *testing.T, s driver.Store) {
	a := start(t, s, task("a"))
	b := start(t, s, task("b"))
	retry := outcome(b, driver.Scheduled)
	retry.Delay, retry.Reason = time.Hour, "retry"
	failed := outcome(a, driver.Failed)
	failed.Reason = "exhausted"
	rs := finish(t, s, outcome(a, driver.Succeeded), failed, retry, outcome(b, driver.Succeeded), outcome(a, driver.Succeeded))
	want := []driver.Result{driver.Applied, driver.Stale, driver.Applied, driver.Stale, driver.Stale}
	for i := range want {
		if rs[i] != want[i] {
			t.Fatalf("results %v, want %v", rs, want)
		}
	}
	if r := record(t, s, a.ID); r.State != driver.Succeeded || len(r.History) != 0 {
		t.Fatalf("job %d: state %s history %+v, want succeeded without history", a.ID, r.State, r.History)
	}
	if r := record(t, s, b.ID); r.State != driver.Scheduled || len(r.History) != 1 {
		t.Fatalf("job %d: state %s history %+v, want scheduled with one entry", b.ID, r.State, r.History)
	}
	if c := counts(t, s); c.Succeeded != 1 || c.Failed != 0 || c.Retries != 1 {
		t.Fatalf("counts %+v, want one succeeded and one retry", c)
	}
}

func testFinishResend(t *testing.T, s driver.Store) {
	insert(t, s, tasks(3, "q")...)
	js := claimN(t, s, 3, "q")
	outs := []driver.Outcome{outcome(js[0], driver.Succeeded), outcome(js[1], driver.Failed), outcome(js[2], driver.Deleted)}
	apply(t, s, outs...)
	for i, r := range finish(t, s, outs...) {
		if r != driver.Stale {
			t.Fatalf("resent outcome %d: %d, want stale", i, r)
		}
	}
	wantState(t, s, driver.Succeeded, js[0].ID)
	wantState(t, s, driver.Failed, js[1].ID)
	wantState(t, s, driver.Deleted, js[2].ID)
}

func testFinishRetry(t *testing.T, s driver.Store) {
	j := start(t, s, task("q"))
	out := outcome(j, driver.Scheduled)
	out.Delay, out.Reason, out.Error = 100*time.Millisecond, "retry", "boom"
	n0 := now(t, s)
	apply(t, s, out)
	n1 := now(t, s)

	r := record(t, s, j.ID)
	if r.State != driver.Scheduled || r.Attempt != 1 {
		t.Fatalf("state %s attempt %d, want scheduled with attempt 1", r.State, r.Attempt)
	}
	within(t, "run at", r.RunAt, n0.Add(out.Delay), n1.Add(out.Delay))
	if e := lastEntry(t, r); e.Reason != "retry" || e.Error != "boom" || e.Attempt != 1 {
		t.Fatalf("history entry %+v", e)
	}
	if js := claim(t, s, 10, "q"); len(js) != 0 {
		t.Fatalf("claimed %v before retry delay", jobIDs(js))
	}
	eventually(t, time.Second, func() bool {
		promote(t, s, 100)
		return stateOf(t, s, j.ID) == driver.Enqueued
	})
	next := claimOne(t, s, "q", j.ID)
	if next.Attempt != 2 || next.Claim <= j.Claim {
		t.Fatalf("second claim attempt %d claim %d, want attempt 2 and claim > %d", next.Attempt, next.Claim, j.Claim)
	}
}

func testFinishRetryNow(t *testing.T, s driver.Store) {
	for i, d := range []time.Duration{0, -time.Second} {
		j := start(t, s, task(fmt.Sprintf("q%d", i)))
		out := outcome(j, driver.Scheduled)
		out.Delay, out.Reason = d, "retry"
		apply(t, s, out)
		wantState(t, s, driver.Enqueued, j.ID)
		if next := claimOne(t, s, j.Queue, j.ID); next.Attempt != 2 {
			t.Fatalf("attempt %d, want 2", next.Attempt)
		}
	}
	j := start(t, s, limited("limited", "k", 1))
	out := outcome(j, driver.Scheduled)
	out.Reason = "retry"
	apply(t, s, out)
	wantState(t, s, driver.Enqueued, j.ID)
}

func testFinishRefund(t *testing.T, s driver.Store) {
	j := start(t, s, task("q"))
	snooze := outcome(j, driver.Scheduled)
	snooze.Delay, snooze.Refund, snooze.Reason = 50*time.Millisecond, true, "snoozed"
	apply(t, s, snooze)
	if r := record(t, s, j.ID); r.State != driver.Scheduled || r.Attempt != 0 {
		t.Fatalf("after snooze: state %s attempt %d, want scheduled with attempt 0", r.State, r.Attempt)
	}
	eventually(t, time.Second, func() bool {
		promote(t, s, 100)
		return stateOf(t, s, j.ID) == driver.Enqueued
	})
	j2 := claimOne(t, s, "q", j.ID)
	if j2.Attempt != 1 || j2.Claim <= j.Claim {
		t.Fatalf("after snooze: attempt %d claim %d, want attempt 1 and claim > %d", j2.Attempt, j2.Claim, j.Claim)
	}

	requeue := outcome(j2, driver.Enqueued)
	requeue.Refund, requeue.Reason = true, "shutdown"
	apply(t, s, requeue)
	if r := record(t, s, j.ID); r.State != driver.Enqueued || r.Attempt != 0 {
		t.Fatalf("after shutdown: state %s attempt %d, want enqueued with attempt 0", r.State, r.Attempt)
	}
	j3 := claimOne(t, s, "q", j.ID)
	if j3.Attempt != 1 || j3.Claim <= j2.Claim {
		t.Fatalf("after shutdown: attempt %d claim %d, want attempt 1 and claim > %d", j3.Attempt, j3.Claim, j2.Claim)
	}

	lost := outcome(j3, driver.Enqueued)
	lost.Reason = "lost"
	apply(t, s, lost)
	if r := record(t, s, j.ID); r.State != driver.Enqueued || r.Attempt != 1 {
		t.Fatalf("after lost: state %s attempt %d, want enqueued with attempt 1", r.State, r.Attempt)
	}
	if j4 := claimOne(t, s, "q", j.ID); j4.Attempt != 2 {
		t.Fatalf("after lost: attempt %d, want 2", j4.Attempt)
	}
}

func testFinishCancel(t *testing.T, s driver.Store) {
	cases := []struct {
		out  driver.Outcome
		want driver.State
	}{
		{driver.Outcome{State: driver.Scheduled, Delay: time.Hour, Reason: "retry"}, driver.Deleted},
		{driver.Outcome{State: driver.Scheduled, Reason: "retry"}, driver.Deleted},
		{driver.Outcome{State: driver.Scheduled, Delay: time.Hour, Refund: true, Reason: "snoozed"}, driver.Deleted},
		{driver.Outcome{State: driver.Enqueued, Refund: true, Reason: "shutdown"}, driver.Deleted},
		{driver.Outcome{State: driver.Failed, Reason: "exhausted"}, driver.Deleted},
		{driver.Outcome{State: driver.Deleted, Reason: "canceled"}, driver.Deleted},
		{driver.Outcome{State: driver.Succeeded}, driver.Succeeded},
	}
	for i, c := range cases {
		j := start(t, s, task(fmt.Sprintf("q%d", i)))
		if n := deleteIDs(t, s, j.ID); n != 1 {
			t.Fatalf("delete of processing job affected %d, want 1", n)
		}
		if r := record(t, s, j.ID); r.State != driver.Processing || !r.CancelRequested {
			t.Fatalf("after delete: state %s cancel %v, want processing with cancel requested", r.State, r.CancelRequested)
		}
		c.out.Ref = j.Ref
		apply(t, s, c.out)
		if got := stateOf(t, s, j.ID); got != c.want {
			t.Fatalf("%s outcome on canceled job: state %s, want %s", c.out.State, got, c.want)
		}
	}
}

func testFinishHistory(t *testing.T, s driver.Store) {
	p := task("q")
	p.MaxAttempts = 100
	id := add(t, s, p)
	for i := range 20 {
		j := claimOne(t, s, "q", id)
		out := outcome(j, driver.Scheduled)
		out.Reason = fmt.Sprintf("retry %d", i)
		apply(t, s, out)
	}
	r := record(t, s, id)
	if len(r.History) != 16 {
		t.Fatalf("history has %d entries, want 16", len(r.History))
	}
	for i, e := range r.History {
		if want := fmt.Sprintf("retry %d", i+4); e.Reason != want {
			t.Fatalf("history[%d] reason %q, want %q", i, e.Reason, want)
		}
		if e.Attempt != i+5 {
			t.Fatalf("history[%d] attempt %d, want %d", i, e.Attempt, i+5)
		}
		if i > 0 && e.At.Before(r.History[i-1].At) {
			t.Fatalf("history[%d] at %v before previous %v", i, e.At, r.History[i-1].At)
		}
	}
}

func testFinishTruncate(t *testing.T, s driver.Store) {
	j := start(t, s, task("q"))
	msg, trace := strings.Repeat("e", 5000), strings.Repeat("t", 20000)
	out := outcome(j, driver.Failed)
	out.Reason, out.Error, out.Trace = "permanent", msg, trace
	apply(t, s, out)
	e := lastEntry(t, record(t, s, j.ID))
	if e.Error != msg[:2<<10] {
		t.Fatalf("error has %d bytes, want the first %d", len(e.Error), 2<<10)
	}
	if e.Trace != trace[:8<<10] {
		t.Fatalf("trace has %d bytes, want the first %d", len(e.Trace), 8<<10)
	}
}

func testFinishRejected(t *testing.T, s driver.Store) {
	a := start(t, s, task("a"))
	b := start(t, s, task("b"))
	bad := outcome(a, driver.Succeeded)
	bad.Output = []byte(`{"broken":`)
	rs := finish(t, s, bad, outcome(b, driver.Succeeded))
	if rs[0] != driver.Rejected || rs[1] != driver.Applied {
		t.Fatalf("results %v, want [rejected applied]", rs)
	}
	wantState(t, s, driver.Processing, a.ID)
	wantState(t, s, driver.Succeeded, b.ID)
	failed := outcome(a, driver.Failed)
	failed.Reason, failed.Error = "permanent", "kiln: outcome rejected"
	apply(t, s, failed)
	wantState(t, s, driver.Failed, a.ID)
}

func testSetMeta(t *testing.T, s driver.Store) {
	ctx := t.Context()
	p := task("q")
	p.Meta = map[string]string{"a": "1", "b": "2"}
	j := start(t, s, p)
	if err := s.SetMeta(ctx, j.Ref, map[string]string{"b": "3", "c": "4"}); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"a": "1", "b": "3", "c": "4"}
	if got := record(t, s, j.ID).Meta; !maps.Equal(got, want) {
		t.Fatalf("meta %v, want %v", got, want)
	}

	bare := start(t, s, task("bare"))
	if err := s.SetMeta(ctx, bare.Ref, map[string]string{"k": "v"}); err != nil {
		t.Fatal(err)
	}
	if got := record(t, s, bare.ID).Meta; !maps.Equal(got, map[string]string{"k": "v"}) {
		t.Fatalf("meta %v, want k=v", got)
	}

	idle := add(t, s, task("idle"))
	for _, ref := range []driver.Ref{
		{ID: j.ID, Claim: j.Claim + 1},
		{ID: j.ID + 1000, Claim: 1},
		{ID: idle},
		{ID: idle, Claim: 1},
	} {
		wantErr(t, s.SetMeta(ctx, ref, map[string]string{"x": "y"}), driver.ErrLost)
	}
	apply(t, s, outcome(j, driver.Succeeded))
	wantErr(t, s.SetMeta(ctx, j.Ref, map[string]string{"x": "y"}), driver.ErrLost)
	if got := record(t, s, j.ID).Meta; !maps.Equal(got, want) {
		t.Fatalf("meta after lost update %v, want %v", got, want)
	}
}

package drivertest

import (
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

var insertTests = []test{
	{"IDs", testInsertIDs},
	{"States", testInsertStates},
	{"LimitState", testInsertLimitState},
	{"Fields", testInsertFields},
	{"RunAt", testInsertRunAt},
	{"Invalid", testInsertInvalid},
}

func testInsertIDs(t *testing.T, s driver.Store) {
	seen := make(map[int64]bool)
	for range 3 {
		ins := insert(t, s, tasks(50, "default")...)
		for i, in := range ins {
			if in.ID <= 0 {
				t.Fatalf("result %d: id %d, want > 0", i, in.ID)
			}
			if i > 0 && in.ID <= ins[i-1].ID {
				t.Fatalf("result %d: id %d after %d, want strictly increasing", i, in.ID, ins[i-1].ID)
			}
			if seen[in.ID] {
				t.Fatalf("id %d returned twice", in.ID)
			}
			seen[in.ID] = true
			if in.Duplicate || in.State != driver.Enqueued {
				t.Fatalf("result %d = %+v, want enqueued and not duplicate", i, in)
			}
		}
	}
	if c := counts(t, s); c.Enqueued != 150 {
		t.Fatalf("enqueued = %d, want 150", c.Enqueued)
	}
}

func testInsertStates(t *testing.T, s driver.Store) {
	pending := add(t, s, task("parents"))
	dead := start(t, s, task("dead"))
	apply(t, s, outcome(dead, driver.Deleted))
	n := now(t, s)

	delay := func(p driver.InsertParams) driver.InsertParams {
		p.Delay = time.Hour
		return p
	}
	runAt := func(when time.Time) driver.InsertParams {
		p := task("q")
		p.RunAt = when
		return p
	}
	limit := func(p driver.InsertParams) driver.InsertParams {
		p.LimitKey, p.LimitMax = "k", 1
		return p
	}
	cases := []struct {
		name string
		p    driver.InsertParams
		want driver.State
	}{
		{"plain", task("q"), driver.Enqueued},
		{"delay", delay(task("q")), driver.Scheduled},
		{"future run at", runAt(n.Add(time.Hour)), driver.Scheduled},
		{"past run at", runAt(n.Add(-time.Hour)), driver.Enqueued},
		{"pending parent", after("q", driver.OnSucceeded, pending), driver.Awaiting},
		{"pending parent with delay", delay(after("q", driver.OnSucceeded, pending)), driver.Awaiting},
		{"pending parent with limit", limit(after("q", driver.OnSucceeded, pending)), driver.Awaiting},
		{"doomed parent", after("q", driver.OnSucceeded, dead.ID), driver.Deleted},
		{"doomed and pending parents", after("q", driver.OnSucceeded, pending, dead.ID), driver.Deleted},
		{"resolved parent", after("q", driver.OnDeleted, dead.ID), driver.Enqueued},
		{"resolved parent with delay", delay(after("q", driver.OnFinished, dead.ID)), driver.Scheduled},
	}
	for _, c := range cases {
		in := insert(t, s, c.p)[0]
		if in.State != c.want || in.Duplicate {
			t.Errorf("%s: inserted %+v, want state %s", c.name, in, c.want)
			continue
		}
		if got := stateOf(t, s, in.ID); got != c.want {
			t.Errorf("%s: stored state %s, want %s", c.name, got, c.want)
		}
	}
}

func testInsertLimitState(t *testing.T, s driver.Store) {
	ins := insert(t, s, limited("q", "k", 1), limited("q", "k", 1))
	if st := ins[0].State; st != driver.Throttled && st != driver.Enqueued {
		t.Fatalf("admitted job inserted as %s, want throttled or enqueued", st)
	}
	if st := ins[1].State; st != driver.Throttled {
		t.Fatalf("waiting job inserted as %s, want throttled", st)
	}
	wantState(t, s, driver.Enqueued, ins[0].ID)
	wantState(t, s, driver.Throttled, ins[1].ID)
}

func testInsertFields(t *testing.T, s driver.Store) {
	p := driver.InsertParams{
		Kind:        "email.send",
		Queue:       "mail",
		Args:        []byte(`{"to":"a@example.com","n":3,"tags":["x","y"]}`),
		Meta:        map[string]string{"trace": "abc", "tenant": "7"},
		Tags:        []string{"b", "a"},
		Priority:    7,
		MaxAttempts: 4,
		Timeout:     1500 * time.Millisecond,
		RecurringID: "nightly",
	}
	n0 := now(t, s)
	id := add(t, s, p)
	n1 := now(t, s)

	check := func(from string, j driver.Job) {
		t.Helper()
		switch {
		case j.ID != id:
			t.Fatalf("%s: id %d, want %d", from, j.ID, id)
		case j.Kind != p.Kind || j.Queue != p.Queue || j.RecurringID != p.RecurringID:
			t.Fatalf("%s: kind %q queue %q recurring %q", from, j.Kind, j.Queue, j.RecurringID)
		case !sameJSON(j.Args, p.Args):
			t.Fatalf("%s: args %s, want %s", from, j.Args, p.Args)
		case !maps.Equal(j.Meta, p.Meta):
			t.Fatalf("%s: meta %v, want %v", from, j.Meta, p.Meta)
		case !slices.Equal(j.Tags, p.Tags):
			t.Fatalf("%s: tags %v, want %v", from, j.Tags, p.Tags)
		case j.Priority != p.Priority || j.MaxAttempts != p.MaxAttempts || j.Timeout != p.Timeout:
			t.Fatalf("%s: priority %d max attempts %d timeout %v", from, j.Priority, j.MaxAttempts, j.Timeout)
		case j.BatchID != 0 || j.LimitKey != "" || len(j.Parents) != 0:
			t.Fatalf("%s: batch %d limit %q parents %v, want none", from, j.BatchID, j.LimitKey, j.Parents)
		}
		within(t, from+" created at", j.CreatedAt, n0, n1)
		within(t, from+" run at", j.RunAt, n0, n1)
	}

	r := record(t, s, id)
	check("record", r.Job)
	switch {
	case r.State != driver.Enqueued:
		t.Fatalf("state %s, want enqueued", r.State)
	case r.Attempt != 0 || !r.AttemptedAt.IsZero() || r.Server != "":
		t.Fatalf("attempt %d attempted at %v server %q before any claim", r.Attempt, r.AttemptedAt, r.Server)
	case !r.FinalizedAt.IsZero() || r.CancelRequested || r.PendingDeps != 0 || r.AfterBatch != 0:
		t.Fatalf("unexpected record %+v", r)
	}

	j := claimOne(t, s, "mail", id)
	n2 := now(t, s)
	check("claim", j)
	if j.Attempt != 1 || j.Claim < 1 {
		t.Fatalf("claimed attempt %d claim %d, want attempt 1 and claim >= 1", j.Attempt, j.Claim)
	}
	within(t, "attempted at", j.AttemptedAt, n1, n2)

	r = record(t, s, id)
	if r.State != driver.Processing || r.Server != server || r.Claim != j.Claim || r.Attempt != 1 {
		t.Fatalf("after claim: state %s server %q claim %d attempt %d", r.State, r.Server, r.Claim, r.Attempt)
	}
	within(t, "record attempted at", r.AttemptedAt, n1, n2)
}

func testInsertRunAt(t *testing.T, s driver.Store) {
	p := task("q")
	p.Delay = time.Hour
	n0 := now(t, s)
	delayed := add(t, s, p)
	n1 := now(t, s)
	within(t, "delayed run at", record(t, s, delayed).RunAt, n0.Add(time.Hour), n1.Add(time.Hour))

	at := n0.Add(2 * time.Hour).Truncate(time.Second)
	p = task("q")
	p.RunAt = at
	if got := record(t, s, add(t, s, p)).RunAt; !got.Equal(at) {
		t.Fatalf("run at = %v, want %v", got, at)
	}
}

func testInsertInvalid(t *testing.T, s driver.Store) {
	cases := []struct {
		name string
		edit func(p *driver.InsertParams)
	}{
		{"empty kind", func(p *driver.InsertParams) { p.Kind = "" }},
		{"empty queue", func(p *driver.InsertParams) { p.Queue = "" }},
		{"zero max attempts", func(p *driver.InsertParams) { p.MaxAttempts = 0 }},
		{"negative max attempts", func(p *driver.InsertParams) { p.MaxAttempts = -1 }},
		{"truncated args", func(p *driver.InsertParams) { p.Args = []byte(`{"a":`) }},
		{"garbage args", func(p *driver.InsertParams) { p.Args = []byte(`not json`) }},
		{"zero limit max", func(p *driver.InsertParams) { p.LimitKey, p.LimitMax = "k", 0 }},
		{"parent without mask", func(p *driver.InsertParams) { p.Parents = []driver.Parent{{Index: 0}} }},
		{"after its own batch", func(p *driver.InsertParams) { p.BatchID, p.AfterBatch = 1, 1 }},
		{"args not json", func(p *driver.InsertParams) { p.Args = []byte("{") }},
	}
	for _, c := range cases {
		bad := task("q")
		c.edit(&bad)
		_, err := s.Insert(t.Context(), []driver.InsertParams{task("q"), bad})
		if err == nil {
			t.Fatalf("%s: insert succeeded", c.name)
		}
		wantErr(t, err, driver.ErrInvalid)
		wantEmpty(t, s)
	}
	insert(t, s, task("q"))
}

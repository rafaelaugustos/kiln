package drivertest

import (
	"bytes"
	"errors"
	"maps"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

var recurringTests = []test{
	{"Put", testRecurringPut},
	{"CAS", testRecurringCAS},
	{"Remove", testRecurringRemove},
	{"Due", testRecurringDue},
	{"Fire", testRecurringFire},
	{"FireEmpty", testRecurringFireEmpty},
	{"FireInvalid", testRecurringFireInvalid},
	{"FireDuplicate", testRecurringFireDuplicate},
	{"FireConcurrent", testRecurringFireConcurrent},
}

func cron(id string, next time.Time) driver.Recurring {
	return driver.Recurring{
		ID:       id,
		Spec:     "*/5 * * * *",
		Location: "UTC",
		Template: driver.InsertParams{
			Kind:        "report",
			Queue:       "cron",
			Args:        []byte(`{"n":1}`),
			MaxAttempts: 3,
			RecurringID: id,
		},
		NextRunAt: next,
	}
}

func put(t *testing.T, s driver.Store, r driver.Recurring) {
	t.Helper()
	if err := s.PutRecurring(t.Context(), r); err != nil {
		t.Fatalf("put recurring %s: %v", r.ID, err)
	}
}

func getRecurring(t *testing.T, s driver.Store, id string) driver.Recurring {
	t.Helper()
	r, err := s.Recurring(t.Context(), id)
	if err != nil {
		t.Fatalf("recurring %s: %v", id, err)
	}
	return r
}

func occurrence(r driver.Recurring, at time.Time) driver.InsertParams {
	p := r.Template
	p.RunAt = at
	p.Meta = map[string]string{"kiln.occurrence": at.UTC().Format(time.RFC3339)}
	return p
}

func sameTemplate(a, b driver.InsertParams) bool {
	return a.Kind == b.Kind && a.Queue == b.Queue && sameJSON(a.Args, b.Args) &&
		maps.Equal(a.Meta, b.Meta) && slices.Equal(a.Tags, b.Tags) && a.Priority == b.Priority &&
		a.MaxAttempts == b.MaxAttempts && a.Timeout == b.Timeout && bytes.Equal(a.UniqueKey, b.UniqueKey) &&
		a.UniqueFor == b.UniqueFor && a.LimitKey == b.LimitKey && a.LimitMax == b.LimitMax &&
		a.RecurringID == b.RecurringID && a.BatchID == b.BatchID && a.AfterBatch == b.AfterBatch
}

func testRecurringPut(t *testing.T, s driver.Store) {
	n := now(t, s).Truncate(time.Second)
	r := cron("nightly", n.Add(time.Minute))
	r.Spec, r.Location = "0 3 * * *", "America/Sao_Paulo"
	r.Misfire, r.Overlap = driver.MisfireAll, true
	r.Template.Meta = map[string]string{"team": "data"}
	r.Template.Tags = []string{"cron", "report"}
	r.Template.Priority, r.Template.Timeout = 3, 90*time.Second
	r.Template.UniqueKey, r.Template.UniqueFor = key("nightly"), time.Hour
	r.Template.LimitKey, r.Template.LimitMax = "reports", 2
	put(t, s, r)

	got := getRecurring(t, s, "nightly")
	switch {
	case got.ID != r.ID || got.Spec != r.Spec || got.Location != r.Location:
		t.Fatalf("got %+v", got)
	case got.Misfire != r.Misfire || got.Overlap != r.Overlap || got.Paused:
		t.Fatalf("misfire %d overlap %v paused %v", got.Misfire, got.Overlap, got.Paused)
	case !got.NextRunAt.Equal(r.NextRunAt) || !got.LastRunAt.IsZero() || got.LastJobID != 0:
		t.Fatalf("next %v last %v job %d", got.NextRunAt, got.LastRunAt, got.LastJobID)
	case got.Version <= 0:
		t.Fatalf("version %d, want > 0", got.Version)
	case !sameTemplate(got.Template, r.Template):
		t.Fatalf("template %+v, want %+v", got.Template, r.Template)
	}

	put(t, s, cron("hourly", n.Add(time.Hour)))
	all, err := s.Recurrings(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]string, len(all))
	for i, r := range all {
		ids[i] = r.ID
	}
	slices.Sort(ids)
	if !slices.Equal(ids, []string{"hourly", "nightly"}) {
		t.Fatalf("recurrings %v", ids)
	}
}

func testRecurringCAS(t *testing.T, s driver.Store) {
	ctx := t.Context()
	r := cron("a", now(t, s).Add(time.Minute).Truncate(time.Second))
	put(t, s, r)
	v1 := getRecurring(t, s, "a").Version

	wantErr(t, s.PutRecurring(ctx, r), driver.ErrConflict)

	r.Version, r.Paused = v1, true
	put(t, s, r)
	got := getRecurring(t, s, "a")
	if got.Version <= v1 || !got.Paused {
		t.Fatalf("version %d paused %v after update from %d", got.Version, got.Paused, v1)
	}

	r.Paused = false
	wantErr(t, s.PutRecurring(ctx, r), driver.ErrConflict)
	if !getRecurring(t, s, "a").Paused {
		t.Fatal("stale put was applied")
	}

	b := cron("b", r.NextRunAt)
	b.Version = 7
	wantErr(t, s.PutRecurring(ctx, b), driver.ErrConflict)
	_, err := s.Recurring(ctx, "b")
	wantErr(t, err, driver.ErrNotFound)
}

func testRecurringRemove(t *testing.T, s driver.Store) {
	ctx := t.Context()
	put(t, s, cron("a", now(t, s).Add(time.Minute)))
	if err := s.RemoveRecurring(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	_, err := s.Recurring(ctx, "a")
	wantErr(t, err, driver.ErrNotFound)
	wantErr(t, s.RemoveRecurring(ctx, "a"), driver.ErrNotFound)
	all, err := s.Recurrings(ctx)
	if err != nil || len(all) != 0 {
		t.Fatalf("recurrings %v %v, want none", all, err)
	}
	put(t, s, cron("a", now(t, s).Add(time.Minute)))
}

func testRecurringDue(t *testing.T, s driver.Store) {
	n := now(t, s)
	put(t, s, cron("due1", n.Add(-time.Minute)))
	put(t, s, cron("due2", n.Add(-2*time.Minute)))
	put(t, s, cron("future", n.Add(time.Hour)))
	paused := cron("paused", n.Add(-time.Minute))
	paused.Paused = true
	put(t, s, paused)
	put(t, s, cron("never", time.Time{}))

	n0 := now(t, s)
	rs, at, err := s.Due(t.Context(), 10)
	if err != nil {
		t.Fatal(err)
	}
	n1 := now(t, s)
	within(t, "due now", at, n0, n1)
	var ids []string
	for _, r := range rs {
		ids = append(ids, r.ID)
		if r.Version <= 0 || r.Spec == "" || r.Template.Kind != "report" {
			t.Fatalf("due row %+v", r)
		}
	}
	slices.Sort(ids)
	if !slices.Equal(ids, []string{"due1", "due2"}) {
		t.Fatalf("due %v, want [due1 due2]", ids)
	}
	if rs, _, err := s.Due(t.Context(), 1); err != nil || len(rs) != 1 {
		t.Fatalf("due with limit 1 returned %d rows, %v", len(rs), err)
	}
}

func testRecurringFire(t *testing.T, s driver.Store) {
	ctx := t.Context()
	n := now(t, s).Truncate(time.Second)
	r := cron("a", n.Add(-time.Minute))
	put(t, s, r)
	r = getRecurring(t, s, "a")
	occ := r.NextRunAt
	f := driver.Fire{ID: "a", Version: r.Version, NextRunAt: n.Add(4 * time.Minute), LastRunAt: occ, Jobs: []driver.InsertParams{occurrence(r, occ)}}
	ins, err := s.Fire(ctx, f)
	if err != nil {
		t.Fatal(err)
	}
	if len(ins) != 1 || ins[0].Duplicate || ins[0].State != driver.Enqueued {
		t.Fatalf("fire inserted %+v", ins)
	}
	got := getRecurring(t, s, "a")
	switch {
	case got.Version <= r.Version:
		t.Fatalf("version %d after fire from %d", got.Version, r.Version)
	case !got.NextRunAt.Equal(f.NextRunAt) || !got.LastRunAt.Equal(occ):
		t.Fatalf("next %v last %v, want %v and %v", got.NextRunAt, got.LastRunAt, f.NextRunAt, occ)
	case got.LastJobID != ins[0].ID:
		t.Fatalf("last job %d, want %d", got.LastJobID, ins[0].ID)
	}
	j := record(t, s, ins[0].ID)
	if j.RecurringID != "a" || j.Kind != "report" || !j.RunAt.Equal(occ) || j.Meta["kiln.occurrence"] != occ.UTC().Format(time.RFC3339) {
		t.Fatalf("fired job %+v", j.Job)
	}
	due, _, err := s.Due(ctx, 10)
	if err != nil || len(due) != 0 {
		t.Fatalf("due after fire: %d rows, %v", len(due), err)
	}

	_, err = s.Fire(ctx, f)
	wantErr(t, err, driver.ErrConflict)
	if c := counts(t, s); c.Enqueued != 1 {
		t.Fatalf("enqueued %d after conflicting fire, want 1", c.Enqueued)
	}
	if again := getRecurring(t, s, "a"); again.Version != got.Version {
		t.Fatalf("version moved to %d on conflicting fire", again.Version)
	}

	_, err = s.Fire(ctx, driver.Fire{ID: "missing", Version: 1, NextRunAt: n, Jobs: []driver.InsertParams{occurrence(r, occ)}})
	wantErr(t, err, driver.ErrNotFound)
	if c := counts(t, s); c.Enqueued != 1 {
		t.Fatalf("enqueued %d after firing a missing recurring, want 1", c.Enqueued)
	}
}

func testRecurringFireEmpty(t *testing.T, s driver.Store) {
	n := now(t, s).Truncate(time.Second)
	put(t, s, cron("a", n.Add(-time.Hour)))
	r := getRecurring(t, s, "a")
	ins, err := s.Fire(t.Context(), driver.Fire{ID: "a", Version: r.Version, NextRunAt: n.Add(5 * time.Minute), LastRunAt: r.NextRunAt})
	if err != nil || len(ins) != 0 {
		t.Fatalf("fire without jobs: %+v %v", ins, err)
	}
	got := getRecurring(t, s, "a")
	if got.Version <= r.Version || !got.NextRunAt.Equal(n.Add(5*time.Minute)) {
		t.Fatalf("version %d next %v", got.Version, got.NextRunAt)
	}
	wantEmpty(t, s)
}

func testRecurringFireInvalid(t *testing.T, s driver.Store) {
	n := now(t, s).Truncate(time.Second)
	put(t, s, cron("a", n.Add(-time.Minute)))
	r := getRecurring(t, s, "a")
	bad := occurrence(r, r.NextRunAt)
	bad.Args = []byte(`{"n":`)
	f := driver.Fire{ID: "a", Version: r.Version, NextRunAt: n.Add(time.Hour), LastRunAt: r.NextRunAt, Jobs: []driver.InsertParams{occurrence(r, r.NextRunAt), bad}}
	_, err := s.Fire(t.Context(), f)
	wantErr(t, err, driver.ErrInvalid)
	got := getRecurring(t, s, "a")
	if got.Version != r.Version || !got.NextRunAt.Equal(r.NextRunAt) || !got.LastRunAt.IsZero() {
		t.Fatalf("recurring %+v changed by a failed fire", got)
	}
	wantEmpty(t, s)
}

func testRecurringFireDuplicate(t *testing.T, s driver.Store) {
	ctx := t.Context()
	n := now(t, s).Truncate(time.Second)
	r := cron("a", n.Add(-2*time.Minute))
	r.Template.UniqueKey = key("recurring\x00a")
	put(t, s, r)
	r = getRecurring(t, s, "a")
	first, err := s.Fire(ctx, driver.Fire{ID: "a", Version: r.Version, NextRunAt: n.Add(-time.Minute), LastRunAt: r.NextRunAt, Jobs: []driver.InsertParams{occurrence(r, r.NextRunAt)}})
	if err != nil || len(first) != 1 || first[0].Duplicate {
		t.Fatalf("first fire %+v %v", first, err)
	}
	r = getRecurring(t, s, "a")
	second, err := s.Fire(ctx, driver.Fire{ID: "a", Version: r.Version, NextRunAt: n.Add(time.Minute), LastRunAt: r.NextRunAt, Jobs: []driver.InsertParams{occurrence(r, r.NextRunAt)}})
	if err != nil || len(second) != 1 || !second[0].Duplicate || second[0].ID != first[0].ID {
		t.Fatalf("second fire %+v %v, want duplicate of %d", second, err, first[0].ID)
	}
	got := getRecurring(t, s, "a")
	if got.Version <= r.Version || !got.NextRunAt.Equal(n.Add(time.Minute)) {
		t.Fatalf("version %d next %v after duplicate fire", got.Version, got.NextRunAt)
	}
	if c := counts(t, s); c.Enqueued != 1 {
		t.Fatalf("enqueued %d, want 1", c.Enqueued)
	}
}

func testRecurringFireConcurrent(t *testing.T, s driver.Store) {
	n := now(t, s).Truncate(time.Second)
	put(t, s, cron("a", n.Add(-time.Minute)))
	r := getRecurring(t, s, "a")
	f := driver.Fire{ID: "a", Version: r.Version, NextRunAt: n.Add(time.Minute), LastRunAt: r.NextRunAt, Jobs: []driver.InsertParams{occurrence(r, r.NextRunAt)}}
	const workers = 8
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		wins int
	)
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.Fire(t.Context(), f)
			switch {
			case err == nil:
				mu.Lock()
				wins++
				mu.Unlock()
			case !errors.Is(err, driver.ErrConflict):
				t.Errorf("fire: %v", err)
			}
		}()
	}
	wg.Wait()
	if wins != 1 {
		t.Fatalf("%d fires succeeded on the same version, want 1", wins)
	}
	if c := counts(t, s); c.Enqueued != 1 {
		t.Fatalf("enqueued %d, want 1", c.Enqueued)
	}
}

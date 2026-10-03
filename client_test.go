package kiln

import (
	"context"
	"errors"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/rafaelaugustos/kiln/driver"
	"github.com/rafaelaugustos/kiln/memstore"
)

func TestParentIDs(t *testing.T) {
	t.Parallel()
	c := NewClient(memstore.New())
	ctx := context.Background()
	for _, opt := range []InsertOption{After{0}, AfterFinished{0}, After{-3}} {
		_, err := c.EnqueueMany(ctx, Spec{Args: testArgs{K: "a"}}, Spec{Args: testArgs{K: "b"}, Options: []InsertOption{opt}})
		if !errors.Is(err, ErrInvalid) {
			t.Errorf("EnqueueMany with %v: err = %v", opt, err)
		}
		_, err = c.Enqueue(ctx, testArgs{K: "b"}, opt)
		if !errors.Is(err, ErrInvalid) || !strings.Contains(err.Error(), "parent id") {
			t.Errorf("Enqueue with %v: err = %v", opt, err)
		}
	}
}

func TestDotNames(t *testing.T) {
	t.Parallel()
	c := NewClient(memstore.New())
	ctx := context.Background()
	for _, name := range []string{".", ".."} {
		if err := c.SetRecurring(ctx, name, "@daily", testArgs{K: "a"}); !errors.Is(err, ErrInvalid) {
			t.Errorf("SetRecurring(%q): err = %v", name, err)
		}
		if _, err := c.Enqueue(ctx, testArgs{K: "a"}, Queue(name)); !errors.Is(err, ErrInvalid) {
			t.Errorf("Enqueue to queue %q: err = %v", name, err)
		}
	}
	if err := c.SetRecurring(ctx, "a..b", "@daily", testArgs{K: "a"}); err != nil {
		t.Errorf("SetRecurring(a..b): %v", err)
	}
	if _, err := c.Enqueue(ctx, testArgs{K: "a"}, Queue("...")); err != nil {
		t.Errorf("Enqueue to queue ...: %v", err)
	}
}

func TestSyncRecurring(t *testing.T) {
	t.Parallel()
	store := memstore.New()
	c := NewClient(store)
	ctx := context.Background()
	if err := c.SetRecurring(ctx, "solo", "@daily", testArgs{K: "a"}); err != nil {
		t.Fatal(err)
	}
	daily := RecurringSpec{ID: "daily", Spec: "@daily", Args: testArgs{K: "a"}}
	hourly := RecurringSpec{ID: "hourly", Spec: "@hourly", Args: testArgs{K: "b"}}
	other := RecurringSpec{ID: "other", Spec: "@daily", Args: testArgs{K: "c"}}
	for _, sync := range []struct {
		group string
		jobs  []RecurringSpec
	}{
		{"reports", []RecurringSpec{daily, hourly}},
		{"misc", []RecurringSpec{other}},
		{"reports", []RecurringSpec{daily}},
	} {
		if err := c.SyncRecurring(ctx, sync.group, sync.jobs...); err != nil {
			t.Fatalf("sync %s: %v", sync.group, err)
		}
	}
	rs, err := store.Recurrings(ctx)
	if err != nil {
		t.Fatal(err)
	}
	groups := make(map[string]string, len(rs))
	for _, r := range rs {
		groups[r.ID] = r.Group
	}
	if want := map[string]string{"daily": "reports", "other": "misc", "solo": ""}; !maps.Equal(groups, want) {
		t.Fatalf("recurring groups %v, want %v", groups, want)
	}
	if err := c.SyncRecurring(ctx, "", daily); !errors.Is(err, ErrInvalid) {
		t.Errorf("sync with no group: err = %v", err)
	}
}

func TestErrorPrefix(t *testing.T) {
	t.Parallel()
	c := NewClient(memstore.New())
	ctx := context.Background()
	var b Batch
	b.Add(testArgs{K: "a"}, Queue("BAD"))
	var then Batch
	then.Add(testArgs{K: "a"})
	then.Then(testArgs{K: "a"}, Queue("BAD"))
	_, many := c.EnqueueMany(ctx, Spec{Args: testArgs{K: "a"}, Options: []InsertOption{Queue("BAD")}})
	_, batch := c.StartBatch(ctx, &b)
	_, cont := c.StartBatch(ctx, &then)
	for _, err := range []error{
		c.SetRecurring(ctx, "r", "bogus", testArgs{K: "a"}),
		many,
		batch,
		cont,
	} {
		if !errors.Is(err, ErrInvalid) || strings.Count(err.Error(), "kiln:") != 1 {
			t.Errorf("err = %v", err)
		}
	}
}

type flakyWriter struct {
	driver.Store
}

func (f flakyWriter) Insert(ctx context.Context, jobs []driver.InsertParams) ([]driver.Inserted, error) {
	if jobs[0].AfterBatch != 0 {
		return nil, errDown
	}
	return f.Store.Insert(ctx, jobs)
}

func TestStartBatchCleansUp(t *testing.T) {
	t.Parallel()
	store := memstore.New()
	ctx := context.Background()
	shared := mustEnqueue(t, NewClient(store), testArgs{K: "a", N: 9}, Unique{})
	c := NewClient(flakyWriter{store})
	var b Batch
	b.Add(testArgs{K: "a", N: 1})
	b.Add(testArgs{K: "a", N: 9}, Unique{})
	b.Then(testArgs{K: "b"})
	if _, err := c.StartBatch(ctx, &b); !errors.Is(err, errDown) {
		t.Fatalf("err = %v", err)
	}
	page, err := store.Batches(ctx, driver.BatchQuery{})
	if err != nil || len(page.Batches) != 1 {
		t.Fatalf("batches %+v %v", page, err)
	}
	bt := page.Batches[0]
	if !bt.Sealed || bt.FinishedAt.IsZero() || bt.Total == 0 || bt.Counts[Deleted] != bt.Total {
		t.Fatalf("batch not discarded: %+v", bt)
	}
	if r := getJob(t, c, shared); r.State != Enqueued || !slices.Equal(reasons(r), nil) {
		t.Errorf("unrelated duplicate touched: %s %v", r.State, reasons(r))
	}
}

func TestStartBatchNested(t *testing.T) {
	t.Parallel()
	store := memstore.New()
	c := NewClient(store)
	ctx := context.Background()
	var top, mid, low Batch
	top.Add(testArgs{K: "a", N: 1})
	top.Then(testArgs{K: "b", N: 1})
	mid.Add(testArgs{K: "a", N: 2})
	mid.Then(testArgs{K: "b", N: 2})
	low.Add(testArgs{K: "a", N: 3})
	mid.AddBatch(&low)
	top.AddBatch(&mid)
	id, err := c.StartBatch(ctx, &top)
	if err != nil {
		t.Fatal(err)
	}
	page, err := store.Batches(ctx, driver.BatchQuery{Parent: id})
	if err != nil || len(page.Batches) != 1 {
		t.Fatalf("nested batches %+v %v", page, err)
	}
	inner := page.Batches[0]
	if !inner.Sealed || inner.Total != 1 || inner.Nested != 1 {
		t.Fatalf("nested batch %+v", inner)
	}
	jobs, err := store.Jobs(ctx, driver.JobQuery{State: Awaiting})
	if err != nil {
		t.Fatal(err)
	}
	want := map[int64]int64{id: 0, inner.ID: id}
	for _, r := range jobs.Records {
		if b, ok := want[r.AfterBatch]; !ok || r.BatchID != b {
			t.Errorf("continuation %d after batch %d in batch %d", r.ID, r.AfterBatch, r.BatchID)
		}
		delete(want, r.AfterBatch)
	}
	if len(want) > 0 {
		t.Errorf("continuations of batches %v missing", want)
	}

	more := &Batch{Parent: id}
	more.Then(testArgs{K: "b", N: 3})
	late, err := c.StartBatch(ctx, more)
	if err != nil {
		t.Fatal(err)
	}
	if b, err := store.Batch(ctx, late); err != nil || b.Parent != id || b.FinishedAt.IsZero() {
		t.Fatalf("batch started in %d: %+v %v", id, b, err)
	}
	if b, _ := store.Batch(ctx, id); b.Total != 3 || b.Nested != 2 {
		t.Fatalf("outer batch %+v", b)
	}

	var self, a, b, x, y, shared Batch
	self.AddBatch(&self)
	a.AddBatch(&b)
	b.AddBatch(&a)
	x.AddBatch(&shared)
	x.AddBatch(&y)
	y.AddBatch(&shared)
	for _, b := range []*Batch{&self, &a, &x} {
		if _, err := c.StartBatch(ctx, b); !errors.Is(err, ErrInvalid) {
			t.Errorf("err = %v, want invalid", err)
		}
	}
	if _, err := c.StartBatch(ctx, &Batch{Parent: 1 << 40}); !errors.Is(err, ErrNotFound) {
		t.Errorf("unknown parent: err = %v", err)
	}
	if page, _ := store.Batches(ctx, driver.BatchQuery{}); len(page.Batches) != 4 {
		t.Errorf("%d batches, want 4", len(page.Batches))
	}
}

func TestStartBatchNestedCleansUp(t *testing.T) {
	t.Parallel()
	store := memstore.New()
	c := NewClient(flakyWriter{store})
	var outer, inner Batch
	outer.Add(testArgs{K: "a", N: 1})
	inner.Add(testArgs{K: "a", N: 2})
	inner.Then(testArgs{K: "b"})
	outer.AddBatch(&inner)
	if _, err := c.StartBatch(context.Background(), &outer); !errors.Is(err, errDown) {
		t.Fatalf("err = %v", err)
	}
	page, err := store.Batches(context.Background(), driver.BatchQuery{})
	if err != nil || len(page.Batches) != 2 {
		t.Fatalf("batches %+v %v", page, err)
	}
	for _, b := range page.Batches {
		if !b.Sealed || b.FinishedAt.IsZero() || b.Total != 1 || b.Counts[Deleted] != 1 {
			t.Errorf("batch not discarded: %+v", b)
		}
	}
}

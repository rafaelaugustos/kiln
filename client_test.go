package kiln

import (
	"context"
	"errors"
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

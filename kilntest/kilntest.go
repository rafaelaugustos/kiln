package kilntest

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln"
	"github.com/rafaelaugustos/kiln/driver"
	"github.com/rafaelaugustos/kiln/internal/testhook"
)

var dispatch = testhook.Dispatch.(func(*kiln.Mux, context.Context, kiln.Args, ...kiln.InsertOption) (driver.Outcome, error))

// Result is what a server would record for the attempt run by [Work].
type Result struct {
	State  kiln.State      // the job's next state; empty when args or options are invalid
	Err    error           // the handler's error, or why the job could not run
	Delay  time.Duration   // wait before the next attempt, for a retry or a snooze
	Output json.RawMessage // what the handler recorded with SetOutput, if it succeeded
}

// Work runs args through the handler m has for its kind, as the first attempt of a job built with
// opts, and returns the outcome. It uses the same code as a server: the args go through JSON, the
// middleware of m runs, and the result is classified by the same rules, so a plain error gives a
// retry unless [kiln.MaxAttempts] is 1. The handler options of the kind apply, and server defaults
// fill in the rest. ctx reaches the handler, where [kiln.JobFrom] works and [kiln.ClientFrom] does
// not, and [kiln.Job.SetParam] changes only the job's Meta. Work does not freeze m.
func Work[T kiln.Args](ctx context.Context, m *kiln.Mux, args T, opts ...kiln.InsertOption) Result {
	o, err := dispatch(m, ctx, args, opts...)
	return Result{State: o.State, Err: err, Delay: o.Delay, Output: o.Output}
}

// RequireEnqueued returns a job of T's kind that is waiting to run (enqueued, scheduled, awaiting
// or throttled) and for which match returns true; a nil match accepts any. It fails the test when
// there is none or the store returns an error.
func RequireEnqueued[T kiln.Args](tb testing.TB, c *kiln.Client, match func(*kiln.Job[T]) bool) *kiln.Job[T] {
	tb.Helper()
	j, err := find(tb.Context(), c, match)
	if err != nil {
		tb.Fatalf("kilntest: %v", err)
	}
	if j == nil {
		var zero T
		tb.Fatalf("kilntest: no matching %s job is enqueued", zero.Kind())
	}
	return j
}

// RequireNotEnqueued fails the test when a job of T's kind is waiting to run and match returns
// true for it, a nil match accepting any, or when the store returns an error. It looks at the same
// states as [RequireEnqueued].
func RequireNotEnqueued[T kiln.Args](tb testing.TB, c *kiln.Client, match func(*kiln.Job[T]) bool) {
	tb.Helper()
	j, err := find(tb.Context(), c, match)
	if err != nil {
		tb.Fatalf("kilntest: %v", err)
	}
	if j != nil {
		tb.Fatalf("kilntest: %s job %d is enqueued", j.Kind, j.ID)
	}
}

var waiting = [...]kiln.State{kiln.Enqueued, kiln.Scheduled, kiln.Awaiting, kiln.Throttled}

func find[T kiln.Args](ctx context.Context, c *kiln.Client, match func(*kiln.Job[T]) bool) (*kiln.Job[T], error) {
	var zero T
	kind := zero.Kind()
	for _, st := range waiting {
		q := kiln.JobQuery{State: st, Kind: kind, Limit: 500}
		for {
			p, err := c.List(ctx, q)
			if err != nil {
				return nil, err
			}
			for i := range p.Records {
				j, err := decode[T](&p.Records[i])
				if err != nil {
					return nil, err
				}
				if match == nil || match(j) {
					return j, nil
				}
			}
			if p.Next == "" {
				break
			}
			q.Cursor = p.Next
		}
	}
	return nil, nil
}

func decode[T kiln.Args](r *kiln.Record) (*kiln.Job[T], error) {
	j := &kiln.Job[T]{
		ID:          r.ID,
		Kind:        r.Kind,
		Queue:       r.Queue,
		Priority:    r.Priority,
		Attempt:     r.Attempt,
		MaxAttempts: r.MaxAttempts,
		CreatedAt:   r.CreatedAt,
		RunAt:       r.RunAt,
		BatchID:     r.BatchID,
		RecurringID: r.RecurringID,
		Parents:     r.Parents,
		Meta:        r.Meta,
		Tags:        r.Tags,
	}
	if err := json.Unmarshal(r.Args, &j.Args); err != nil {
		return nil, fmt.Errorf("decode %s job %d: %w", r.Kind, r.ID, err)
	}
	return j, nil
}

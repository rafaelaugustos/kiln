package kiln

import (
	"context"
	"fmt"

	"github.com/rafaelaugustos/kiln/driver"
)

// Delete deletes the jobs with the given ids and returns how many it affected. A running job has
// its handler's context canceled with [ErrCanceled], and ends deleted unless the handler returns
// nil. Waiting and failed jobs are deleted immediately, and jobs that already succeeded or were
// deleted are left alone.
func (c *Client) Delete(ctx context.Context, ids ...int64) (int, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	return c.store.Delete(ctx, driver.Filter{IDs: ids})
}

// Requeue puts the given jobs back in their queues to run now, and returns how many it moved. It
// applies to failed, scheduled, succeeded and deleted jobs and skips the others. A failed,
// succeeded or deleted job starts over with all of its [MaxAttempts], and its retries back off
// from the start again; a scheduled job keeps its attempt count. A job is also skipped when its
// [Unique] key, held without For, now belongs to another job.
func (c *Client) Requeue(ctx context.Context, ids ...int64) (int, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	return c.store.Requeue(ctx, driver.Filter{IDs: ids})
}

// DeleteWhere is [Client.Delete] for every job that matches f, which must name ids or a state.
func (c *Client) DeleteWhere(ctx context.Context, f Filter) (int, error) {
	if err := driver.CheckFilter(f); err != nil {
		return 0, err
	}
	return c.store.Delete(ctx, f)
}

// RequeueWhere is [Client.Requeue] for every job that matches f, which must name ids or a state.
func (c *Client) RequeueWhere(ctx context.Context, f Filter) (int, error) {
	if err := driver.CheckFilter(f); err != nil {
		return 0, err
	}
	return c.store.Requeue(ctx, f)
}

// PauseQueue stops servers from claiming jobs from the named queue until [Client.ResumeQueue].
// Running jobs are not affected, and jobs can still be enqueued.
func (c *Client) PauseQueue(ctx context.Context, name string) error {
	return c.pause(ctx, name, true)
}

// ResumeQueue lets servers claim jobs from the named queue again.
func (c *Client) ResumeQueue(ctx context.Context, name string) error {
	return c.pause(ctx, name, false)
}

func (c *Client) pause(ctx context.Context, name string, paused bool) error {
	if !validQueue(name) {
		return fmt.Errorf("%w: queue %q", ErrInvalid, name)
	}
	return c.store.PauseQueue(ctx, name, paused)
}

// Get returns the job with the given id, with its history and output. It fails with
// [ErrNotFound] when there is no such job, or when it has been pruned.
func (c *Client) Get(ctx context.Context, id int64) (Record, error) {
	return c.store.Job(ctx, id)
}

// List returns a page of the jobs in q.State, which is required, optionally narrowed by queue,
// kind, batch and tag, in the order described at [driver.Inspector.Jobs]. To get the next page,
// pass the returned Next as q.Cursor. A page narrowed by tag can be short, or empty, with more to
// come.
func (c *Client) List(ctx context.Context, q JobQuery) (Page, error) {
	if !q.State.Valid() {
		return Page{}, fmt.Errorf("%w: state %q", ErrInvalid, q.State)
	}
	return c.store.Jobs(ctx, q)
}

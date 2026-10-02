package kiln

import (
	"context"
	"fmt"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

// Batch groups jobs so that continuations can run once all of them have finished. Add jobs with
// [Batch.Add] and continuations with [Batch.Then], then insert everything with [Client.StartBatch].
//
// A batch finishes when every job in it has succeeded or been deleted; a failed job keeps it open
// until the job is requeued and succeeds, or is deleted. Continuations run when the batch
// finishes, whether its jobs succeeded or were deleted.
type Batch struct {
	Description string            // shown in the dashboard
	Meta        map[string]string // up to 64 keys and 16 KiB in all

	jobs []Spec
	then []Spec
}

// Add appends a job to the batch. Nothing is inserted until [Client.StartBatch].
func (b *Batch) Add(args Args, opts ...InsertOption) {
	b.jobs = append(b.jobs, Spec{Args: args, Options: opts})
}

// Then appends a continuation, a job inserted with the batch that runs once the batch has
// finished. Continuations are not members of the batch.
func (b *Batch) Then(args Args, opts ...InsertOption) {
	b.then = append(b.then, Spec{Args: args, Options: opts})
}

// Len returns the number of jobs added with [Batch.Add], not counting continuations.
func (b *Batch) Len() int {
	return len(b.jobs)
}

// StartBatch inserts the jobs and continuations of b under a new batch, seals it and returns its
// id. With a store that implements [driver.Transactor] this happens in one transaction; with other
// stores, a failure midway deletes the jobs already inserted. A job whose [Unique] key is held
// stays out of the batch. A batch without jobs finishes as it is sealed, so its continuations run
// right away.
func (c *Client) StartBatch(ctx context.Context, b *Batch) (int64, error) {
	jobs, then, err := b.params()
	if err != nil {
		return 0, err
	}
	t, ok := c.store.(driver.Transactor)
	if !ok {
		id, added, err := c.startBatch(ctx, c.store, b, jobs, then)
		if err != nil {
			if id != 0 {
				c.discardBatch(ctx, id, added)
			}
			return 0, err
		}
		return id, nil
	}
	var id int64
	err = t.InTx(ctx, func(w driver.Writer) error {
		id, _, err = c.startBatch(ctx, w, b, jobs, then)
		return err
	})
	if err != nil {
		return 0, err
	}
	return id, nil
}

// StartBatchTx is [Client.StartBatch] inside the application's transaction, through w; see
// [Client.EnqueueTx].
func (c *Client) StartBatchTx(ctx context.Context, w driver.Writer, b *Batch) (int64, error) {
	if w == nil {
		return 0, driver.ErrNilTx
	}
	jobs, then, err := b.params()
	if err != nil {
		return 0, err
	}
	id, _, err := c.startBatch(ctx, w, b, jobs, then)
	if err != nil {
		return 0, err
	}
	return id, nil
}

// OpenBatch creates an empty batch and returns its id, for a batch built over several calls: add
// jobs to it with the [InBatch] option, then call [Client.SealBatch]. The batch cannot finish
// before it is sealed.
func (c *Client) OpenBatch(ctx context.Context, description string, meta map[string]string) (int64, error) {
	if err := checkMeta(meta); err != nil {
		return 0, err
	}
	return c.store.OpenBatch(ctx, driver.NewBatch{Description: description, Meta: meta})
}

// SealBatch marks the batch complete, so that it finishes once every job in it has succeeded or
// been deleted. Jobs can still join it with [InBatch] until then. Sealing twice is not an error;
// an unknown id is [ErrNotFound].
func (c *Client) SealBatch(ctx context.Context, id int64) error {
	return c.store.SealBatch(ctx, id)
}

func (b *Batch) params() (jobs, then []driver.InsertParams, err error) {
	if err := checkMeta(b.Meta); err != nil {
		return nil, nil, err
	}
	if len(b.jobs) > 0 {
		if jobs, err = buildAll(b.jobs); err != nil {
			return nil, nil, fmt.Errorf("batch: %w", err)
		}
	}
	if len(b.then) > 0 {
		if then, err = buildAll(b.then); err != nil {
			return nil, nil, fmt.Errorf("batch continuation: %w", err)
		}
	}
	return jobs, then, nil
}

func (c *Client) startBatch(ctx context.Context, w driver.Writer, b *Batch, jobs, then []driver.InsertParams) (id int64, added []int64, err error) {
	id, err = w.OpenBatch(ctx, driver.NewBatch{Description: b.Description, Meta: b.Meta})
	if err != nil {
		return 0, nil, err
	}
	for i := range jobs {
		jobs[i].BatchID = id
	}
	for i := range then {
		then[i].AfterBatch = id
	}
	for _, ps := range [][]driver.InsertParams{jobs, then} {
		if len(ps) == 0 {
			continue
		}
		res, err := c.insertAll(ctx, w, ps)
		if err != nil {
			return id, added, err
		}
		for _, r := range res {
			if !r.Duplicate {
				added = append(added, r.ID)
			}
		}
	}
	return id, added, w.SealBatch(ctx, id)
}

func (c *Client) discardBatch(ctx context.Context, id int64, added []int64) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if len(added) > 0 {
		c.store.Delete(ctx, driver.Filter{IDs: added})
	}
	c.store.SealBatch(ctx, id)
}

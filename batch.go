package kiln

import (
	"context"
	"fmt"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

// Batch groups jobs so that continuations can run once all of them have finished. Add jobs with
// [Batch.Add], continuations with [Batch.Then] and nested batches with [Batch.AddBatch], then
// insert everything with [Client.StartBatch], or with [Client.OpenBatch] to keep adding to it.
//
// A batch finishes when every job in it has succeeded or been deleted and every batch nested in
// it has finished; a failed job keeps it open until the job is requeued and succeeds, or is
// deleted. Continuations run when the batch finishes, whether its jobs succeeded or were deleted.
type Batch struct {
	Description string            // shown in the dashboard
	Meta        map[string]string // up to 64 keys and 16 KiB in all

	// Parent nests the batch in an existing batch that can still take jobs, as [InBatch] does
	// for a job; a running job of that batch can pass its BatchID. The parent waits for the
	// batch to finish, and the batch's continuations become jobs of the parent.
	Parent int64

	jobs   []Spec
	then   []Spec
	nested []*Batch
}

// Add appends a job to the batch. Nothing is inserted until [Client.StartBatch].
func (b *Batch) Add(args Args, opts ...InsertOption) {
	b.jobs = append(b.jobs, Spec{Args: args, Options: opts})
}

// Then appends a continuation, a job inserted with the batch that runs once the batch has
// finished. Continuations are not members of the batch; those of a nested batch are members of
// the batch it is nested in, which therefore waits for them too.
func (b *Batch) Then(args Args, opts ...InsertOption) {
	b.then = append(b.then, Spec{Args: args, Options: opts})
}

// AddBatch nests child in the batch, which then finishes only after child has. [Client.StartBatch]
// inserts child, its jobs and its own nested batches together with b. A batch can be nested in
// one place only, and never in itself.
func (b *Batch) AddBatch(child *Batch) {
	b.nested = append(b.nested, child)
}

// Len returns the number of jobs added with [Batch.Add], not counting continuations.
func (b *Batch) Len() int {
	return len(b.jobs)
}

// StartBatch inserts the jobs and continuations of b under a new batch, with the batches nested
// in it, seals them and returns the id of b's batch. With a store that implements
// [driver.Transactor] this happens in one transaction; with other stores, a failure midway
// deletes the jobs already inserted and seals the batches already opened. A job whose [Unique]
// key is held stays out of the batch. A batch with no jobs and no nested batches finishes as it is
// sealed, so its continuations run right away. A batch nested in itself or in two places is
// [ErrInvalid], and a [Batch.Parent] that does not exist or has finished is [ErrNotFound] or
// [ErrClosed].
func (c *Client) StartBatch(ctx context.Context, b *Batch) (int64, error) {
	return c.begin(ctx, b, true)
}

// OpenBatch inserts b as [Client.StartBatch] does but leaves its batch open, for a batch built
// over several calls: jobs join it later with the [InBatch] option and batches with
// [Batch.Parent], and it cannot finish before [Client.SealBatch]. The batches nested in b are
// sealed as usual.
func (c *Client) OpenBatch(ctx context.Context, b *Batch) (int64, error) {
	return c.begin(ctx, b, false)
}

func (c *Client) begin(ctx context.Context, b *Batch, seal bool) (int64, error) {
	t, err := b.tree(make(map[*Batch]bool))
	if err != nil {
		return 0, err
	}
	tx, ok := c.store.(driver.Transactor)
	if !ok {
		var p progress
		id, err := c.startBatch(ctx, c.store, t, b.Parent, &p, seal)
		if err != nil {
			c.discardBatch(ctx, &p)
			return 0, err
		}
		return id, nil
	}
	var id int64
	err = tx.InTx(ctx, func(w driver.Writer) error {
		id, err = c.startBatch(ctx, w, t, b.Parent, &progress{}, seal)
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
	t, err := b.tree(make(map[*Batch]bool))
	if err != nil {
		return 0, err
	}
	return c.startBatch(ctx, w, t, b.Parent, &progress{}, true)
}

// SealBatch marks the batch complete, so that it finishes once every job in it has succeeded or
// been deleted and every batch nested in it has finished. Jobs can still join it with [InBatch]
// until then. Sealing twice is not an error; an unknown id is [ErrNotFound].
func (c *Client) SealBatch(ctx context.Context, id int64) error {
	return c.store.SealBatch(ctx, id)
}

type batchTree struct {
	batch  *Batch
	jobs   []driver.InsertParams
	then   []driver.InsertParams
	nested []*batchTree
}

type progress struct {
	batches []int64
	jobs    []int64
}

func (b *Batch) tree(seen map[*Batch]bool) (*batchTree, error) {
	if seen[b] {
		return nil, fmt.Errorf("%w: batch nested in itself or in two places", ErrInvalid)
	}
	seen[b] = true
	jobs, then, err := b.params()
	if err != nil {
		return nil, err
	}
	t := &batchTree{batch: b, jobs: jobs, then: then}
	for _, child := range b.nested {
		n, err := child.tree(seen)
		if err != nil {
			return nil, err
		}
		t.nested = append(t.nested, n)
	}
	return t, nil
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

func (c *Client) startBatch(ctx context.Context, w driver.Writer, t *batchTree, parent int64, p *progress, seal bool) (int64, error) {
	b := t.batch
	id, err := w.OpenBatch(ctx, driver.NewBatch{Description: b.Description, Meta: b.Meta, Parent: parent})
	if err != nil {
		return 0, err
	}
	p.batches = append(p.batches, id)
	for i := range t.jobs {
		t.jobs[i].BatchID = id
	}
	if err := c.insertBatch(ctx, w, t.jobs, p); err != nil {
		return 0, err
	}
	for _, n := range t.nested {
		if _, err := c.startBatch(ctx, w, n, id, p, true); err != nil {
			return 0, err
		}
	}
	for i := range t.then {
		t.then[i].AfterBatch = id
		if parent != 0 {
			t.then[i].BatchID = parent
		}
	}
	if err := c.insertBatch(ctx, w, t.then, p); err != nil {
		return 0, err
	}
	if !seal {
		return id, nil
	}
	return id, w.SealBatch(ctx, id)
}

func (c *Client) insertBatch(ctx context.Context, w driver.Writer, ps []driver.InsertParams, p *progress) error {
	if len(ps) == 0 {
		return nil
	}
	res, err := c.insertAll(ctx, w, ps)
	if err != nil {
		return err
	}
	for _, r := range res {
		if !r.Duplicate {
			p.jobs = append(p.jobs, r.ID)
		}
	}
	return nil
}

func (c *Client) discardBatch(ctx context.Context, p *progress) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if len(p.jobs) > 0 {
		c.store.Delete(ctx, driver.Filter{IDs: p.jobs})
	}
	for _, id := range p.batches {
		c.store.SealBatch(ctx, id)
	}
}

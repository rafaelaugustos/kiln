package kiln

import (
	"context"
	"fmt"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

type Batch struct {
	Description string
	Meta        map[string]string

	jobs []Spec
	then []Spec
}

func (b *Batch) Add(args Args, opts ...InsertOption) {
	b.jobs = append(b.jobs, Spec{Args: args, Options: opts})
}

func (b *Batch) Then(args Args, opts ...InsertOption) {
	b.then = append(b.then, Spec{Args: args, Options: opts})
}

func (b *Batch) Len() int {
	return len(b.jobs)
}

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

func (c *Client) OpenBatch(ctx context.Context, description string, meta map[string]string) (int64, error) {
	if err := checkMeta(meta); err != nil {
		return 0, err
	}
	return c.store.OpenBatch(ctx, driver.NewBatch{Description: description, Meta: meta})
}

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

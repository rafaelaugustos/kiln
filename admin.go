package kiln

import (
	"context"
	"fmt"

	"github.com/rafaelaugustos/kiln/driver"
)

func (c *Client) Delete(ctx context.Context, ids ...int64) (int, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	return c.store.Delete(ctx, driver.Filter{IDs: ids})
}

func (c *Client) Requeue(ctx context.Context, ids ...int64) (int, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	return c.store.Requeue(ctx, driver.Filter{IDs: ids})
}

func (c *Client) DeleteWhere(ctx context.Context, f Filter) (int, error) {
	if err := checkFilter(f); err != nil {
		return 0, err
	}
	return c.store.Delete(ctx, f)
}

func (c *Client) RequeueWhere(ctx context.Context, f Filter) (int, error) {
	if err := checkFilter(f); err != nil {
		return 0, err
	}
	return c.store.Requeue(ctx, f)
}

func (c *Client) PauseQueue(ctx context.Context, name string) error {
	return c.pause(ctx, name, true)
}

func (c *Client) ResumeQueue(ctx context.Context, name string) error {
	return c.pause(ctx, name, false)
}

func (c *Client) pause(ctx context.Context, name string, paused bool) error {
	if !validQueue(name) {
		return fmt.Errorf("%w: queue %q", ErrInvalid, name)
	}
	return c.store.PauseQueue(ctx, name, paused)
}

func (c *Client) Get(ctx context.Context, id int64) (Record, error) {
	return c.store.Job(ctx, id)
}

func (c *Client) List(ctx context.Context, q JobQuery) (Page, error) {
	if !q.State.Valid() {
		return Page{}, fmt.Errorf("%w: state %q", ErrInvalid, q.State)
	}
	return c.store.Jobs(ctx, q)
}

func checkFilter(f Filter) error {
	if len(f.IDs) == 0 && f.State == "" {
		return fmt.Errorf("%w: filter needs ids or a state", ErrInvalid)
	}
	if f.State != "" && !f.State.Valid() {
		return fmt.Errorf("%w: state %q", ErrInvalid, f.State)
	}
	return nil
}

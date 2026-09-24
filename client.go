package kiln

import (
	"context"
	"fmt"
	"slices"

	"github.com/rafaelaugustos/kiln/driver"
)

type EnqueueFunc func(ctx context.Context, w driver.Writer, jobs []driver.InsertParams) ([]driver.Inserted, error)

type EnqueueMiddleware func(next EnqueueFunc) EnqueueFunc

type Client struct {
	store   driver.Store
	enqueue EnqueueFunc
}

func NewClient(s driver.Store, mw ...EnqueueMiddleware) *Client {
	f := EnqueueFunc(insert)
	for _, m := range slices.Backward(mw) {
		f = m(f)
	}
	return &Client{store: s, enqueue: f}
}

func insert(ctx context.Context, w driver.Writer, jobs []driver.InsertParams) ([]driver.Inserted, error) {
	return w.Insert(ctx, jobs)
}

func (c *Client) Store() driver.Store {
	return c.store
}

func (c *Client) Enqueue(ctx context.Context, args Args, opts ...InsertOption) (int64, error) {
	return c.enqueueOne(ctx, c.store, args, opts)
}

func (c *Client) EnqueueTx(ctx context.Context, w driver.Writer, args Args, opts ...InsertOption) (int64, error) {
	if w == nil {
		return 0, driver.ErrNilTx
	}
	return c.enqueueOne(ctx, w, args, opts)
}

func (c *Client) EnqueueMany(ctx context.Context, specs ...Spec) ([]Inserted, error) {
	return c.enqueueMany(ctx, c.store, specs)
}

func (c *Client) EnqueueManyTx(ctx context.Context, w driver.Writer, specs ...Spec) ([]Inserted, error) {
	if w == nil {
		return nil, driver.ErrNilTx
	}
	return c.enqueueMany(ctx, w, specs)
}

func (c *Client) enqueueOne(ctx context.Context, w driver.Writer, args Args, opts []InsertOption) (int64, error) {
	p, err := buildParams(args, opts)
	if err != nil {
		return 0, err
	}
	for _, parent := range p.Parents {
		if parent.ID == 0 {
			return 0, fmt.Errorf("%w: Needs is only valid in EnqueueMany", ErrInvalid)
		}
	}
	res, err := c.enqueue(ctx, w, []driver.InsertParams{p})
	if err != nil {
		return 0, err
	}
	if len(res) != 1 {
		return 0, fmt.Errorf("kiln: store returned %d results for 1 job", len(res))
	}
	return res[0].ID, nil
}

func (c *Client) enqueueMany(ctx context.Context, w driver.Writer, specs []Spec) ([]Inserted, error) {
	if len(specs) == 0 {
		return nil, nil
	}
	ps, err := buildAll(specs)
	if err != nil {
		return nil, err
	}
	return c.insertAll(ctx, w, ps)
}

func (c *Client) insertAll(ctx context.Context, w driver.Writer, ps []driver.InsertParams) ([]Inserted, error) {
	res, err := c.enqueue(ctx, w, ps)
	if err != nil {
		return nil, err
	}
	if len(res) != len(ps) {
		return nil, fmt.Errorf("kiln: store returned %d results for %d jobs", len(res), len(ps))
	}
	return res, nil
}

func buildAll(specs []Spec) ([]driver.InsertParams, error) {
	ps := make([]driver.InsertParams, len(specs))
	for i, s := range specs {
		p, err := buildParams(s.Args, s.Options)
		if err != nil {
			return nil, fmt.Errorf("job %d: %w", i, err)
		}
		ps[i] = p
	}
	if err := driver.CheckInsert(ps); err != nil {
		return nil, err
	}
	return ps, nil
}

type Flow []Spec

func (f *Flow) Add(args Args, opts ...InsertOption) int {
	*f = append(*f, Spec{Args: args, Options: opts})
	return len(*f) - 1
}

package kiln

import (
	"context"
	"fmt"

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
	for i := len(mw) - 1; i >= 0; i-- {
		f = mw[i](f)
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
	if err := checkRefs(ps); err != nil {
		return nil, err
	}
	return ps, nil
}

func checkRefs(ps []driver.InsertParams) error {
	refs := false
	for i, p := range ps {
		for _, parent := range p.Parents {
			if parent.ID != 0 {
				continue
			}
			if parent.Index < 0 || parent.Index >= len(ps) || parent.Index == i {
				return fmt.Errorf("%w: job %d needs %d", ErrInvalid, i, parent.Index)
			}
			refs = true
		}
	}
	if !refs {
		return nil
	}
	const (
		unseen = iota
		visiting
		done
	)
	marks := make([]uint8, len(ps))
	var visit func(int) bool
	visit = func(i int) bool {
		switch marks[i] {
		case visiting:
			return false
		case done:
			return true
		}
		marks[i] = visiting
		for _, parent := range ps[i].Parents {
			if parent.ID == 0 && !visit(parent.Index) {
				return false
			}
		}
		marks[i] = done
		return true
	}
	for i := range ps {
		if !visit(i) {
			return fmt.Errorf("%w: dependency cycle through job %d", ErrInvalid, i)
		}
	}
	return nil
}

type Flow []Spec

func (f *Flow) Add(args Args, opts ...InsertOption) int {
	*f = append(*f, Spec{Args: args, Options: opts})
	return len(*f) - 1
}

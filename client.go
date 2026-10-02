package kiln

import (
	"context"
	"fmt"
	"slices"

	"github.com/rafaelaugustos/kiln/driver"
)

// EnqueueFunc inserts jobs through w, a store or a writer bound to a transaction, and returns
// one result per job. It is what an [EnqueueMiddleware] wraps.
type EnqueueFunc func(ctx context.Context, w driver.Writer, jobs []driver.InsertParams) ([]driver.Inserted, error)

// EnqueueMiddleware wraps the inserts of a [Client]: the jobs of Enqueue, EnqueueMany, batches and
// TriggerRecurring, with or without a transaction. It may change the jobs before passing them to
// next, for example to add meta. Jobs fired on a recurring schedule are inserted by the leader and
// do not pass through it.
type EnqueueMiddleware func(next EnqueueFunc) EnqueueFunc

// Client inserts and manages jobs in a store. It is safe for concurrent use.
type Client struct {
	store   driver.Store
	enqueue EnqueueFunc
}

// NewClient returns a client for s. Every insert goes through mw, the first middleware being the
// outermost.
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

// Store returns the store the client works on, for the operations the client does not cover.
func (c *Client) Store() driver.Store {
	return c.store
}

// Enqueue inserts a job and returns its id. When a [Unique] key is held by another job, nothing is
// inserted and the id returned is the holder's; [Client.EnqueueMany] tells the two cases apart.
// Enqueue fails with [ErrInvalid] or [ErrTooLarge] for args or options that cannot be stored, and
// with [ErrNotFound] when a parent job or a batch it names does not exist.
func (c *Client) Enqueue(ctx context.Context, args Args, opts ...InsertOption) (int64, error) {
	return c.enqueueOne(ctx, c.store, args, opts)
}

// EnqueueTx is [Client.Enqueue] inside the application's transaction: w is a [driver.Writer]
// bound to it, such as the TxWriter of pgstore, mysqlstore or sqlitestore. The job exists only if
// the transaction commits. Servers find it at their next poll, or immediately if the writer's
// Notify method is called after the commit. EnqueueTx returns [driver.ErrNilTx] when w is nil.
func (c *Client) EnqueueTx(ctx context.Context, w driver.Writer, args Args, opts ...InsertOption) (int64, error) {
	if w == nil {
		return 0, driver.ErrNilTx
	}
	return c.enqueueOne(ctx, w, args, opts)
}

// EnqueueMany inserts several jobs in one call and returns a result for each spec, in order. The
// call is atomic: after an error, none of the jobs exists. The exception is a job whose [Unique]
// key is held, which is reported as a duplicate, with the holder's id, while the others are
// inserted. Jobs can depend on each other with [Needs]; see [Flow]. With no specs, EnqueueMany
// does nothing.
func (c *Client) EnqueueMany(ctx context.Context, specs ...Spec) ([]Inserted, error) {
	return c.enqueueMany(ctx, c.store, specs)
}

// EnqueueManyTx is [Client.EnqueueMany] inside the application's transaction, through w; see
// [Client.EnqueueTx].
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

// Flow collects jobs for [Client.EnqueueMany] and hands out their positions, so that jobs
// inserted together can wait for each other with [Needs].
type Flow []Spec

// Add appends a job to the flow and returns its index, to use in [Needs].
func (f *Flow) Add(args Args, opts ...InsertOption) int {
	*f = append(*f, Spec{Args: args, Options: opts})
	return len(*f) - 1
}

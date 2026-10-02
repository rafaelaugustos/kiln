package sqlitestore

import (
	"context"
	"fmt"
	"slices"

	"github.com/rafaelaugustos/kiln/driver"
)

// TxWriter is a [driver.TxWriter] bound to an application's transaction, made by [Store.Tx]. Its
// writes, the admission of throttled jobs included, happen inside the transaction, each under a
// savepoint, so one that fails is undone and leaves the transaction usable, unless SQLite has
// rolled back the whole transaction; every later write then fails. Call [TxWriter.Notify] after
// the commit to wake the servers. A TxWriter is not safe for concurrent use.
type TxWriter struct {
	s      *Store
	q      querier
	queues []string
	lost   error
}

// Insert inserts jobs inside the transaction, as [driver.Writer.Insert] describes, and admits the
// throttled jobs of the limit keys they use, also inside the transaction.
func (w *TxWriter) Insert(ctx context.Context, jobs []driver.InsertParams) ([]driver.Inserted, error) {
	if len(jobs) == 0 {
		return nil, nil
	}
	if err := driver.CheckInsert(jobs); err != nil {
		return nil, err
	}
	in := w.s.plan(jobs)
	if err := w.atomic(ctx, func() error { return in.write(ctx, w.q) }); err != nil {
		return nil, wrap("insert", err)
	}
	w.queues = merge(w.queues, in.f.queues)
	return in.res, nil
}

// OpenBatch creates an unsealed batch inside the transaction and returns its id.
func (w *TxWriter) OpenBatch(ctx context.Context, nb driver.NewBatch) (int64, error) {
	var id int64
	err := w.atomic(ctx, func() (err error) {
		id, err = w.s.openBatch(ctx, w.q, nb)
		return err
	})
	if err != nil {
		return 0, wrap("open batch", err)
	}
	return id, nil
}

// SealBatch seals the batch id inside the transaction and, when none of its members is live,
// finishes it and releases the jobs that wait for it. It fails with [driver.ErrNotFound] when
// there is no such batch.
func (w *TxWriter) SealBatch(ctx context.Context, id int64) error {
	var f *fallout
	err := w.atomic(ctx, func() (err error) {
		f, err = w.s.seal(ctx, w.q, id)
		return err
	})
	if err != nil {
		return wrap("seal batch", err)
	}
	w.queues = merge(w.queues, f.queues)
	return nil
}

// Notify tells the servers subscribed to the Store, and the bus when the store has one, which
// queues received jobs to run. Call it after the transaction commits. Its error, which only the
// bus can cause, is advisory: the jobs are committed either way, and without Notify servers find
// them at their next poll. Calling it twice, or after a rollback, does no harm, but a second call
// does nothing, even after the first failed.
func (w *TxWriter) Notify(ctx context.Context) error {
	queues := w.queues
	w.queues = nil
	return w.s.hub.notify(ctx, queues)
}

func (w *TxWriter) atomic(ctx context.Context, fn func() error) error {
	switch {
	case w.q == nil:
		return driver.ErrNilTx
	case w.lost != nil:
		return w.lost
	}
	if _, err := w.q.ExecContext(ctx, "SAVEPOINT kiln"); err != nil {
		return err
	}
	bg := context.WithoutCancel(ctx)
	err := fn()
	if err != nil {
		if _, rerr := w.q.ExecContext(bg, "ROLLBACK TO kiln"); rerr != nil {
			w.lost = fmt.Errorf("transaction rolled back: %w", err)
			return w.lost
		}
	}
	if _, rerr := w.q.ExecContext(bg, "RELEASE kiln"); rerr != nil {
		w.lost = fmt.Errorf("release savepoint: %w", rerr)
		return w.lost
	}
	return err
}

// InTx runs fn in a transaction begun with BEGIN IMMEDIATE on the connection kept for writes, and
// commits it if fn returns nil; otherwise it rolls back and returns fn's error. Every other write
// on the file waits until it ends, so fn must write only through the Writer it is given: a write
// method of the Store called from fn would wait for InTx, and so for fn, until its ctx is done.
// After the commit InTx tells the servers which queues received jobs, publishing to the bus in the
// background.
func (s *Store) InTx(ctx context.Context, fn func(w driver.Writer) error) error {
	var w *TxWriter
	err := s.write(ctx, func(_ context.Context, q querier) error {
		w = &TxWriter{s: s, q: q}
		return fn(w)
	})
	if err != nil {
		return err
	}
	s.hub.ready(w.queues)
	return nil
}

func merge(dst, src []string) []string {
	for _, k := range src {
		if !slices.Contains(dst, k) {
			dst = append(dst, k)
		}
	}
	return dst
}

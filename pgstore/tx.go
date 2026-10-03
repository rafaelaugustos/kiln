package pgstore

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/rafaelaugustos/kiln/driver"
)

// TxWriter is a [driver.TxWriter] bound to an application's transaction, made by [Store.Tx] or
// [Store.SQLTx]. Its writes neither admit throttled jobs nor send NOTIFY from inside the
// transaction; after the commit, [TxWriter.Notify] admits the throttled jobs of the limit keys its
// inserts used or its seals released jobs under, and sends the NOTIFY. Keep the transaction
// short: until it ends, its row locks make other work wait, such as inserts with the same unique
// keys, into the same batches or under a limit key it created, and the jobs its inserts name as
// parents can be neither claimed nor finished. A write that fails in the database, such as an
// insert naming a parent that does not exist, aborts the transaction, as any failed statement does
// in PostgreSQL. A TxWriter is not safe for concurrent use.
type TxWriter struct {
	s  *Store
	tx sender
	w  wake
}

// Insert inserts jobs inside the transaction, as [driver.Writer.Insert] describes, leaving the
// admission of throttled jobs to [TxWriter.Notify].
func (w *TxWriter) Insert(ctx context.Context, jobs []driver.InsertParams) ([]driver.Inserted, error) {
	if w.tx == nil {
		return nil, driver.ErrNilTx
	}
	res, wk, err := w.s.insert(ctx, w.tx, jobs)
	if err != nil {
		return nil, err
	}
	w.w.merge(wk)
	return res, nil
}

// OpenBatch creates an unsealed batch inside the transaction and returns its id.
func (w *TxWriter) OpenBatch(ctx context.Context, nb driver.NewBatch) (int64, error) {
	if w.tx == nil {
		return 0, driver.ErrNilTx
	}
	return w.s.openBatch(ctx, w.tx, nb)
}

// SealBatch seals the batch id inside the transaction and finishes it when none of its members is
// live, leaving the admission of the jobs it releases to [TxWriter.Notify]. It fails with
// [driver.ErrNotFound] when there is no such batch.
func (w *TxWriter) SealBatch(ctx context.Context, id int64) error {
	if w.tx == nil {
		return driver.ErrNilTx
	}
	wk, err := w.s.sealBatch(ctx, w.tx, id)
	if err != nil {
		return err
	}
	w.w.merge(wk)
	return nil
}

// Notify admits the throttled jobs of the limit keys the transaction's inserts used or its seals
// released jobs under, and sends a NOTIFY for the queues that received jobs to run, so that
// servers start them at once. Call it after the transaction commits. Its error is advisory: the
// jobs are committed either way, and without Notify they wait for the next sweep and the servers'
// next poll. Calling it twice, or after a rollback, does no harm, but a second call does nothing,
// even after the first failed.
func (w *TxWriter) Notify(ctx context.Context) error {
	wk := w.w
	w.w = wake{}
	if err := w.s.admitLate(ctx, &wk); err != nil {
		return err
	}
	if len(wk.queues) == 0 {
		return nil
	}
	if err := w.s.notifyNow(ctx, w.s.channel("jobs"), wk.queues); err != nil {
		return fmt.Errorf("kiln: notify: %w", err)
	}
	return nil
}

// InTx runs fn in a transaction on the store's pool and commits it if fn returns nil; otherwise
// it rolls back and returns fn's error. After the commit it admits the throttled jobs of the limit
// keys fn's inserts used or its seals released jobs under, and hands the queues that received jobs
// to the store's notifier, which sends NOTIFY from its own goroutine; neither can fail the call.
func (s *Store) InTx(ctx context.Context, fn func(w driver.Writer) error) error {
	var w *TxWriter
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		w = s.Tx(tx)
		return fn(w)
	})
	if err != nil {
		return err
	}
	s.admitLate(ctx, &w.w)
	s.nt.jobs(w.w.queues...)
	return nil
}

package mssqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

const tries = 5

var readCommitted = &sql.TxOptions{Isolation: sql.LevelReadCommitted}

type querier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func (s *Store) txn(ctx context.Context, fn func(tx *sql.Tx) error) error {
	return s.retry(ctx, func() error { return s.once(ctx, fn) })
}

func (s *Store) retry(ctx context.Context, fn func() error) error {
	for try := 1; ; try++ {
		err := fn()
		if err == nil || try == tries || !retryable(err) {
			return err
		}
		if !pause(ctx, time.Duration(try*try)*time.Millisecond+rand.N(2*time.Millisecond)) {
			return err
		}
	}
}

func pause(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func (s *Store) once(ctx context.Context, fn func(tx *sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, readCommitted)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

// TxWriter is a [driver.TxWriter] bound to an application's transaction, made by [Store.Tx]. Its
// writes neither admit throttled jobs nor publish events from inside the transaction; after the
// commit, [TxWriter.Notify] admits the throttled jobs of the limit keys its inserts used or its
// seals released jobs under, and publishes the events. Each write runs under a savepoint, so one
// that fails is undone and leaves the transaction usable, unless SQL Server has rolled back the
// whole transaction, as it does to a deadlock victim; every later write then fails. A TxWriter is
// not safe for concurrent use.
type TxWriter struct {
	s    *Store
	tx   *sql.Tx
	wake wake
	lost error
}

// Insert inserts jobs inside the transaction, as [driver.Writer.Insert] describes, leaving the
// admission of throttled jobs to [TxWriter.Notify]. When another transaction takes the same new
// unique key at the same moment, Insert undoes its work and tries again, up to three times.
func (w *TxWriter) Insert(ctx context.Context, jobs []driver.InsertParams) ([]driver.Inserted, error) {
	var (
		res []driver.Inserted
		wk  wake
		err error
	)
	for try := 1; ; try++ {
		err = w.atomic(ctx, func() (err error) {
			res, wk, err = w.s.insert(ctx, w.tx, jobs)
			return err
		})
		if try == 3 || !duplicate(err) {
			break
		}
	}
	if err != nil {
		return nil, err
	}
	w.wake.merge(wk)
	return res, nil
}

// OpenBatch creates an unsealed batch inside the transaction and returns its id.
func (w *TxWriter) OpenBatch(ctx context.Context, nb driver.NewBatch) (int64, error) {
	var id int64
	err := w.atomic(ctx, func() (err error) {
		id, err = w.s.openBatch(ctx, w.tx, nb)
		return err
	})
	return id, err
}

// SealBatch seals the batch id inside the transaction, or fails with [driver.ErrNotFound], leaving
// the admission of the jobs it releases to [TxWriter.Notify].
func (w *TxWriter) SealBatch(ctx context.Context, id int64) error {
	var f *fallout
	err := w.atomic(ctx, func() (err error) {
		f, err = w.s.seal(ctx, w.tx, id)
		return err
	})
	if err != nil {
		return err
	}
	w.wake.queues = merge(w.wake.queues, f.queues...)
	w.wake.hold(f.throttle)
	return nil
}

// Notify admits the throttled jobs of the limit keys the transaction's inserts used or its seals
// released jobs under, in a transaction of its own, and, when the store has a bus, publishes the
// queues that received jobs to run. Call it after the transaction commits. Its error is advisory:
// the jobs are committed either way, and without Notify they wait for the next sweep and the
// servers' next poll. Calling it twice, or after a rollback, does no harm, but a second call does
// nothing, even after the first failed.
func (w *TxWriter) Notify(ctx context.Context) error {
	wk := w.wake
	w.wake = wake{}
	err := w.s.admitLate(ctx, &wk)
	return errors.Join(err, w.s.publish(ctx, wk.queues))
}

func (w *TxWriter) atomic(ctx context.Context, fn func() error) error {
	switch {
	case w.tx == nil:
		return driver.ErrNilTx
	case w.lost != nil:
		return w.lost
	}
	if _, err := w.tx.ExecContext(ctx, "SAVE TRANSACTION kiln"); err != nil {
		w.lost = fmt.Errorf("kiln: the transaction is gone: %w", err)
		return w.lost
	}
	err := fn()
	if err == nil {
		return nil
	}
	if _, rerr := w.tx.ExecContext(context.WithoutCancel(ctx), "ROLLBACK TRANSACTION kiln"); rerr != nil {
		w.lost = fmt.Errorf("kiln: the transaction was rolled back: %w", err)
		return w.lost
	}
	return err
}

// InTx runs fn in a READ COMMITTED transaction and commits it if fn returns nil; otherwise it
// rolls back and returns fn's error. A transaction chosen as a deadlock victim is retried with a
// new call to fn, up to five attempts in all, so fn must be safe to repeat. After the commit InTx
// admits the throttled jobs of the limit keys fn's inserts used or its seals released jobs under,
// and publishes events in the background; neither can fail the call.
func (s *Store) InTx(ctx context.Context, fn func(w driver.Writer) error) error {
	var w *TxWriter
	err := s.txn(ctx, func(tx *sql.Tx) error {
		w = &TxWriter{s: s, tx: tx}
		return fn(w)
	})
	if err != nil {
		return err
	}
	s.admitLate(ctx, &w.wake)
	s.nt.ready(w.wake.queues)
	return nil
}

func merge(dst []string, src ...string) []string {
	for _, k := range src {
		if !slices.Contains(dst, k) {
			dst = append(dst, k)
		}
	}
	return dst
}

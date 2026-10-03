package mysqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/rafaelaugustos/kiln/driver"
)

const sqlOpenBatch = `INSERT INTO {p}batches (description, meta, parent_id, created_at) VALUES (?, ?, ?, UTC_TIMESTAMP(6))`

const sqlLockBatch = `SELECT b.sealed, b.total = 0 OR b.total <=> (SELECT x.total FROM {p}batches x WHERE x.id = b.id),
	UTC_TIMESTAMP(6)
FROM {p}batches b WHERE b.id = ? FOR UPDATE`

const sqlSeal = `UPDATE {p}batches SET sealed = TRUE WHERE id = ?`

// OpenBatch creates an unsealed batch and returns its id. With nb.Parent set it nests the batch in
// that one, in a transaction that locks the parent's row for the check that it can take members,
// and fails with [driver.ErrNotFound] or [driver.ErrClosed] as [driver.Batch] describes.
func (s *Store) OpenBatch(ctx context.Context, nb driver.NewBatch) (int64, error) {
	if nb.Parent <= 0 {
		return s.openBatch(ctx, s.db, nb)
	}
	var id int64
	err := s.txn(ctx, func(tx *sql.Tx) (err error) {
		id, err = s.openBatch(ctx, tx, nb)
		return err
	})
	return id, err
}

func (s *Store) openBatch(ctx context.Context, q querier, nb driver.NewBatch) (int64, error) {
	var parent any
	if nb.Parent > 0 {
		var (
			id   int64
			open bool
		)
		err := q.QueryRowContext(ctx, render(s.q.lockBatches, []int64{nb.Parent})).Scan(&id, &open)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			return 0, fmt.Errorf("%w: batch %d", driver.ErrNotFound, nb.Parent)
		case err != nil:
			return 0, wrap("open batch", err)
		case !open:
			return 0, fmt.Errorf("%w: batch %d", driver.ErrClosed, nb.Parent)
		}
		parent = id
	}
	r, err := q.ExecContext(ctx, render(s.q.openBatch, nb.Description, encodeMeta(nb.Meta), parent))
	if err != nil {
		return 0, wrap("open batch", err)
	}
	id, err := r.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("kiln: open batch: %w", err)
	}
	return id, nil
}

// SealBatch seals the batch id and, when none of its members is live and its nested batches have
// finished, finishes it, with the batches it is nested in that can finish in turn, and releases the
// jobs that wait for them, in one transaction. It fails with [driver.ErrNotFound] for an unknown id.
func (s *Store) SealBatch(ctx context.Context, id int64) error {
	var queues []string
	err := s.txn(ctx, func(tx *sql.Tx) error {
		queues = nil
		f, err := s.seal(ctx, tx, id)
		if err != nil {
			return err
		}
		if len(f.throttle) > 0 {
			slots, err := s.lockLimits(ctx, tx, f.throttle, true)
			if err != nil {
				return wrap("seal batch", err)
			}
			a, err := s.fill(ctx, tx, slots)
			if err != nil {
				return wrap("seal batch", err)
			}
			f.queue(a.queues...)
		}
		queues = f.queues
		return nil
	})
	if err != nil {
		return err
	}
	s.nt.ready(queues)
	return nil
}

func (s *Store) seal(ctx context.Context, q querier, id int64) (*fallout, error) {
	var (
		sealed, current bool
		now             stamp
	)
	err := q.QueryRowContext(ctx, render(s.q.lockBatch, id)).Scan(&sealed, &current, &now)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, fmt.Errorf("%w: batch %d", driver.ErrNotFound, id)
	case err != nil:
		return nil, wrap("seal batch", err)
	}
	if !sealed {
		if _, err := q.ExecContext(ctx, render(s.q.seal, id)); err != nil {
			return nil, wrap("seal batch", err)
		}
	}
	f := &fallout{now: now.Time}
	if !current {
		return f, nil
	}
	f.batches = []int64{id}
	if err := s.complete(ctx, q, f); err != nil {
		return nil, wrap("seal batch", err)
	}
	return f, nil
}

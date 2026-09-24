package mysqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/rafaelaugustos/kiln/driver"
)

const sqlOpenBatch = `INSERT INTO {p}batches (description, meta, created_at) VALUES (?, ?, UTC_TIMESTAMP(6))`

const sqlLockBatch = `SELECT b.sealed, b.total = 0 OR b.total <=> (SELECT x.total FROM {p}batches x WHERE x.id = b.id),
	UTC_TIMESTAMP(6)
FROM {p}batches b WHERE b.id = ? FOR UPDATE`

const sqlSeal = `UPDATE {p}batches SET sealed = TRUE WHERE id = ?`

func (s *Store) OpenBatch(ctx context.Context, nb driver.NewBatch) (int64, error) {
	return s.openBatch(ctx, s.db, nb)
}

func (s *Store) openBatch(ctx context.Context, q querier, nb driver.NewBatch) (int64, error) {
	r, err := q.ExecContext(ctx, render(s.q.openBatch, nb.Description, encodeMeta(nb.Meta)))
	if err != nil {
		return 0, wrap("open batch", err)
	}
	id, err := r.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("kiln: open batch: %w", err)
	}
	return id, nil
}

func (s *Store) SealBatch(ctx context.Context, id int64) error {
	return s.txn(ctx, func(tx *sql.Tx) error {
		f, err := s.seal(ctx, tx, id)
		if err != nil || len(f.throttle) == 0 {
			return err
		}
		slots, err := s.lockLimits(ctx, tx, f.throttle, true)
		if err != nil {
			return wrap("seal batch", err)
		}
		if _, err := s.fill(ctx, tx, slots); err != nil {
			return wrap("seal batch", err)
		}
		return nil
	})
}

func (s *Store) sealBatch(ctx context.Context, tx *sql.Tx, id int64) error {
	_, err := s.seal(ctx, tx, id)
	return err
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

package mssqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/rafaelaugustos/kiln/driver"
)

const sqlOpenBatch = `INSERT INTO {p}batches (description, meta, created_at) OUTPUT inserted.id
VALUES (@description, @meta, SYSUTCDATETIME())`

const sqlLockBatch = `SELECT b.sealed, CASE WHEN b.total = 0 OR b.total = (SELECT x.total FROM {p}batches x WHERE x.id = b.id)
	THEN 1 ELSE 0 END, CAST(SYSUTCDATETIME() AS DATETIME2(6))
FROM {p}batches b WITH (UPDLOCK, ROWLOCK) WHERE b.id = @id`

const sqlSeal = `UPDATE {p}batches SET sealed = 1 WHERE id = @id`

// OpenBatch creates an unsealed batch and returns its id.
func (s *Store) OpenBatch(ctx context.Context, nb driver.NewBatch) (int64, error) {
	return s.openBatch(ctx, s.db, nb)
}

func (s *Store) openBatch(ctx context.Context, q querier, nb driver.NewBatch) (int64, error) {
	var id int64
	err := q.QueryRowContext(ctx, s.q.openBatch, sql.Named("description", nb.Description),
		sql.Named("meta", text(encodeMeta(nb.Meta)))).Scan(&id)
	if err != nil {
		return 0, wrap("open batch", err)
	}
	return id, nil
}

// SealBatch seals the batch id and, when none of its members is live, finishes it and releases the
// jobs that wait for it, in one transaction. It fails with [driver.ErrNotFound] for an unknown id.
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
		now             moment
	)
	err := q.QueryRowContext(ctx, s.q.lockBatch, sql.Named("id", id)).Scan(&sealed, &current, &now)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, fmt.Errorf("%w: batch %d", driver.ErrNotFound, id)
	case err != nil:
		return nil, wrap("seal batch", err)
	}
	if !sealed {
		if _, err := q.ExecContext(ctx, s.q.seal, sql.Named("id", id)); err != nil {
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

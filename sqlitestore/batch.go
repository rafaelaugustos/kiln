package sqlitestore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/rafaelaugustos/kiln/driver"
)

const sqlOpenBatch = `INSERT INTO {p}batches (description, meta, created_at) VALUES (?, ?, {now}) RETURNING id`

const sqlLockBatch = `SELECT sealed, finished_at IS NULL, {now} FROM {p}batches WHERE id = ?`

const sqlSeal = `UPDATE {p}batches SET sealed = 1 WHERE id = ?`

func (s *Store) OpenBatch(ctx context.Context, nb driver.NewBatch) (int64, error) {
	var id int64
	err := s.write(ctx, func(ctx context.Context, q querier) (err error) {
		id, err = s.openBatch(ctx, q, nb)
		return err
	})
	if err != nil {
		return 0, wrap("open batch", err)
	}
	return id, nil
}

func (s *Store) openBatch(ctx context.Context, q querier, nb driver.NewBatch) (int64, error) {
	var id int64
	err := q.QueryRowContext(ctx, s.q.openBatch, nb.Description, text(encodeMeta(nb.Meta))).Scan(&id)
	return id, err
}

func (s *Store) SealBatch(ctx context.Context, id int64) error {
	var f *fallout
	err := s.write(ctx, func(ctx context.Context, q querier) (err error) {
		f, err = s.seal(ctx, q, id)
		return err
	})
	if err != nil {
		return wrap("seal batch", err)
	}
	s.hub.ready(f.queues)
	return nil
}

func (s *Store) seal(ctx context.Context, q querier, id int64) (*fallout, error) {
	var (
		sealed, open bool
		now          int64
	)
	err := q.QueryRowContext(ctx, s.q.lockBatch, id).Scan(&sealed, &open, &now)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, fmt.Errorf("%w: batch %d", driver.ErrNotFound, id)
	case err != nil:
		return nil, err
	}
	if !sealed {
		if _, err := q.ExecContext(ctx, s.q.seal, id); err != nil {
			return nil, err
		}
	}
	f := &fallout{now: now}
	if open {
		f.batch(id)
		if err := s.settle(ctx, q, f); err != nil {
			return nil, err
		}
	}
	return f, nil
}

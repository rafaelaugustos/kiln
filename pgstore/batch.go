package pgstore

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/rafaelaugustos/kiln/driver"
)

const sqlOpenBatch = `INSERT INTO {s}.batches (description, meta) VALUES ($1, nullif($2, '')::jsonb) RETURNING id`

const sqlSeal = `UPDATE {s}.batches SET sealed = true WHERE id = $1`

type sender interface {
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
}

func (s *Store) OpenBatch(ctx context.Context, nb driver.NewBatch) (int64, error) {
	return s.openBatch(ctx, s.pool, nb)
}

func (s *Store) SealBatch(ctx context.Context, id int64) error {
	w, err := s.sealBatch(ctx, s.pool, id)
	if err != nil {
		return err
	}
	s.nt.jobs(w.queues...)
	return nil
}

func (s *Store) openBatch(ctx context.Context, q sender, nb driver.NewBatch) (int64, error) {
	var id int64
	b := &pgx.Batch{}
	b.Queue(s.q.openBatch, nb.Description, encodeMeta(nb.Meta)).QueryRow(func(row pgx.Row) error {
		return row.Scan(&id)
	})
	if err := q.SendBatch(ctx, b).Close(); err != nil {
		return 0, wrap("open batch", err)
	}
	return id, nil
}

func (s *Store) sealBatch(ctx context.Context, q sender, id int64) (wake, error) {
	var (
		w     wake
		found bool
	)
	b := &pgx.Batch{}
	b.Queue(s.q.seal, id).Exec(func(tag pgconn.CommandTag) error {
		found = tag.RowsAffected() > 0
		return nil
	})
	b.Queue(s.q.complete, []int64{id}).Query(w.scanStates)
	if err := q.SendBatch(ctx, b).Close(); err != nil {
		return wake{}, wrap("seal batch", err)
	}
	if !found {
		return wake{}, fmt.Errorf("%w: batch %d", driver.ErrNotFound, id)
	}
	return w, nil
}

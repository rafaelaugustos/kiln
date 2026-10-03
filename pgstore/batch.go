package pgstore

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/rafaelaugustos/kiln/driver"
)

const sqlOpenBatch = `INSERT INTO {s}.batches (description, meta) VALUES ($1, nullif($2, '')::jsonb) RETURNING id`

const unfinishedNested = `EXISTS (SELECT 1 FROM {s}.batches n WHERE n.parent_id = b.id AND n.finished_at IS NULL)`

const sqlOpenNested = `WITH p AS MATERIALIZED (
	SELECT b.finished_at IS NULL AND (NOT b.sealed OR EXISTS (SELECT 1 FROM {s}.jobs j WHERE j.batch_id = b.id)
		OR ` + unfinishedNested + `) AS open
	FROM {s}.batches b WHERE b.id = $3
	FOR NO KEY UPDATE
)
INSERT INTO {s}.batches (description, meta, parent_id)
SELECT $1, nullif($2, '')::jsonb, $3
WHERE CASE
	WHEN NOT EXISTS (SELECT 1 FROM p) THEN {s}.raise('KL002', 'batch not found')
	WHEN NOT (SELECT open FROM p) THEN {s}.raise('KL001', 'batch is closed')
	ELSE true
END
RETURNING id`

const sqlSeal = `UPDATE {s}.batches SET sealed = true WHERE id = $1`

type sender interface {
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
}

// OpenBatch creates an unsealed batch and returns its id. With nb.Parent set it nests the batch
// in that one, locking the parent's row for the check that it can take members, and fails with
// [driver.ErrNotFound] or [driver.ErrClosed] as [driver.Batch] describes.
func (s *Store) OpenBatch(ctx context.Context, nb driver.NewBatch) (int64, error) {
	return s.openBatch(ctx, s.pool, nb)
}

// SealBatch seals the batch id and, when none of its members is live and its nested batches have
// finished, finishes it, with the batches it is nested in that can finish in turn, and releases
// the jobs that wait for them, in one round trip, and in a second one admits those of them that
// went to throttled. It fails with [driver.ErrNotFound] for an unknown id.
func (s *Store) SealBatch(ctx context.Context, id int64) error {
	w, err := s.sealBatch(ctx, s.pool, id)
	if err != nil {
		return err
	}
	s.admitLate(ctx, &w)
	s.nt.jobs(w.queues...)
	return nil
}

func (s *Store) openBatch(ctx context.Context, q sender, nb driver.NewBatch) (int64, error) {
	var id int64
	scan := func(row pgx.Row) error { return row.Scan(&id) }
	b := &pgx.Batch{}
	if nb.Parent > 0 {
		b.Queue(s.q.openNested, nb.Description, encodeMeta(nb.Meta), nb.Parent).QueryRow(scan)
	} else {
		b.Queue(s.q.openBatch, nb.Description, encodeMeta(nb.Meta)).QueryRow(scan)
	}
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
	b.Queue(s.q.lockAncestors, []int64{id})
	b.Queue(s.q.completeAncestors, []int64{id}).Query(w.scanStates)
	if err := q.SendBatch(ctx, b).Close(); err != nil {
		return wake{}, wrap("seal batch", err)
	}
	if !found {
		return wake{}, fmt.Errorf("%w: batch %d", driver.ErrNotFound, id)
	}
	return w, nil
}

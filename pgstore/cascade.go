package pgstore

import (
	"context"
	"slices"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rafaelaugustos/kiln/driver"
)

func (s *Store) txn(ctx context.Context, fn func(c *pgxpool.Conn) error) error {
	c, err := s.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer c.Release()
	if err := fn(c); err != nil {
		c.Exec(context.WithoutCancel(ctx), "rollback")
		return err
	}
	return nil
}

func (s *Store) cascade(ctx context.Context, c *pgxpool.Conn, b *pgx.Batch, parents, batches []int64, deleted int, w *wake) (int, error) {
	changed, doomed := 0, 0
	if len(parents) > 0 {
		b.Queue(s.q.resolve, parents).Query(func(rows pgx.Rows) error {
			var (
				q, state string
				batch    int64
			)
			for rows.Next() {
				if err := rows.Scan(&q, &state, &batch); err != nil {
					return err
				}
				changed++
				switch driver.State(state) {
				case driver.Deleted:
					doomed++
					if batch != 0 && !slices.Contains(batches, batch) {
						batches = append(batches, batch)
					}
				case driver.Enqueued:
					w.queue(q)
				}
			}
			return rows.Err()
		})
		b.Queue(s.q.admitFinished, parents).Query(w.scanAdmitted)
		if err := c.SendBatch(ctx, b).Close(); err != nil {
			return changed, err
		}
		b = &pgx.Batch{}
	}
	if len(batches) > 0 {
		b.Queue(s.q.lockBatches, batches)
		b.Queue(s.q.complete, batches).Query(counting(w, &changed))
	}
	if n := deleted + doomed; n > 0 {
		b.Queue(s.q.countDeleted, "", n)
	}
	b.Queue("commit")
	return changed, c.SendBatch(ctx, b).Close()
}

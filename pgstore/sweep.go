package pgstore

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const sqlStrandedParents = `WITH RECURSIVE p(id) AS (
	(SELECT parent_id FROM {s}.deps WHERE NOT batch AND NOT resolved AND parent_id > $1 ORDER BY parent_id LIMIT 1)
	UNION ALL
	SELECT (SELECT d.parent_id FROM {s}.deps d WHERE NOT d.batch AND NOT d.resolved AND d.parent_id > p.id
		ORDER BY d.parent_id LIMIT 1)
	FROM p WHERE p.id IS NOT NULL
)
SELECT p.id, NOT EXISTS (SELECT 1 FROM {s}.jobs j WHERE j.id = p.id AND (j.state <> 'failed' OR NOT EXISTS (
	SELECT 1 FROM {s}.deps d WHERE NOT d.batch AND NOT d.resolved AND d.parent_id = p.id AND d.mask & 2 <> 0)))
FROM p WHERE p.id IS NOT NULL
LIMIT $2`

const sqlStuck = `WITH c AS (
	SELECT id FROM {s}.jobs WHERE state = 'awaiting' AND deps_pending <= 0
	ORDER BY id
	LIMIT $1
	FOR NO KEY UPDATE SKIP LOCKED
)
UPDATE {s}.jobs j SET deps_pending = 0, state = {s}.ready(j.run_at, j.limit_key) WHERE j.id = ANY(ARRAY(SELECT id FROM c))
RETURNING j.queue, j.state::text`

const sqlIdleBatches = `SELECT b.id FROM {s}.batches b
WHERE b.sealed AND b.finished_at IS NULL AND NOT EXISTS (SELECT 1 FROM {s}.jobs j WHERE j.batch_id = b.id)
ORDER BY b.id
LIMIT $1`

const sqlFinishedBatchDeps = `SELECT DISTINCT d.parent_id FROM {s}.deps d
WHERE d.batch AND NOT d.resolved AND NOT EXISTS (
	SELECT 1 FROM {s}.batches b WHERE b.id = d.parent_id AND b.finished_at IS NULL)
LIMIT $1`

const sqlThrottledKeys = `WITH RECURSIVE k(key) AS (
	(SELECT limit_key FROM {s}.jobs WHERE state = 'throttled' ORDER BY limit_key LIMIT 1)
	UNION ALL
	SELECT (SELECT j.limit_key FROM {s}.jobs j WHERE j.state = 'throttled' AND j.limit_key > k.key
		ORDER BY j.limit_key LIMIT 1)
	FROM k WHERE k.key IS NOT NULL
)
SELECT key FROM k WHERE key IS NOT NULL LIMIT $1`

const sqlLockLimits = `SELECT key FROM {s}.limits WHERE key > $1 ORDER BY key LIMIT $2 FOR NO KEY UPDATE SKIP LOCKED`

const sqlReconcile = `UPDATE {s}.limits l SET active = c.n
FROM (
	SELECT k, (SELECT count(*) FROM {s}.jobs j WHERE j.limit_key = k AND j.state IN ('enqueued', 'processing')) AS n
	FROM unnest($1::text[]) AS k
) c
WHERE l.key = c.k AND l.active <> c.n`

func (s *Store) Sweep(ctx context.Context, limit int) (int, error) {
	limit = max(limit, 1)
	var (
		w     wake
		total int
	)
	steps := []func(context.Context, int, *wake) (int, error){
		s.sweepDeps,
		s.sweepStuck,
		s.sweepBatches,
		s.sweepThrottled,
		s.reconcile,
	}
	for _, step := range steps {
		n, err := step(ctx, limit, &w)
		total += n
		if err != nil {
			s.nt.jobs(w.queues...)
			return total, fmt.Errorf("kiln: sweep: %w", err)
		}
	}
	s.nt.jobs(w.queues...)
	return total, nil
}

func (s *Store) ids(ctx context.Context, q string, args ...any) ([]int64, error) {
	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[int64])
}

func (s *Store) keys(ctx context.Context, q string, args ...any) ([]string, error) {
	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

func counting(w *wake, n *int) func(pgx.Rows) error {
	return func(rows pgx.Rows) error {
		var q, state string
		for rows.Next() {
			if err := rows.Scan(&q, &state); err != nil {
				return err
			}
			*n++
			if state == "enqueued" {
				w.queue(q)
			}
		}
		return rows.Err()
	}
}

func (s *Store) sweepDeps(ctx context.Context, limit int, w *wake) (int, error) {
	s.mu.Lock()
	after := s.sweepParent
	s.mu.Unlock()
	rows, err := s.pool.Query(ctx, s.q.strandedParents, after, limit)
	if err != nil {
		return 0, err
	}
	var (
		parents  []int64
		id       int64
		stranded bool
	)
	tag, err := pgx.ForEachRow(rows, []any{&id, &stranded}, func() error {
		if stranded {
			parents = append(parents, id)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	if len(parents) == 0 {
		if tag.RowsAffected() < int64(limit) {
			id = 0
		}
		s.mu.Lock()
		s.sweepParent = id
		s.mu.Unlock()
		return 0, nil
	}
	n := 0
	err = s.txn(ctx, func(c *pgxpool.Conn) (err error) {
		b := &pgx.Batch{}
		b.Queue("begin")
		n, err = s.cascade(ctx, c, b, parents, nil, 0, w)
		return err
	})
	return n + len(parents), err
}

func (s *Store) sweepStuck(ctx context.Context, limit int, w *wake) (int, error) {
	n := 0
	b := &pgx.Batch{}
	b.Queue(s.q.stuck, limit).Query(counting(w, &n))
	err := s.pool.SendBatch(ctx, b).Close()
	return n, err
}

func (s *Store) sweepBatches(ctx context.Context, limit int, w *wake) (int, error) {
	idle, err := s.ids(ctx, s.q.idleBatches, limit)
	if err != nil {
		return 0, err
	}
	finished, err := s.ids(ctx, s.q.finishedBatchDeps, limit)
	if err != nil || len(idle)+len(finished) == 0 {
		return 0, err
	}
	n := 0
	b := &pgx.Batch{}
	if len(idle) > 0 {
		b.Queue(s.q.lockBatches, idle)
		b.Queue(s.q.complete, idle).Query(counting(w, &n))
	}
	if len(finished) > 0 {
		b.Queue(s.q.releaseBatches, finished).Query(counting(w, &n))
	}
	err = s.pool.SendBatch(ctx, b).Close()
	return n, err
}

func (s *Store) sweepThrottled(ctx context.Context, limit int, w *wake) (int, error) {
	keys, err := s.keys(ctx, s.q.throttledKeys, limit)
	if err != nil || len(keys) == 0 {
		return 0, err
	}
	var a wake
	b := &pgx.Batch{}
	b.Queue(s.q.admitSkip, keys).Query(a.scanAdmitted)
	err = s.pool.SendBatch(ctx, b).Close()
	w.merge(a)
	return a.moved, err
}

func (s *Store) reconcile(ctx context.Context, limit int, w *wake) (int, error) {
	c, err := s.pool.Acquire(ctx)
	if err != nil {
		return 0, err
	}
	defer c.Release()
	s.mu.Lock()
	after := s.sweepKey
	s.mu.Unlock()
	var keys []string
	b := &pgx.Batch{}
	b.Queue("begin")
	b.Queue(s.q.lockLimits, after, limit).Query(func(rows pgx.Rows) error {
		var err error
		keys, err = pgx.CollectRows(rows, pgx.RowTo[string])
		return err
	})
	if err := c.SendBatch(ctx, b).Close(); err != nil {
		c.Exec(context.WithoutCancel(ctx), "rollback")
		return 0, err
	}
	next := ""
	if len(keys) == limit {
		next = keys[len(keys)-1]
	}
	s.mu.Lock()
	s.sweepKey = next
	s.mu.Unlock()
	var (
		n int
		a wake
	)
	b = &pgx.Batch{}
	if len(keys) > 0 {
		b.Queue(s.q.reconcile, keys).Exec(func(tag pgconn.CommandTag) error {
			n += int(tag.RowsAffected())
			return nil
		})
		b.Queue(s.q.admit, rules{keys: keys}.args()...).Query(a.scanAdmitted)
	}
	b.Queue("commit")
	if err := c.SendBatch(ctx, b).Close(); err != nil {
		c.Exec(context.WithoutCancel(ctx), "rollback")
		return 0, err
	}
	w.merge(a)
	return n + a.moved, nil
}

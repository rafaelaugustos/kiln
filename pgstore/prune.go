package pgstore

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rafaelaugustos/kiln/driver"
)

const sqlPruneArchive = `WITH d AS (
	DELETE FROM {s}.archive WHERE id IN (
		SELECT id FROM {s}.archive
		WHERE state = $1::text::{s}.state AND finalized_at < now() - $2 * interval '1 microsecond'
		LIMIT $3
		FOR UPDATE SKIP LOCKED)
	RETURNING id
), x AS (
	DELETE FROM {s}.deps WHERE job_id IN (SELECT id FROM d)
)
SELECT count(*) FROM d`

const sqlPruneFailed = `WITH d AS (
	DELETE FROM {s}.jobs WHERE id IN (
		SELECT id FROM {s}.jobs
		WHERE state = 'failed' AND finalized_at < now() - $1 * interval '1 microsecond'
		LIMIT $2
		FOR UPDATE SKIP LOCKED)
	RETURNING id, coalesce(batch_id, 0) AS batch_id
), x AS (
	DELETE FROM {s}.deps WHERE job_id IN (SELECT id FROM d)
)
SELECT id, batch_id FROM d`

const sqlPruneServers = `WITH d AS (
	DELETE FROM {s}.servers WHERE id IN (
		SELECT id FROM {s}.servers WHERE heartbeat_at < now() - $1 * interval '1 microsecond' LIMIT $2)
	RETURNING 1
)
SELECT count(*) FROM d`

const sqlPruneStats = `WITH d AS (
	DELETE FROM {s}.stats WHERE (bucket, server) IN (
		SELECT bucket, server FROM {s}.stats
		WHERE bucket > '-infinity' AND bucket < now() - $1 * interval '1 microsecond'
		LIMIT $2
		FOR UPDATE SKIP LOCKED)
	RETURNING succeeded, failed, deleted, retried
), f AS (
	INSERT INTO {s}.stats AS x (bucket, server, succeeded, failed, deleted, retried)
	SELECT '-infinity', '', sum(succeeded), sum(failed), sum(deleted), sum(retried) FROM d HAVING count(*) > 0
	ON CONFLICT (bucket, server) DO UPDATE SET succeeded = x.succeeded + excluded.succeeded,
		failed = x.failed + excluded.failed, deleted = x.deleted + excluded.deleted,
		retried = x.retried + excluded.retried
)
SELECT count(*) FROM d`

const sqlPruneUniques = `WITH d AS (
	DELETE FROM {s}.uniques WHERE key IN (
		SELECT key FROM {s}.uniques WHERE expires_at <= now()
		ORDER BY expires_at
		LIMIT $1
		FOR UPDATE SKIP LOCKED)
	RETURNING 1
)
SELECT count(*) FROM d`

const sqlPruneHolders = `WITH w AS MATERIALIZED (
	SELECT key, job_id FROM {s}.uniques WHERE key > coalesce($1, ''::bytea) ORDER BY key LIMIT $2
), d AS (
	DELETE FROM {s}.uniques WHERE key IN (
		SELECT u.key FROM {s}.uniques u JOIN w ON w.key = u.key AND w.job_id = u.job_id
		WHERE u.expires_at IS NULL AND NOT EXISTS (
			SELECT 1 FROM {s}.jobs j WHERE j.id = u.job_id AND j.state <> 'failed')
		FOR UPDATE OF u SKIP LOCKED)
	RETURNING 1
)
SELECT (SELECT key FROM w ORDER BY key DESC LIMIT 1), (SELECT count(*) FROM w), (SELECT count(*) FROM d)`

const unusedLimit = `(l.tat IS NULL OR l.tat <= now())
	AND NOT EXISTS (SELECT 1 FROM {s}.jobs j WHERE j.limit_key = l.key)
	AND NOT EXISTS (SELECT 1 FROM {s}.archive a WHERE a.limit_key = l.key)`

const sqlUnusedLimits = `SELECT l.key FROM {s}.limits l WHERE l.active = 0 AND ` + unusedLimit + `
LIMIT $1
FOR UPDATE SKIP LOCKED`

const sqlPruneLimits = `DELETE FROM {s}.limits l WHERE l.key = ANY($1) AND ` + unusedLimit

const sqlPruneBatches = `WITH d AS (
	DELETE FROM {s}.batches WHERE id IN (
		SELECT b.id FROM {s}.batches b
		WHERE b.finished_at IS NOT NULL
			AND NOT EXISTS (SELECT 1 FROM {s}.jobs j WHERE j.batch_id = b.id)
			AND NOT EXISTS (SELECT 1 FROM {s}.archive a WHERE a.batch_id = b.id)
		LIMIT $1
		FOR UPDATE SKIP LOCKED)
	RETURNING 1
)
SELECT count(*) FROM d`

func (s *Store) Prune(ctx context.Context, p driver.PruneParams) (int, error) {
	limit := p.Limit
	if limit <= 0 {
		limit = 1000
	}
	servers := p.Servers
	if servers <= 0 {
		servers = time.Hour
	}
	stats := p.Stats
	if stats <= 0 {
		stats = 14 * 24 * time.Hour
	}
	count := func(sql string, args ...any) func() (int, error) {
		return func() (int, error) {
			var n int
			err := s.pool.QueryRow(ctx, sql, args...).Scan(&n)
			return n, err
		}
	}
	var steps []func() (int, error)
	if p.Succeeded >= 0 {
		steps = append(steps, count(s.q.pruneArchive, string(driver.Succeeded), micros(p.Succeeded), limit))
	}
	if p.Deleted >= 0 {
		steps = append(steps, count(s.q.pruneArchive, string(driver.Deleted), micros(p.Deleted), limit))
	}
	if p.Failed >= 0 {
		steps = append(steps, func() (int, error) { return s.pruneFailed(ctx, micros(p.Failed), limit) })
	}
	steps = append(steps,
		count(s.q.pruneServers, micros(servers), limit),
		count(s.q.pruneStats, micros(stats), limit),
		count(s.q.pruneUniques, limit),
		func() (int, error) { return s.pruneHolders(ctx, limit) },
		func() (int, error) { return s.pruneLimits(ctx, limit) },
		count(s.q.pruneBatches, limit),
	)
	total := 0
	for _, step := range steps {
		n, err := step()
		if err != nil {
			return total, fmt.Errorf("kiln: prune: %w", err)
		}
		total += n
	}
	return total, nil
}

func (s *Store) pruneFailed(ctx context.Context, keep int64, limit int) (int, error) {
	var (
		ids, batches []int64
		w            wake
	)
	err := s.txn(ctx, func(c *pgxpool.Conn) error {
		b := &pgx.Batch{}
		b.Queue("begin")
		b.Queue(s.q.pruneFailed, keep, limit).Query(func(rows pgx.Rows) error {
			var id, batch int64
			for rows.Next() {
				if err := rows.Scan(&id, &batch); err != nil {
					return err
				}
				ids = append(ids, id)
				if batch != 0 && !slices.Contains(batches, batch) {
					batches = append(batches, batch)
				}
			}
			return rows.Err()
		})
		if err := c.SendBatch(ctx, b).Close(); err != nil {
			return err
		}
		_, err := s.cascade(ctx, c, &pgx.Batch{}, ids, batches, 0, &w)
		return err
	})
	if err != nil {
		return 0, err
	}
	s.nt.jobs(w.queues...)
	return len(ids), nil
}

func (s *Store) pruneHolders(ctx context.Context, limit int) (int, error) {
	s.mu.Lock()
	after := s.pruneKey
	s.mu.Unlock()
	var (
		last    []byte
		seen, n int
	)
	if err := s.pool.QueryRow(ctx, s.q.pruneHolders, after, limit).Scan(&last, &seen, &n); err != nil {
		return 0, err
	}
	if seen < limit {
		last = nil
	}
	s.mu.Lock()
	s.pruneKey = last
	s.mu.Unlock()
	return n, nil
}

func (s *Store) pruneLimits(ctx context.Context, limit int) (int, error) {
	n := 0
	err := s.txn(ctx, func(c *pgxpool.Conn) error {
		var keys []string
		b := &pgx.Batch{}
		b.Queue("begin")
		b.Queue(s.q.unusedLimits, limit).Query(func(rows pgx.Rows) error {
			var err error
			keys, err = pgx.CollectRows(rows, pgx.RowTo[string])
			return err
		})
		if err := c.SendBatch(ctx, b).Close(); err != nil {
			return err
		}
		b = &pgx.Batch{}
		if len(keys) > 0 {
			b.Queue(s.q.pruneLimits, keys).Exec(func(tag pgconn.CommandTag) error {
				n = int(tag.RowsAffected())
				return nil
			})
		}
		b.Queue("commit")
		return c.SendBatch(ctx, b).Close()
	})
	return n, err
}

package pgstore

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/rafaelaugustos/kiln/driver"
)

const sqlNow = `SELECT now()`

const sqlLead = `INSERT INTO {s}.leases AS l (name, holder, expires_at, acquired_at)
VALUES ($1, $2, now() + $3 * interval '1 microsecond', now())
ON CONFLICT (name) DO UPDATE SET holder = excluded.holder, expires_at = excluded.expires_at,
	acquired_at = CASE WHEN l.holder = excluded.holder AND l.expires_at > now() THEN l.acquired_at ELSE now() END
WHERE l.holder = excluded.holder OR l.expires_at <= now()
RETURNING (extract(epoch FROM now() - acquired_at) * 1000000)::bigint`

const sqlResign = `DELETE FROM {s}.leases WHERE name = $1 AND holder = $2`

const sqlPromote = `WITH c AS (
	SELECT id FROM {s}.jobs WHERE state = 'scheduled' AND run_at <= now()
	ORDER BY run_at
	LIMIT $1
	FOR NO KEY UPDATE SKIP LOCKED
), u AS (
	UPDATE {s}.jobs j SET state = CASE WHEN j.limit_key IS NULL THEN 'enqueued' ELSE 'throttled' END::{s}.state
	WHERE j.id = ANY(ARRAY(SELECT id FROM c))
	RETURNING j.queue, j.state, j.limit_key
)
SELECT count(*), coalesce(array_agg(DISTINCT queue) FILTER (WHERE state = 'enqueued'), '{}'),
	coalesce(array_agg(DISTINCT limit_key) FILTER (WHERE limit_key IS NOT NULL), '{}')
FROM u`

const sqlNextDue = `SELECT (extract(epoch FROM min(run_at) - now()) * 1000000)::bigint FROM {s}.jobs WHERE state = 'scheduled'`

const sqlOrphans = `SELECT j.id, j.claim, j.kind, j.queue, j.attempt, j.max_attempts, coalesce(j.server, ''), j.cancel_requested
FROM {s}.jobs j
WHERE j.state = 'processing' AND NOT EXISTS (
	SELECT 1 FROM {s}.servers s
	WHERE s.id = j.server AND s.heartbeat_at > now() - $1 * interval '1 microsecond')
LIMIT $2`

func (s *Store) Now(ctx context.Context) (time.Time, error) {
	var t time.Time
	if err := s.pool.QueryRow(ctx, s.q.now).Scan(&t); err != nil {
		return time.Time{}, fmt.Errorf("kiln: now: %w", err)
	}
	return t, nil
}

func (s *Store) Lead(ctx context.Context, name, holder string, ttl time.Duration) (time.Duration, bool, error) {
	var held int64
	err := s.pool.QueryRow(ctx, s.q.lead, name, holder, micros(ttl)).Scan(&held)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return 0, false, nil
	case err != nil:
		return 0, false, fmt.Errorf("kiln: lead: %w", err)
	}
	return time.Duration(max(held, 0)) * time.Microsecond, true, nil
}

func (s *Store) Resign(ctx context.Context, name, holder string) error {
	if _, err := s.pool.Exec(ctx, s.q.resign, name, holder); err != nil {
		return fmt.Errorf("kiln: resign: %w", err)
	}
	return nil
}

func (s *Store) Promote(ctx context.Context, limit int) (driver.Promoted, error) {
	var (
		p    driver.Promoted
		keys []string
		next *int64
	)
	b := &pgx.Batch{}
	b.Queue(s.q.promote, max(limit, 1)).QueryRow(func(row pgx.Row) error {
		return row.Scan(&p.Count, &p.Queues, &keys)
	})
	b.Queue(s.q.nextDue).QueryRow(func(row pgx.Row) error {
		return row.Scan(&next)
	})
	if err := s.pool.SendBatch(ctx, b).Close(); err != nil {
		return driver.Promoted{}, fmt.Errorf("kiln: promote: %w", err)
	}
	if next != nil {
		p.Next = max(time.Duration(*next)*time.Microsecond, time.Microsecond)
	}
	if len(keys) > 0 {
		queues, err := s.admit(ctx, keys, nil)
		if err != nil {
			return p, err
		}
		for _, q := range queues {
			if !slices.Contains(p.Queues, q) {
				p.Queues = append(p.Queues, q)
			}
		}
	}
	s.nt.jobs(p.Queues...)
	return p, nil
}

func (s *Store) admit(ctx context.Context, keys []string, maxes []int32) ([]string, error) {
	rows, err := s.pool.Query(ctx, s.q.admit, keys, maxes)
	if err != nil {
		return nil, fmt.Errorf("kiln: admit: %w", err)
	}
	queues, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, fmt.Errorf("kiln: admit: %w", err)
	}
	return queues, nil
}

func (s *Store) Orphans(ctx context.Context, deadAfter time.Duration, limit int) ([]driver.Orphan, error) {
	rows, err := s.pool.Query(ctx, s.q.orphans, micros(deadAfter), max(limit, 1))
	if err != nil {
		return nil, fmt.Errorf("kiln: orphans: %w", err)
	}
	defer rows.Close()
	var out []driver.Orphan
	for rows.Next() {
		var o driver.Orphan
		if err := rows.Scan(&o.ID, &o.Claim, &o.Kind, &o.Queue, &o.Attempt, &o.MaxAttempts, &o.Server, &o.Cancel); err != nil {
			return nil, fmt.Errorf("kiln: orphans: %w", err)
		}
		out = append(out, o)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("kiln: orphans: %w", err)
	}
	slices.SortFunc(out, func(a, b driver.Orphan) int { return cmp.Compare(a.ID, b.ID) })
	return out, nil
}

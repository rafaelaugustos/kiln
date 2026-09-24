package mysqlstore

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

const sqlLead = `INSERT INTO {p}leases (name, holder, expires_at, acquired_at)
VALUES (?, ?, UTC_TIMESTAMP(6) + INTERVAL ? MICROSECOND, UTC_TIMESTAMP(6)) AS n
ON DUPLICATE KEY UPDATE
	holder = IF({p}leases.holder = n.holder OR {p}leases.expires_at <= n.acquired_at, n.holder, {p}leases.holder),
	acquired_at = IF({p}leases.holder = n.holder AND {p}leases.expires_at <= n.acquired_at, n.acquired_at, {p}leases.acquired_at),
	expires_at = IF({p}leases.holder = n.holder, n.expires_at, {p}leases.expires_at)`

const sqlLeader = `SELECT holder = ?, TIMESTAMPDIFF(MICROSECOND, acquired_at, expires_at) FROM {p}leases WHERE name = ?`

const sqlResign = `DELETE FROM {p}leases WHERE name = ? AND holder = ?`

const sqlNextDue = `SELECT TIMESTAMPDIFF(MICROSECOND, UTC_TIMESTAMP(6), MIN(run_at)) FROM {p}jobs WHERE state = 'scheduled'`

const sqlDue = `SELECT UTC_TIMESTAMP(6), id, queue, COALESCE(limit_key, '') FROM {p}jobs FORCE INDEX (jobs_due)
WHERE state = 'scheduled' AND run_at <= UTC_TIMESTAMP(6)
ORDER BY run_at LIMIT ? FOR UPDATE SKIP LOCKED`

const sqlPromote = `UPDATE {p}jobs SET state = IF(limit_key IS NULL, 'enqueued', 'throttled') WHERE id IN (?)`

const sqlOrphans = `SELECT j.id, j.claim, j.kind, j.queue, j.attempt, j.max_attempts, COALESCE(j.server, ''), j.cancel_requested
FROM {p}jobs j
WHERE j.state = 'processing' AND NOT EXISTS (
	SELECT 1 FROM {p}servers s WHERE s.id = j.server AND s.heartbeat_at > UTC_TIMESTAMP(6) - INTERVAL ? MICROSECOND)
LIMIT ?`

func (s *Store) Now(ctx context.Context) (time.Time, error) {
	var now stamp
	if err := s.db.QueryRowContext(ctx, sqlNow).Scan(&now); err != nil {
		return time.Time{}, fmt.Errorf("kiln: now: %w", err)
	}
	return now.Time, nil
}

func (s *Store) Lead(ctx context.Context, name, holder string, ttl time.Duration) (time.Duration, bool, error) {
	us := micros(ttl)
	if _, err := s.db.ExecContext(ctx, render(s.q.lead, name, holder, us)); err != nil {
		return 0, false, wrap("lead", err)
	}
	var (
		ok     bool
		window int64
	)
	err := s.db.QueryRowContext(ctx, render(s.q.leader, holder, name)).Scan(&ok, &window)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return 0, false, nil
	case err != nil:
		return 0, false, wrap("lead", err)
	case !ok:
		return 0, false, nil
	}
	return time.Duration(max(window-us, 0)) * time.Microsecond, true, nil
}

func (s *Store) Resign(ctx context.Context, name, holder string) error {
	if _, err := s.db.ExecContext(ctx, render(s.q.resign, name, holder)); err != nil {
		return wrap("resign", err)
	}
	return nil
}

func (s *Store) nextDue(ctx context.Context, q querier) (time.Duration, bool, error) {
	var next sql.NullInt64
	if err := q.QueryRowContext(ctx, s.q.nextDue).Scan(&next); err != nil {
		return 0, false, err
	}
	if !next.Valid {
		return 0, false, nil
	}
	return time.Duration(next.Int64) * time.Microsecond, true, nil
}

func (s *Store) Promote(ctx context.Context, limit int) (driver.Promoted, error) {
	var p driver.Promoted
	next, found, err := s.nextDue(ctx, s.db)
	if err != nil {
		return p, fmt.Errorf("kiln: promote: %w", err)
	}
	if !found || next > 0 {
		p.Next = next
		return p, nil
	}
	err = s.txn(ctx, func(tx *sql.Tx) error {
		p = driver.Promoted{}
		rows, err := tx.QueryContext(ctx, render(s.q.due, max(limit, 1)))
		if err != nil {
			return err
		}
		var (
			ids  []int64
			keys []string
			now  stamp
		)
		for rows.Next() {
			var (
				id           int64
				queue, limit string
			)
			if err := rows.Scan(&now, &id, &queue, &limit); err != nil {
				rows.Close()
				return err
			}
			ids = append(ids, id)
			if limit != "" {
				keys = merge(keys, []string{limit})
			} else if !slices.Contains(p.Queues, queue) {
				p.Queues = append(p.Queues, queue)
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		if len(ids) > 0 {
			slices.Sort(ids)
			if _, err := tx.ExecContext(ctx, render(s.q.promote, ids)); err != nil {
				return err
			}
			p.Count = len(ids)
		}
		if len(keys) > 0 {
			slices.Sort(keys)
			slots, err := s.lockLimits(ctx, tx, keys, false)
			if err != nil {
				return err
			}
			a, err := s.fill(ctx, tx, slots)
			if err != nil {
				return err
			}
			p.Queues = merge(p.Queues, a.queues)
		}
		next, found, err := s.nextDue(ctx, tx)
		if err != nil {
			return err
		}
		if found {
			p.Next = max(next, time.Microsecond)
		}
		return nil
	})
	if err != nil {
		return driver.Promoted{}, fmt.Errorf("kiln: promote: %w", err)
	}
	return p, nil
}

func (s *Store) Orphans(ctx context.Context, deadAfter time.Duration, limit int) ([]driver.Orphan, error) {
	rows, err := s.db.QueryContext(ctx, render(s.q.orphans, micros(deadAfter), max(limit, 1)))
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

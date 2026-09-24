package sqlitestore

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

const sqlNow = `SELECT {now}`

const sqlLead = `INSERT INTO {p}leases (name, holder, expires_at, acquired_at) VALUES (?, ?, {now} + ?, {now})
ON CONFLICT (name) DO UPDATE SET
	holder = CASE WHEN holder = excluded.holder OR expires_at <= excluded.acquired_at THEN excluded.holder ELSE holder END,
	acquired_at = CASE WHEN expires_at <= excluded.acquired_at THEN excluded.acquired_at ELSE acquired_at END,
	expires_at = CASE WHEN holder = excluded.holder OR expires_at <= excluded.acquired_at
		THEN excluded.expires_at ELSE expires_at END
RETURNING holder = ?, expires_at - acquired_at`

const sqlResign = `DELETE FROM {p}leases WHERE name = ? AND holder = ?`

const sqlNextDue = `SELECT MIN(run_at) - {now} FROM {p}jobs WHERE state = 'scheduled'`

const sqlPromote = `UPDATE {p}jobs SET state = CASE WHEN limit_key IS NULL THEN 'enqueued' ELSE 'throttled' END
WHERE id IN (SELECT id FROM {p}jobs WHERE state = 'scheduled' AND run_at <= {now} ORDER BY run_at LIMIT ?)
RETURNING {now}, queue, COALESCE(limit_key, '')`

const sqlOrphans = `SELECT j.id, j.claim, j.kind, j.queue, j.attempt, j.max_attempts, COALESCE(j.server, ''),
	j.cancel_requested
FROM {p}jobs j
WHERE j.state = 'processing' AND NOT EXISTS (
	SELECT 1 FROM {p}servers s WHERE s.id = j.server AND s.heartbeat_at > {now} - ?)
ORDER BY j.id
LIMIT ?`

func (s *Store) Now(ctx context.Context) (time.Time, error) {
	var now int64
	if err := s.db.QueryRowContext(ctx, s.q.now).Scan(&now); err != nil {
		return time.Time{}, fmt.Errorf("kiln: now: %w", err)
	}
	return time.UnixMicro(now).UTC(), nil
}

func (s *Store) Lead(ctx context.Context, name, holder string, ttl time.Duration) (time.Duration, bool, error) {
	us := micros(ttl)
	var (
		ok     bool
		window int64
	)
	err := s.write(ctx, func(ctx context.Context, q querier) error {
		return q.QueryRowContext(ctx, s.q.lead, name, holder, us, holder).Scan(&ok, &window)
	})
	if err != nil {
		return 0, false, wrap("lead", err)
	}
	if !ok {
		return 0, false, nil
	}
	return time.Duration(max(window-us, 0)) * time.Microsecond, true, nil
}

func (s *Store) Resign(ctx context.Context, name, holder string) error {
	err := s.write(ctx, func(ctx context.Context, q querier) error {
		_, err := q.ExecContext(ctx, s.q.resign, name, holder)
		return err
	})
	if err != nil {
		return wrap("resign", err)
	}
	return nil
}

func (s *Store) nextDue(ctx context.Context, q querier) (time.Duration, bool, error) {
	var next sql.NullInt64
	if err := q.QueryRowContext(ctx, s.q.nextDue).Scan(&next); err != nil {
		return 0, false, err
	}
	return time.Duration(next.Int64) * time.Microsecond, next.Valid, nil
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
	var f *fallout
	err = s.write(ctx, func(ctx context.Context, q querier) error {
		p = driver.Promoted{}
		f = &fallout{}
		rows, err := q.QueryContext(ctx, s.q.promote, max(limit, 1))
		err = each(rows, err, func() error {
			var queue, key string
			if err := rows.Scan(&f.now, &queue, &key); err != nil {
				return err
			}
			p.Count++
			if key == "" {
				f.queue(queue)
			} else {
				f.throttled(key)
			}
			return nil
		})
		if err != nil {
			return err
		}
		if p.Count > 0 {
			if err := s.settle(ctx, q, f); err != nil {
				return err
			}
		}
		next, found, err := s.nextDue(ctx, q)
		if found {
			p.Next = max(next, time.Microsecond)
		}
		return err
	})
	if err != nil {
		return driver.Promoted{}, fmt.Errorf("kiln: promote: %w", err)
	}
	p.Queues = f.queues
	s.hub.ready(f.queues)
	return p, nil
}

func (s *Store) Orphans(ctx context.Context, deadAfter time.Duration, limit int) ([]driver.Orphan, error) {
	var out []driver.Orphan
	rows, err := s.db.QueryContext(ctx, s.q.orphans, micros(deadAfter), max(limit, 1))
	err = each(rows, err, func() error {
		var o driver.Orphan
		if err := rows.Scan(&o.ID, &o.Claim, &o.Kind, &o.Queue, &o.Attempt, &o.MaxAttempts, &o.Server, &o.Cancel); err != nil {
			return err
		}
		out = append(out, o)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("kiln: orphans: %w", err)
	}
	return out, nil
}

package mssqlstore

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

const sqlLead = `MERGE {p}leases WITH (HOLDLOCK) AS l
USING (SELECT @name COLLATE Latin1_General_100_BIN2 AS name, @holder COLLATE Latin1_General_100_BIN2 AS holder,
	CAST(SYSUTCDATETIME() AS DATETIME2(6)) AS now) AS n
ON l.name = n.name
WHEN MATCHED AND (l.holder = n.holder OR l.expires_at <= n.now) THEN UPDATE SET
	acquired_at = CASE WHEN l.holder = n.holder AND l.expires_at > n.now THEN l.acquired_at ELSE n.now END,
	holder = n.holder,
	expires_at = DATEADD(MICROSECOND, @us % 1000000, DATEADD(SECOND, @us / 1000000, n.now))
WHEN NOT MATCHED THEN INSERT (name, holder, expires_at, acquired_at)
	VALUES (n.name, n.holder, DATEADD(MICROSECOND, @us % 1000000, DATEADD(SECOND, @us / 1000000, n.now)), n.now)
OUTPUT DATEDIFF_BIG(MICROSECOND, inserted.acquired_at, n.now);`

const sqlResign = `DELETE FROM {p}leases WHERE name = @name AND holder = @holder`

const sqlNextDue = `SELECT DATEDIFF_BIG(MICROSECOND, SYSUTCDATETIME(), MIN(run_at)) FROM {p}jobs WHERE state = 'scheduled'`

const sqlPending = `SELECT DATEDIFF_BIG(MICROSECOND, SYSUTCDATETIME(), MIN(run_at)), NULL FROM {p}jobs WHERE state = 'scheduled'
UNION ALL
SELECT NULL, g.limit_key FROM (
	SELECT DISTINCT TOP (100) limit_key FROM {p}jobs WHERE state = 'throttled' AND granted = 1 ORDER BY limit_key
) g`

const sqlPromote = `WITH d AS (
	SELECT TOP (@n) id, state, queue, limit_key FROM {p}jobs WITH (XLOCK, READPAST, ROWLOCK, INDEX(jobs_due))
	WHERE state = 'scheduled' AND run_at <= SYSUTCDATETIME()
	ORDER BY run_at
)
UPDATE d SET state = CASE WHEN limit_key IS NULL THEN 'enqueued' ELSE 'throttled' END
OUTPUT inserted.id, inserted.queue, COALESCE(inserted.limit_key, N'')`

const sqlOrphans = `SELECT TOP (@n) j.id, j.claim, j.kind, j.queue, j.attempt, j.max_attempts, COALESCE(j.server, N''),
	j.cancel_requested, COALESCE((SELECT TOP (1) JSON_VALUE(h.value, '$.reason') FROM OPENJSON(j.history) h
		ORDER BY CAST(h.[key] AS INT) DESC), N'')
FROM {p}jobs j WITH (INDEX(jobs_running))
WHERE j.state = 'processing' AND NOT EXISTS (
	SELECT 1 FROM {p}servers s WHERE s.id = j.server
		AND s.heartbeat_at > DATEADD(MICROSECOND, -(@us % 1000000), DATEADD(SECOND, -(@us / 1000000), SYSUTCDATETIME())))`

// Now returns SYSUTCDATETIME() from the database server, to the microsecond, the clock every
// method of the store uses.
func (s *Store) Now(ctx context.Context) (time.Time, error) {
	var now moment
	if err := s.db.QueryRowContext(ctx, sqlNow).Scan(&now); err != nil {
		return time.Time{}, fmt.Errorf("kiln: now: %w", err)
	}
	return now.Time, nil
}

// Lead acquires or renews the lease called name for holder, as [driver.Coordinator.Lead]
// describes, in one statement.
func (s *Store) Lead(ctx context.Context, name, holder string, ttl time.Duration) (time.Duration, bool, error) {
	var held int64
	err := s.retry(ctx, func() error {
		return s.db.QueryRowContext(ctx, s.q.lead, sql.Named("name", name), sql.Named("holder", holder),
			sql.Named("us", micros(ttl))).Scan(&held)
	})
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return 0, false, nil
	case err != nil:
		return 0, false, wrap("lead", err)
	}
	return time.Duration(max(held, 0)) * time.Microsecond, true, nil
}

// Resign deletes the lease called name if holder has it.
func (s *Store) Resign(ctx context.Context, name, holder string) error {
	if _, err := s.db.ExecContext(ctx, s.q.resign, sql.Named("name", name), sql.Named("holder", holder)); err != nil {
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

func (s *Store) pending(ctx context.Context) (next time.Duration, found bool, granted []string, err error) {
	rows, err := s.db.QueryContext(ctx, s.q.pending)
	if err != nil {
		return 0, false, nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			us  sql.NullInt64
			key sql.NullString
		)
		if err := rows.Scan(&us, &key); err != nil {
			return 0, false, nil, err
		}
		if key.Valid {
			granted = append(granted, key.String)
		} else if us.Valid {
			next, found = time.Duration(us.Int64)*time.Microsecond, true
		}
	}
	return next, found, granted, rows.Err()
}

// Promote moves up to limit scheduled jobs whose run time has come, earliest first, as
// [driver.Coordinator.Promote] describes, skipping due jobs that other transactions have locked. It
// looks with a plain read first and opens a transaction only when jobs are due or granted jobs
// wait for admission, so that frequent calls on an idle store stay cheap.
func (s *Store) Promote(ctx context.Context, limit int) (driver.Promoted, error) {
	var p driver.Promoted
	next, found, granted, err := s.pending(ctx)
	if err != nil {
		return p, fmt.Errorf("kiln: promote: %w", err)
	}
	if len(granted) == 0 && (!found || next > 0) {
		p.Next = next
		return p, nil
	}
	err = s.txn(ctx, func(tx *sql.Tx) error {
		p = driver.Promoted{}
		rows, err := tx.QueryContext(ctx, s.q.promote, sql.Named("n", max(limit, 1)))
		if err != nil {
			return err
		}
		var keys []string
		for rows.Next() {
			var (
				id           int64
				queue, limit string
			)
			if err := rows.Scan(&id, &queue, &limit); err != nil {
				rows.Close()
				return err
			}
			p.Count++
			if limit != "" {
				keys = merge(keys, limit)
			} else {
				p.Queues = merge(p.Queues, queue)
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		keys = merge(keys, granted...)
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
			p.Queues = merge(p.Queues, a.queues...)
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
	s.nt.ready(p.Queues)
	return p, nil
}

// Orphans returns up to limit processing jobs whose server has no row or has not sent a heartbeat
// for deadAfter. It changes nothing.
func (s *Store) Orphans(ctx context.Context, deadAfter time.Duration, limit int) ([]driver.Orphan, error) {
	rows, err := s.db.QueryContext(ctx, s.q.orphans, sql.Named("us", micros(deadAfter)), sql.Named("n", max(limit, 1)))
	if err != nil {
		return nil, fmt.Errorf("kiln: orphans: %w", err)
	}
	defer rows.Close()
	var out []driver.Orphan
	for rows.Next() {
		var o driver.Orphan
		if err := rows.Scan(&o.ID, &o.Claim, &o.Kind, &o.Queue, &o.Attempt, &o.MaxAttempts, &o.Server, &o.Cancel, &o.LastReason); err != nil {
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

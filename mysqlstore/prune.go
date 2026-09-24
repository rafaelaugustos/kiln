package mysqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

var totals = time.Date(1000, time.January, 1, 0, 0, 0, 0, time.UTC)

const sqlExpiredArchive = `SELECT id FROM {p}archive
WHERE state = ? AND finalized_at < UTC_TIMESTAMP(6) - INTERVAL ? MICROSECOND
ORDER BY finalized_at
LIMIT ?
FOR UPDATE SKIP LOCKED`

const sqlDropDeps = `DELETE FROM {p}deps WHERE job_id IN (?)`

const sqlExpiredFailed = `SELECT UTC_TIMESTAMP(6), j.id, COALESCE(j.batch_id, 0),
	EXISTS (SELECT 1 FROM {p}deps d WHERE d.batch = FALSE AND d.parent_id = j.id AND d.resolved = FALSE)
FROM {p}jobs j
WHERE j.state = 'failed' AND j.finalized_at < UTC_TIMESTAMP(6) - INTERVAL ? MICROSECOND
ORDER BY j.finalized_at
LIMIT ?
FOR UPDATE SKIP LOCKED`

const sqlPruneServers = `DELETE FROM {p}servers WHERE heartbeat_at < UTC_TIMESTAMP(6) - INTERVAL ? MICROSECOND LIMIT ?`

const sqlExpiredStats = `SELECT bucket, server, succeeded, failed, deleted, retried FROM {p}stats
WHERE bucket > ? AND bucket < UTC_TIMESTAMP(6) - INTERVAL ? MICROSECOND
ORDER BY bucket, server
LIMIT ?
FOR UPDATE SKIP LOCKED`

const sqlDropStats = `DELETE FROM {p}stats WHERE (bucket, server) IN (`

const sqlExpiredUniques = `SELECT unique_key FROM {p}uniques WHERE expires_at <= UTC_TIMESTAMP(6) ORDER BY expires_at LIMIT ?`

const sqlPruneUniques = `DELETE u FROM {p}uniques u FORCE INDEX (PRIMARY)
WHERE u.unique_key IN (?) AND u.expires_at <= UTC_TIMESTAMP(6)`

const holderGone = `expires_at IS NULL AND NOT EXISTS (
	SELECT 1 FROM {p}jobs j WHERE j.id = {p}uniques.job_id AND j.state <> 'failed')`

const sqlDropHolders = `DELETE FROM {p}uniques WHERE unique_key IN (?) AND ` + holderGone

const sqlHolderPage = `SELECT unique_key, ` + holderGone + `
FROM {p}uniques WHERE unique_key > ? ORDER BY unique_key LIMIT ?`

const sqlUnusedLimits = `SELECT l.limit_key FROM {p}limits l
WHERE l.active = 0 AND (l.declared_at IS NULL OR l.declared_at < UTC_TIMESTAMP(6) - INTERVAL 1 HOUR)
	AND NOT EXISTS (SELECT 1 FROM {p}jobs j WHERE j.limit_key = l.limit_key)
	AND NOT EXISTS (SELECT 1 FROM {p}archive a WHERE a.limit_key = l.limit_key)
LIMIT ?
FOR UPDATE SKIP LOCKED`

const sqlPruneLimits = `DELETE FROM {p}limits WHERE limit_key IN (?)`

const sqlDoneBatches = `SELECT b.id FROM {p}batches b
WHERE b.finished_at IS NOT NULL
	AND NOT EXISTS (SELECT 1 FROM {p}jobs j WHERE j.batch_id = b.id)
	AND NOT EXISTS (SELECT 1 FROM {p}archive a WHERE a.batch_id = b.id)
LIMIT ?
FOR UPDATE SKIP LOCKED`

const sqlPruneBatches = `DELETE FROM {p}batches WHERE id IN (?)`

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
	var steps []func() (int, error)
	if p.Succeeded >= 0 {
		steps = append(steps, func() (int, error) { return s.pruneArchive(ctx, driver.Succeeded, p.Succeeded, limit) })
	}
	if p.Deleted >= 0 {
		steps = append(steps, func() (int, error) { return s.pruneArchive(ctx, driver.Deleted, p.Deleted, limit) })
	}
	if p.Failed >= 0 {
		steps = append(steps, func() (int, error) { return s.pruneFailed(ctx, p.Failed, limit) })
	}
	steps = append(steps,
		func() (int, error) { return s.affected(ctx, render(s.q.pruneServers, micros(servers), limit)) },
		func() (int, error) { return s.pruneStats(ctx, stats, limit) },
		func() (int, error) { return s.pruneUniques(ctx, limit) },
		func() (int, error) { return s.pruneHolders(ctx, limit) },
		func() (int, error) { return s.pruneLimits(ctx, limit) },
		func() (int, error) { return s.pruneBatches(ctx, limit) },
	)
	total := 0
	var errs []error
	for _, step := range steps {
		n, err := step()
		total += n
		if err != nil {
			errs = append(errs, err)
			if ctx.Err() != nil {
				break
			}
		}
	}
	if len(errs) > 0 {
		return total, fmt.Errorf("kiln: prune: %w", errors.Join(errs...))
	}
	return total, nil
}

func (s *Store) affected(ctx context.Context, stmt string) (int, error) {
	n := 0
	err := s.txn(ctx, func(tx *sql.Tx) error {
		r, err := tx.ExecContext(ctx, stmt)
		if err != nil {
			return err
		}
		affected, err := r.RowsAffected()
		n = int(affected)
		return err
	})
	return n, err
}

func (s *Store) pruneUniques(ctx context.Context, limit int) (int, error) {
	n := 0
	err := s.txn(ctx, func(tx *sql.Tx) error {
		n = 0
		rows, err := tx.QueryContext(ctx, render(s.q.expiredUniques, limit))
		if err != nil {
			return err
		}
		var keys [][]byte
		for rows.Next() {
			var key []byte
			if err := rows.Scan(&key); err != nil {
				rows.Close()
				return err
			}
			keys = append(keys, key)
		}
		rows.Close()
		if err := rows.Err(); err != nil || len(keys) == 0 {
			return err
		}
		r, err := tx.ExecContext(ctx, render(s.q.pruneUniques, keys))
		if err != nil {
			return err
		}
		affected, err := r.RowsAffected()
		n = int(affected)
		return err
	})
	return n, err
}

func (s *Store) pruneBatches(ctx context.Context, limit int) (int, error) {
	n := 0
	err := s.txn(ctx, func(tx *sql.Tx) error {
		n = 0
		rows, err := tx.QueryContext(ctx, render(s.q.doneBatches, limit))
		if err != nil {
			return err
		}
		var ids []int64
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return err
			}
			ids = append(ids, id)
		}
		rows.Close()
		if err := rows.Err(); err != nil || len(ids) == 0 {
			return err
		}
		if _, err := tx.ExecContext(ctx, render(s.q.pruneBatches, ids)); err != nil {
			return err
		}
		n = len(ids)
		return nil
	})
	return n, err
}

func (s *Store) pruneArchive(ctx context.Context, state driver.State, keep time.Duration, limit int) (int, error) {
	n := 0
	err := s.txn(ctx, func(tx *sql.Tx) error {
		n = 0
		rows, err := tx.QueryContext(ctx, render(s.q.expiredArchive, state, micros(keep), limit))
		if err != nil {
			return err
		}
		var ids []int64
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return err
			}
			ids = append(ids, id)
		}
		rows.Close()
		if err := rows.Err(); err != nil || len(ids) == 0 {
			return err
		}
		if _, err := tx.ExecContext(ctx, render(s.q.dropArchived, ids)); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, render(s.q.dropDeps, ids)); err != nil {
			return err
		}
		n = len(ids)
		return nil
	})
	return n, err
}

func (s *Store) pruneFailed(ctx context.Context, keep time.Duration, limit int) (int, error) {
	n := 0
	err := s.txn(ctx, func(tx *sql.Tx) error {
		n = 0
		rows, err := tx.QueryContext(ctx, render(s.q.expiredFailed, micros(keep), limit))
		if err != nil {
			return err
		}
		f := &fallout{}
		var ids []int64
		for rows.Next() {
			var (
				now      stamp
				id       int64
				batch    int64
				children bool
			)
			if err := rows.Scan(&now, &id, &batch, &children); err != nil {
				rows.Close()
				return err
			}
			f.now = now.Time
			ids = append(ids, id)
			f.batch(batch)
			if children {
				f.parents = append(f.parents, parent{id: id, state: driver.Deleted, label: "pruned"})
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil || len(ids) == 0 {
			return err
		}
		if _, err := tx.ExecContext(ctx, render(s.q.remove, ids)); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, render(s.q.dropDeps, ids)); err != nil {
			return err
		}
		n = len(ids)
		return s.settle(ctx, tx, f)
	})
	return n, err
}

func (s *Store) pruneStats(ctx context.Context, keep time.Duration, limit int) (int, error) {
	n := 0
	err := s.txn(ctx, func(tx *sql.Tx) error {
		n = 0
		rows, err := tx.QueryContext(ctx, render(s.q.expiredStats, totals, micros(keep), limit))
		if err != nil {
			return err
		}
		var (
			sum  tally
			keys []byte
		)
		for rows.Next() {
			var (
				bucket stamp
				server string
				t      tally
			)
			if err := rows.Scan(&bucket, &server, &t.succeeded, &t.failed, &t.deleted, &t.retried); err != nil {
				rows.Close()
				return err
			}
			sum.succeeded += t.succeeded
			sum.failed += t.failed
			sum.deleted += t.deleted
			sum.retried += t.retried
			if n > 0 {
				keys = append(keys, ',')
			}
			keys = appendSQL(keys, "(?, ?)", bucket.Time, server)
			n++
		}
		rows.Close()
		if err := rows.Err(); err != nil || n == 0 {
			return err
		}
		drop := s.q.dropStats + string(keys) + ")"
		if _, err := tx.ExecContext(ctx, drop); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, render(s.q.count, totals, "", sum.succeeded, sum.failed, sum.deleted, sum.retried))
		return err
	})
	return n, err
}

func (s *Store) pruneHolders(ctx context.Context, limit int) (int, error) {
	s.mu.Lock()
	after := s.cursors.holder
	s.mu.Unlock()
	if after == nil {
		after = []byte{}
	}
	rows, err := s.db.QueryContext(ctx, render(s.q.holderPage, after, limit))
	if err != nil {
		return 0, err
	}
	var (
		seen int
		last []byte
		dead [][]byte
	)
	for rows.Next() {
		var (
			key  []byte
			gone bool
		)
		if err := rows.Scan(&key, &gone); err != nil {
			rows.Close()
			return 0, err
		}
		seen++
		last = key
		if gone {
			dead = append(dead, key)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if seen < limit {
		last = nil
	}
	s.mu.Lock()
	s.cursors.holder = last
	s.mu.Unlock()
	if dead == nil {
		return 0, nil
	}
	return s.affected(ctx, render(s.q.dropHolders, dead))
}

func (s *Store) pruneLimits(ctx context.Context, limit int) (int, error) {
	n := 0
	err := s.txn(ctx, func(tx *sql.Tx) error {
		n = 0
		rows, err := tx.QueryContext(ctx, render(s.q.unusedLimits, limit))
		if err != nil {
			return err
		}
		var keys []string
		for rows.Next() {
			var key string
			if err := rows.Scan(&key); err != nil {
				rows.Close()
				return err
			}
			keys = append(keys, key)
		}
		rows.Close()
		if err := rows.Err(); err != nil || len(keys) == 0 {
			return err
		}
		r, err := tx.ExecContext(ctx, render(s.q.pruneLimits, keys))
		if err != nil {
			return err
		}
		affected, err := r.RowsAffected()
		n = int(affected)
		return err
	})
	return n, err
}

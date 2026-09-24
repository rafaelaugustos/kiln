package sqlitestore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

const totals = 0

const sqlPruneArchive = `DELETE FROM {p}archive WHERE id IN (
	SELECT id FROM {p}archive WHERE state = ? AND finalized_at < {now} - ? ORDER BY finalized_at LIMIT ?)
RETURNING id`

const sqlDropDeps = `DELETE FROM {p}deps WHERE job_id IN (SELECT value FROM json_each(?))`

const sqlPruneFailed = `SELECT {now}, j.id, COALESCE(j.batch_id, 0),
	EXISTS (SELECT 1 FROM {p}deps d WHERE d.batch = 0 AND d.parent_id = j.id AND d.resolved = 0)
FROM {p}jobs j
WHERE j.state = 'failed' AND j.finalized_at < {now} - ?
ORDER BY j.finalized_at
LIMIT ?`

const sqlPruneServers = `DELETE FROM {p}servers WHERE id IN (
	SELECT id FROM {p}servers WHERE heartbeat_at < {now} - ? LIMIT ?)`

const sqlExpiredStats = `DELETE FROM {p}stats WHERE (bucket, server) IN (
	SELECT bucket, server FROM {p}stats WHERE bucket > 0 AND bucket < {now} - ? ORDER BY bucket, server LIMIT ?)
RETURNING succeeded, failed, deleted, retried`

const sqlPruneUniques = `DELETE FROM {p}uniques WHERE unique_key IN (
	SELECT unique_key FROM {p}uniques WHERE expires_at <= {now} ORDER BY expires_at LIMIT ?)`

const holderGone = `expires_at IS NULL AND NOT EXISTS (
	SELECT 1 FROM {p}jobs j WHERE j.id = {p}uniques.job_id AND j.state <> 'failed')`

const sqlHolderPage = `SELECT unique_key, ` + holderGone + ` FROM {p}uniques
WHERE unique_key > COALESCE(?, X'') ORDER BY unique_key LIMIT ?`

const sqlDropHolder = `DELETE FROM {p}uniques WHERE unique_key = ? AND ` + holderGone

const sqlPruneLimits = `DELETE FROM {p}limits WHERE limit_key IN (
	SELECT l.limit_key FROM {p}limits l
	WHERE l.active = 0 AND (l.tat IS NULL OR l.tat <= {now})
		AND NOT EXISTS (SELECT 1 FROM {p}jobs j WHERE j.limit_key = l.limit_key)
		AND NOT EXISTS (SELECT 1 FROM {p}archive a WHERE a.limit_key = l.limit_key)
	LIMIT ?)`

const sqlPruneBatches = `DELETE FROM {p}batches WHERE id IN (
	SELECT b.id FROM {p}batches b
	WHERE b.finished_at IS NOT NULL
		AND NOT EXISTS (SELECT 1 FROM {p}jobs j WHERE j.batch_id = b.id)
		AND NOT EXISTS (SELECT 1 FROM {p}archive a WHERE a.batch_id = b.id)
	LIMIT ?)`

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
		func() (int, error) { return s.affected(ctx, s.q.pruneServers, micros(servers), limit) },
		func() (int, error) { return s.pruneStats(ctx, stats, limit) },
		func() (int, error) { return s.affected(ctx, s.q.pruneUniques, limit) },
		func() (int, error) { return s.pruneHolders(ctx, limit) },
		func() (int, error) { return s.affected(ctx, s.q.pruneLimits, limit) },
		func() (int, error) { return s.affected(ctx, s.q.pruneBatches, limit) },
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

func (s *Store) affected(ctx context.Context, stmt string, args ...any) (int, error) {
	var n int64
	err := s.write(ctx, func(ctx context.Context, q querier) error {
		r, err := q.ExecContext(ctx, stmt, args...)
		if err != nil {
			return err
		}
		n, err = r.RowsAffected()
		return err
	})
	return int(n), err
}

func (s *Store) pruneArchive(ctx context.Context, state driver.State, keep time.Duration, limit int) (int, error) {
	var ids []int64
	err := s.write(ctx, func(ctx context.Context, q querier) error {
		ids = ids[:0]
		rows, err := q.QueryContext(ctx, s.q.pruneArchive, string(state), micros(keep), limit)
		err = each(rows, err, func() error {
			var id int64
			if err := rows.Scan(&id); err != nil {
				return err
			}
			ids = append(ids, id)
			return nil
		})
		if err != nil || len(ids) == 0 {
			return err
		}
		_, err = q.ExecContext(ctx, s.q.dropDeps, idList(ids))
		return err
	})
	if err != nil {
		return 0, err
	}
	return len(ids), nil
}

func (s *Store) pruneFailed(ctx context.Context, keep time.Duration, limit int) (int, error) {
	n := 0
	var f *fallout
	err := s.write(ctx, func(ctx context.Context, q querier) error {
		n = 0
		f = &fallout{}
		var ids []int64
		rows, err := q.QueryContext(ctx, s.q.pruneFailed, micros(keep), limit)
		err = each(rows, err, func() error {
			var (
				id       int64
				batch    int64
				children bool
			)
			if err := rows.Scan(&f.now, &id, &batch, &children); err != nil {
				return err
			}
			ids = append(ids, id)
			f.batch(batch)
			if children {
				f.parents = append(f.parents, parent{id: id, state: driver.Deleted, label: "pruned"})
			}
			return nil
		})
		if err != nil || len(ids) == 0 {
			return err
		}
		list := idList(ids)
		if _, err := q.ExecContext(ctx, s.q.remove, list); err != nil {
			return err
		}
		if _, err := q.ExecContext(ctx, s.q.dropDeps, list); err != nil {
			return err
		}
		n = len(ids)
		return s.settle(ctx, q, f)
	})
	if err != nil {
		return 0, err
	}
	s.hub.ready(f.queues)
	return n, nil
}

func (s *Store) pruneStats(ctx context.Context, keep time.Duration, limit int) (int, error) {
	n := 0
	err := s.write(ctx, func(ctx context.Context, q querier) error {
		n = 0
		var sum tally
		rows, err := q.QueryContext(ctx, s.q.expiredStats, micros(keep), limit)
		err = each(rows, err, func() error {
			var t tally
			if err := rows.Scan(&t.succeeded, &t.failed, &t.deleted, &t.retried); err != nil {
				return err
			}
			sum.succeeded += t.succeeded
			sum.failed += t.failed
			sum.deleted += t.deleted
			sum.retried += t.retried
			n++
			return nil
		})
		if err != nil || n == 0 {
			return err
		}
		_, err = q.ExecContext(ctx, s.q.count, totals, "", sum.succeeded, sum.failed, sum.deleted, sum.retried)
		return err
	})
	if err != nil {
		return 0, err
	}
	return n, nil
}

func (s *Store) pruneHolders(ctx context.Context, limit int) (int, error) {
	s.mu.Lock()
	after := s.cursors.holder
	s.mu.Unlock()
	var (
		seen int
		last []byte
		dead []any
	)
	rows, err := s.db.QueryContext(ctx, s.q.holderPage, after, limit)
	err = each(rows, err, func() error {
		var (
			key  []byte
			gone bool
		)
		if err := rows.Scan(&key, &gone); err != nil {
			return err
		}
		seen++
		last = key
		if gone {
			dead = append(dead, key)
		}
		return nil
	})
	if err != nil {
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
	n := 0
	err = s.write(ctx, func(ctx context.Context, q querier) error {
		n = 0
		for _, key := range dead {
			r, err := q.ExecContext(ctx, s.q.dropHolder, key)
			if err != nil {
				return err
			}
			k, err := r.RowsAffected()
			if err != nil {
				return err
			}
			n += int(k)
		}
		return nil
	})
	return n, err
}

package sqlitestore

import (
	"context"
	"database/sql"
	"fmt"
	"maps"
	"slices"

	"github.com/rafaelaugustos/kiln/driver"
)

const sqlStranded = `SELECT p.parent_id, j.state, EXISTS (
	SELECT 1 FROM {p}deps x WHERE x.batch = 0 AND x.resolved = 0 AND x.parent_id = p.parent_id AND x.mask & 2 <> 0)
FROM (
	SELECT DISTINCT parent_id FROM {p}deps WHERE batch = 0 AND resolved = 0 AND parent_id > ?
	ORDER BY parent_id LIMIT ?
) p
LEFT JOIN {p}jobs j ON j.id = p.parent_id
ORDER BY p.parent_id`

const sqlStrandedStates = `SELECT p.value, COALESCE(j.state, a.state, '')
FROM json_each(?) p
LEFT JOIN {p}jobs j ON j.id = p.value
LEFT JOIN {p}archive a ON a.id = p.value`

const sqlAwaiting = `SELECT id, deps_pending FROM {p}jobs WHERE state = 'awaiting' AND id > ? ORDER BY id LIMIT ?`

const sqlIdleBatches = `SELECT b.id FROM {p}batches b
WHERE b.finished_at IS NULL AND b.sealed AND NOT EXISTS (SELECT 1 FROM {p}jobs j WHERE j.batch_id = b.id)
ORDER BY b.id
LIMIT ?`

const sqlFinishedBatchDeps = `SELECT DISTINCT d.parent_id FROM {p}deps d
WHERE d.batch = 1 AND d.resolved = 0 AND NOT EXISTS (
	SELECT 1 FROM {p}batches b WHERE b.id = d.parent_id AND b.finished_at IS NULL)
ORDER BY d.parent_id
LIMIT ?`

const sqlThrottledKeys = `SELECT DISTINCT limit_key FROM {p}jobs WHERE state = 'throttled' ORDER BY limit_key LIMIT ?`

const sqlLimitPage = `SELECT limit_key, max, active FROM {p}limits WHERE limit_key > ? ORDER BY limit_key LIMIT ?`

const sqlActive = `SELECT limit_key, count(*) FROM {p}jobs
WHERE limit_key IN (SELECT value FROM json_each(?)) AND state IN ('enqueued', 'processing')
GROUP BY limit_key`

func (s *Store) Sweep(ctx context.Context, limit int) (int, error) {
	limit = max(limit, 1)
	total := 0
	for _, step := range []func(context.Context, int) (int, error){
		s.sweepDeps,
		s.sweepStuck,
		s.sweepBatches,
		s.sweepThrottled,
		s.reconcile,
	} {
		n, err := step(ctx, limit)
		total += n
		if err != nil {
			return total, fmt.Errorf("kiln: sweep: %w", err)
		}
	}
	return total, nil
}

func (s *Store) repair(ctx context.Context, fn func(ctx context.Context, q querier, f *fallout) error) (int, error) {
	var f *fallout
	err := s.write(ctx, func(ctx context.Context, q querier) error {
		f = &fallout{}
		if err := q.QueryRowContext(ctx, s.q.now).Scan(&f.now); err != nil {
			return err
		}
		if err := fn(ctx, q, f); err != nil {
			return err
		}
		return s.settle(ctx, q, f)
	})
	if err != nil {
		return 0, err
	}
	s.hub.ready(f.queues)
	return f.changed, nil
}

func (s *Store) sweepDeps(ctx context.Context, limit int) (int, error) {
	s.mu.Lock()
	after := s.cursors.parent
	s.mu.Unlock()
	var (
		stranded []int64
		seen     int
		last     int64
	)
	rows, err := s.db.QueryContext(ctx, s.q.stranded, after, limit)
	err = each(rows, err, func() error {
		var (
			id     int64
			state  sql.NullString
			onFail bool
		)
		if err := rows.Scan(&id, &state, &onFail); err != nil {
			return err
		}
		seen++
		last = id
		if !state.Valid || state.String == string(driver.Failed) && onFail {
			stranded = append(stranded, id)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	if seen < limit {
		last = 0
	}
	s.mu.Lock()
	s.cursors.parent = last
	s.mu.Unlock()
	if len(stranded) == 0 {
		return 0, nil
	}
	return s.repair(ctx, func(ctx context.Context, q querier, f *fallout) error {
		var parents []parent
		rows, err := q.QueryContext(ctx, s.q.strandedStates, idList(stranded))
		err = each(rows, err, func() error {
			var (
				id    int64
				state string
			)
			if err := rows.Scan(&id, &state); err != nil {
				return err
			}
			switch st := driver.State(state); st {
			case "":
				parents = append(parents, parent{id: id, state: driver.Deleted, label: "pruned"})
			case driver.Succeeded, driver.Failed, driver.Deleted:
				parents = append(parents, parent{id: id, state: st, label: state})
			}
			return nil
		})
		f.parents = parents
		f.changed += len(parents)
		return err
	})
}

func (s *Store) sweepStuck(ctx context.Context, limit int) (int, error) {
	s.mu.Lock()
	after := s.cursors.awaiting
	s.mu.Unlock()
	var (
		stuck []int64
		seen  int
		last  int64
	)
	rows, err := s.db.QueryContext(ctx, s.q.awaiting, after, limit)
	err = each(rows, err, func() error {
		var (
			id      int64
			pending int
		)
		if err := rows.Scan(&id, &pending); err != nil {
			return err
		}
		seen++
		last = id
		if pending <= 0 {
			stuck = append(stuck, id)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	if seen < limit {
		last = 0
	}
	s.mu.Lock()
	s.cursors.awaiting = last
	s.mu.Unlock()
	if len(stuck) == 0 {
		return 0, nil
	}
	return s.repair(ctx, func(ctx context.Context, q querier, f *fallout) error {
		return s.advance(ctx, q, f, stuck, nil, nil)
	})
}

func (s *Store) sweepBatches(ctx context.Context, limit int) (int, error) {
	idle, err := s.ids(ctx, s.q.idleBatches, limit)
	if err != nil {
		return 0, err
	}
	finished, err := s.ids(ctx, s.q.finishedBatchDeps, limit)
	if err != nil || len(idle)+len(finished) == 0 {
		return 0, err
	}
	return s.repair(ctx, func(ctx context.Context, q querier, f *fallout) error {
		f.batches = idle
		if len(finished) == 0 {
			return nil
		}
		return s.releaseBatches(ctx, q, f, finished)
	})
}

func (s *Store) ids(ctx context.Context, stmt string, args ...any) ([]int64, error) {
	var out []int64
	rows, err := s.db.QueryContext(ctx, stmt, args...)
	err = each(rows, err, func() error {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return err
		}
		out = append(out, id)
		return nil
	})
	return out, err
}

func (s *Store) sweepThrottled(ctx context.Context, limit int) (int, error) {
	var keys []string
	rows, err := s.db.QueryContext(ctx, s.q.throttledKeys, limit)
	err = each(rows, err, func() error {
		var key string
		if err := rows.Scan(&key); err != nil {
			return err
		}
		keys = append(keys, key)
		return nil
	})
	if err != nil || len(keys) == 0 {
		return 0, err
	}
	return s.repair(ctx, func(ctx context.Context, q querier, f *fallout) error {
		if _, err := q.ExecContext(ctx, s.q.restore, nameList(keys)); err != nil {
			return err
		}
		for _, key := range keys {
			f.throttled(key)
		}
		return nil
	})
}

func (s *Store) reconcile(ctx context.Context, limit int) (int, error) {
	s.mu.Lock()
	after := s.cursors.limit
	s.mu.Unlock()
	return s.repair(ctx, func(ctx context.Context, q querier, f *fallout) error {
		slots := make(map[string]*slot)
		rows, err := q.QueryContext(ctx, s.q.limitPage, after, limit)
		err = each(rows, err, func() error {
			var (
				key string
				sl  slot
			)
			if err := rows.Scan(&key, &sl.max, &sl.active); err != nil {
				return err
			}
			slots[key] = &sl
			return nil
		})
		if err != nil {
			return err
		}
		keys := slices.Sorted(maps.Keys(slots))
		next := ""
		if len(keys) == limit {
			next = keys[len(keys)-1]
		}
		s.mu.Lock()
		s.cursors.limit = next
		s.mu.Unlock()
		if len(keys) == 0 {
			return nil
		}
		counts := make(map[string]int, len(keys))
		rows, err = q.QueryContext(ctx, s.q.active, nameList(keys))
		err = each(rows, err, func() error {
			var (
				key string
				n   int
			)
			if err := rows.Scan(&key, &n); err != nil {
				return err
			}
			counts[key] = n
			return nil
		})
		if err != nil {
			return err
		}
		for key, sl := range slots {
			if sl.active != counts[key] {
				sl.active, sl.dirty = counts[key], true
				f.changed++
			}
		}
		return s.fill(ctx, q, slots, f)
	})
}

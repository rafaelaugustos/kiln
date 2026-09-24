package mysqlstore

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/rafaelaugustos/kiln/driver"
)

const sqlStranded = `SELECT p.parent_id, j.state, a.state, EXISTS (
	SELECT 1 FROM {p}deps x WHERE x.batch = FALSE AND x.resolved = FALSE AND x.parent_id = p.parent_id AND x.mask & 2 <> 0)
FROM (
	SELECT DISTINCT parent_id FROM {p}deps WHERE batch = FALSE AND resolved = FALSE AND parent_id > ?
	ORDER BY parent_id LIMIT ?
) p
LEFT JOIN {p}jobs j ON j.id = p.parent_id
LEFT JOIN {p}archive a ON a.id = p.parent_id
ORDER BY p.parent_id`

const sqlAwaiting = `SELECT id, deps_pending FROM {p}jobs FORCE INDEX (jobs_state)
WHERE state = 'awaiting' AND id > ? ORDER BY id LIMIT ?`

const sqlIdleBatches = `SELECT b.id FROM {p}batches b
WHERE b.finished_at IS NULL AND b.sealed AND NOT EXISTS (SELECT 1 FROM {p}jobs j WHERE j.batch_id = b.id)
ORDER BY b.id
LIMIT ?`

const sqlFinishedBatchDeps = `SELECT DISTINCT d.parent_id FROM {p}deps d
WHERE d.batch = TRUE AND d.resolved = FALSE AND NOT EXISTS (
	SELECT 1 FROM {p}batches b WHERE b.id = d.parent_id AND b.finished_at IS NULL)
ORDER BY d.parent_id
LIMIT ?`

const sqlThrottledKeys = `SELECT DISTINCT limit_key FROM {p}jobs WHERE state = 'throttled' ORDER BY limit_key LIMIT ?`

const sqlLimitPage = `SELECT limit_key, max, active FROM {p}limits WHERE limit_key > ? ORDER BY limit_key LIMIT ?
FOR UPDATE SKIP LOCKED`

const sqlActive = `SELECT limit_key, COUNT(*) FROM {p}jobs WHERE state IN ('enqueued', 'processing') AND limit_key IN (?)
GROUP BY limit_key`

func (s *Store) Sweep(ctx context.Context, limit int) (int, error) {
	limit = max(limit, 1)
	steps := []func(context.Context, int) (int, error){
		s.sweepDeps,
		s.sweepStuck,
		s.sweepBatches,
		s.sweepThrottled,
		s.reconcile,
	}
	total := 0
	for _, step := range steps {
		n, err := step(ctx, limit)
		total += n
		if err != nil {
			return total, fmt.Errorf("kiln: sweep: %w", err)
		}
	}
	return total, nil
}

func (s *Store) sweepDeps(ctx context.Context, limit int) (int, error) {
	s.mu.Lock()
	after := s.cursors.parent
	s.mu.Unlock()
	rows, err := s.db.QueryContext(ctx, render(s.q.stranded, after, limit))
	if err != nil {
		return 0, err
	}
	var (
		stranded []int64
		seen     int
		last     int64
	)
	for rows.Next() {
		var (
			id       int64
			live     sql.NullString
			archived sql.NullString
			onFail   bool
		)
		if err := rows.Scan(&id, &live, &archived, &onFail); err != nil {
			rows.Close()
			return 0, err
		}
		seen++
		last = id
		if !live.Valid || live.String == string(driver.Failed) && onFail {
			stranded = append(stranded, id)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(stranded) == 0 {
		if seen < limit {
			last = 0
		}
		s.mu.Lock()
		s.cursors.parent = last
		s.mu.Unlock()
		return 0, nil
	}
	changed := 0
	err = s.txn(ctx, func(tx *sql.Tx) error {
		changed = 0
		rows, err := tx.QueryContext(ctx, render(s.q.lockParents, stranded, stranded)+s.q.linkedNow)
		if err != nil {
			return err
		}
		f := &fallout{}
		states := make(map[int64]driver.State, len(stranded))
		for rows.Next() {
			var (
				tag, state string
				id         int64
				now        stamp
			)
			if err := rows.Scan(&tag, &id, &state, &now); err != nil {
				rows.Close()
				return err
			}
			switch _, seen := states[id]; {
			case tag == "n":
				f.now = now.Time
			case !seen:
				states[id] = driver.State(state)
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		for _, id := range stranded {
			switch st, ok := states[id]; {
			case !ok:
				f.parents = append(f.parents, parent{id: id, state: driver.Deleted, label: "pruned"})
			case st == driver.Succeeded || st == driver.Failed || st == driver.Deleted:
				f.parents = append(f.parents, parent{id: id, state: st, label: string(st)})
			}
		}
		if len(f.parents) == 0 {
			return nil
		}
		if err := s.settle(ctx, tx, f); err != nil {
			return err
		}
		changed = f.changed + len(f.parents)
		return nil
	})
	return changed, err
}

func (s *Store) sweepStuck(ctx context.Context, limit int) (int, error) {
	s.mu.Lock()
	after := s.cursors.awaiting
	s.mu.Unlock()
	rows, err := s.db.QueryContext(ctx, render(s.q.awaiting, after, limit))
	if err != nil {
		return 0, err
	}
	var (
		stuck []int64
		seen  int
		last  int64
	)
	for rows.Next() {
		var (
			id      int64
			pending int
		)
		if err := rows.Scan(&id, &pending); err != nil {
			rows.Close()
			return 0, err
		}
		seen++
		last = id
		if pending <= 0 {
			stuck = append(stuck, id)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
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
	return s.repair(ctx, func(tx *sql.Tx, f *fallout) error {
		return s.advance(ctx, tx, f, stuck, nil, nil)
	})
}

func (s *Store) repair(ctx context.Context, fn func(tx *sql.Tx, f *fallout) error) (int, error) {
	changed := 0
	err := s.txn(ctx, func(tx *sql.Tx) error {
		var now stamp
		if err := tx.QueryRowContext(ctx, sqlNow).Scan(&now); err != nil {
			return err
		}
		f := &fallout{now: now.Time}
		if err := fn(tx, f); err != nil {
			return err
		}
		if err := s.settle(ctx, tx, f); err != nil {
			return err
		}
		changed = f.changed
		return nil
	})
	return changed, err
}

func (s *Store) sweepBatches(ctx context.Context, limit int) (int, error) {
	idle, err := s.ids(ctx, render(s.q.idleBatches, limit))
	if err != nil {
		return 0, err
	}
	finished, err := s.ids(ctx, render(s.q.finishedBatchDeps, limit))
	if err != nil || len(idle)+len(finished) == 0 {
		return 0, err
	}
	return s.repair(ctx, func(tx *sql.Tx, f *fallout) error {
		f.batches = idle
		if len(finished) == 0 {
			return nil
		}
		return s.releaseBatches(ctx, tx, f, finished)
	})
}

func (s *Store) ids(ctx context.Context, stmt string) ([]int64, error) {
	rows, err := s.db.QueryContext(ctx, stmt)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

func (s *Store) sweepThrottled(ctx context.Context, limit int) (int, error) {
	rows, err := s.db.QueryContext(ctx, render(s.q.throttledKeys, limit))
	if err != nil {
		return 0, err
	}
	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			rows.Close()
			return 0, err
		}
		keys = append(keys, key)
	}
	rows.Close()
	if err := rows.Err(); err != nil || len(keys) == 0 {
		return 0, err
	}
	floor := make(map[string]int, len(keys))
	for _, key := range keys {
		floor[key] = 1
	}
	n := 0
	err = s.txn(ctx, func(tx *sql.Tx) error {
		slots, err := s.lockLimits(ctx, tx, keys, true)
		if err != nil {
			return err
		}
		if err := s.restore(ctx, tx, keys, floor, slots); err != nil {
			return err
		}
		a, err := s.fill(ctx, tx, slots)
		n = len(a.ids)
		return err
	})
	return n, err
}

func (s *Store) reconcile(ctx context.Context, limit int) (int, error) {
	s.mu.Lock()
	after := s.cursors.limit
	s.mu.Unlock()
	n := 0
	err := s.txn(ctx, func(tx *sql.Tx) error {
		n = 0
		rows, err := tx.QueryContext(ctx, render(s.q.limitPage, after, limit))
		if err != nil {
			return err
		}
		slots := make(map[string]*slot)
		var keys []string
		for rows.Next() {
			var (
				key string
				sl  slot
			)
			if err := rows.Scan(&key, &sl.max, &sl.active); err != nil {
				rows.Close()
				return err
			}
			slots[key] = &sl
			keys = append(keys, key)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
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
		rows, err = tx.QueryContext(ctx, render(s.q.active, keys))
		if err != nil {
			return err
		}
		counts := make(map[string]int, len(keys))
		for rows.Next() {
			var (
				key   string
				count int
			)
			if err := rows.Scan(&key, &count); err != nil {
				rows.Close()
				return err
			}
			counts[key] = count
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		for key, sl := range slots {
			if sl.active != counts[key] {
				sl.active, sl.dirty = counts[key], true
				n++
			}
		}
		a, err := s.fill(ctx, tx, slots)
		n += len(a.ids)
		return err
	})
	return n, err
}

package mssqlstore

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/rafaelaugustos/kiln/driver"
)

const sqlStranded = `SELECT p.parent_id, j.state, a.state, CASE WHEN EXISTS (
	SELECT 1 FROM {p}deps x WHERE x.batch = 0 AND x.resolved = 0 AND x.parent_id = p.parent_id AND x.mask & 2 <> 0)
	THEN 1 ELSE 0 END
FROM (
	SELECT DISTINCT TOP (@n) parent_id FROM {p}deps WHERE batch = 0 AND resolved = 0 AND parent_id > @after
	ORDER BY parent_id
) p
LEFT JOIN {p}jobs j ON j.id = p.parent_id
LEFT JOIN {p}archive a ON a.id = p.parent_id
ORDER BY p.parent_id`

const sqlStrandedStates = `SELECT 'j', j.id, j.state FROM OPENJSON(@parents) WITH (id BIGINT '$') v
JOIN {p}jobs j WITH (REPEATABLEREAD, ROWLOCK, FORCESEEK) ON j.id = v.id
UNION ALL
SELECT 'a', a.id, a.state FROM {p}archive a WHERE a.id IN (SELECT id FROM OPENJSON(@parents) WITH (id BIGINT '$'))`

const sqlAwaiting = `SELECT TOP (@n) id, deps_pending FROM {p}jobs WITH (INDEX(jobs_state))
WHERE state = 'awaiting' AND id > @after ORDER BY id`

const sqlIdleBatches = `SELECT TOP (@n) b.id FROM {p}batches b
WHERE b.finished_at IS NULL AND b.sealed = 1 AND NOT EXISTS (SELECT 1 FROM {p}jobs j WHERE j.batch_id = b.id)
	AND NOT ` + unfinishedNested + `
ORDER BY b.id`

const sqlFinishedBatchDeps = `SELECT DISTINCT TOP (@n) d.parent_id FROM {p}deps d
WHERE d.batch = 1 AND d.resolved = 0 AND NOT EXISTS (
	SELECT 1 FROM {p}batches b WHERE b.id = d.parent_id AND b.finished_at IS NULL)
ORDER BY d.parent_id`

const sqlThrottledKeys = `SELECT DISTINCT TOP (@n) limit_key FROM {p}jobs WHERE state = 'throttled' ORDER BY limit_key`

const sqlLimitPage = `SELECT TOP (@n) ` + limitColumns + ` FROM {p}limits l WITH (UPDLOCK, READPAST, ROWLOCK)
WHERE l.limit_key > @after ORDER BY l.limit_key`

const sqlActive = `SELECT j.limit_key, COUNT(*) FROM ` + keyRows + `
JOIN {p}jobs j WITH (FORCESEEK) ON j.state IN ('enqueued', 'processing') AND j.limit_key = v.k COLLATE Latin1_General_100_BIN2
GROUP BY j.limit_key`

// Sweep repairs what other methods leave to it, as [driver.Coordinator.Sweep] describes, except
// that limit caps what each of its steps takes on rather than the rows they change in all, so the
// count it returns can exceed limit. Each step writes in a short transaction of its own, and Sweep
// keeps its place among the parents of unresolved dependencies, the awaiting jobs and the limit
// keys between calls, so successive calls on the same Store go through all of them.
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
	rows, err := s.db.QueryContext(ctx, s.q.stranded, sql.Named("n", limit), sql.Named("after", after))
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
	var f *fallout
	err = s.txn(ctx, func(tx *sql.Tx) error {
		var now moment
		if err := tx.QueryRowContext(ctx, sqlNow).Scan(&now); err != nil {
			return err
		}
		f = &fallout{now: now.Time}
		rows, err := tx.QueryContext(ctx, s.q.strandedStates, sql.Named("parents", idList(stranded)))
		if err != nil {
			return err
		}
		states := make(map[int64]driver.State, len(stranded))
		for rows.Next() {
			var (
				tag, state string
				id         int64
			)
			if err := rows.Scan(&tag, &id, &state); err != nil {
				rows.Close()
				return err
			}
			if _, seen := states[id]; !seen {
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
		return s.settle(ctx, tx, f)
	})
	if err != nil {
		return 0, err
	}
	s.nt.ready(f.queues)
	return f.changed + len(f.parents), nil
}

func (s *Store) sweepStuck(ctx context.Context, limit int) (int, error) {
	s.mu.Lock()
	after := s.cursors.awaiting
	s.mu.Unlock()
	rows, err := s.db.QueryContext(ctx, s.q.awaiting, sql.Named("n", limit), sql.Named("after", after))
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
	var f *fallout
	err := s.txn(ctx, func(tx *sql.Tx) error {
		var now moment
		if err := tx.QueryRowContext(ctx, sqlNow).Scan(&now); err != nil {
			return err
		}
		f = &fallout{now: now.Time}
		if err := fn(tx, f); err != nil {
			return err
		}
		return s.settle(ctx, tx, f)
	})
	if err != nil {
		return 0, err
	}
	s.nt.ready(f.queues)
	return f.changed, nil
}

func (s *Store) sweepBatches(ctx context.Context, limit int) (int, error) {
	idle, err := s.ids(ctx, s.q.idleBatches, sql.Named("n", limit))
	if err != nil {
		return 0, err
	}
	finished, err := s.ids(ctx, s.q.finishedBatchDeps, sql.Named("n", limit))
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

func (s *Store) ids(ctx context.Context, stmt string, args ...any) ([]int64, error) {
	rows, err := s.db.QueryContext(ctx, stmt, args...)
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
	rows, err := s.db.QueryContext(ctx, s.q.throttledKeys, sql.Named("n", limit))
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
	floor := make(map[string]rule, len(keys))
	for _, key := range keys {
		floor[key] = rule{max: 1}
	}
	var a admitted
	err = s.txn(ctx, func(tx *sql.Tx) error {
		slots, err := s.lockLimits(ctx, tx, keys, true)
		if err != nil {
			return err
		}
		if err := s.restore(ctx, tx, keys, floor, slots); err != nil {
			return err
		}
		a, err = s.fill(ctx, tx, slots)
		return err
	})
	if err != nil {
		return 0, err
	}
	s.nt.ready(a.queues)
	return a.changed(), nil
}

func (s *Store) reconcile(ctx context.Context, limit int) (int, error) {
	s.mu.Lock()
	after := s.cursors.limit
	s.mu.Unlock()
	var (
		n      int
		queues []string
	)
	err := s.txn(ctx, func(tx *sql.Tx) error {
		n, queues = 0, nil
		rows, err := tx.QueryContext(ctx, s.q.limitPage, sql.Named("n", limit), sql.Named("after", after))
		if err != nil {
			return err
		}
		slots := make(map[string]*slot)
		var keys []string
		for rows.Next() {
			var key string
			sl := &slot{}
			if err := sl.scan(rows, &key); err != nil {
				rows.Close()
				return err
			}
			slots[key] = sl
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
		rows, err = tx.QueryContext(ctx, s.q.active, sql.Named("keys", stringList(keys)))
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
		if err != nil {
			return err
		}
		n += a.changed()
		queues = a.queues
		return nil
	})
	if err != nil {
		return 0, err
	}
	s.nt.ready(queues)
	return n, nil
}

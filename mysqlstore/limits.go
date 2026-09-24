package mysqlstore

import (
	"context"
	"database/sql"
	"maps"
	"slices"
)

const sqlLockLimits = `SELECT limit_key, max, active FROM {p}limits FORCE INDEX (PRIMARY)
WHERE limit_key IN (?) ORDER BY limit_key FOR UPDATE`

const sqlWaiting = `(SELECT id, queue, limit_key FROM {p}jobs FORCE INDEX (jobs_throttled)
	WHERE state = 'throttled' AND limit_key = ?
	ORDER BY priority DESC, id LIMIT ? FOR UPDATE SKIP LOCKED)`

const sqlEnqueue = `UPDATE {p}jobs SET state = 'enqueued' WHERE id IN (?)`

const sqlDeclared = `SELECT limit_key, max FROM {p}limits WHERE limit_key IN (?)`

const sqlDeclare = `INSERT INTO {p}limits (limit_key, max, declared_at) VALUES `

const sqlDeclareTail = ` AS n ON DUPLICATE KEY UPDATE max = n.max, declared_at = COALESCE(n.declared_at, {p}limits.declared_at)`

const sqlEnsureTail = ` AS n ON DUPLICATE KEY UPDATE limit_key = {p}limits.limit_key`

type slot struct {
	max    int
	active int
	dirty  bool
}

func (s *Store) lockLimits(ctx context.Context, q querier, keys []string, skip bool) (map[string]*slot, error) {
	stmt := render(s.q.lockLimits, keys)
	if skip {
		stmt += " SKIP LOCKED"
	}
	rows, err := q.QueryContext(ctx, stmt)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	slots := make(map[string]*slot, len(keys))
	for rows.Next() {
		var (
			key string
			sl  slot
		)
		if err := rows.Scan(&key, &sl.max, &sl.active); err != nil {
			return nil, err
		}
		slots[key] = &sl
	}
	return slots, rows.Err()
}

type admitted struct {
	ids    []int64
	queues []string
}

func (s *Store) fill(ctx context.Context, q querier, slots map[string]*slot) (admitted, error) {
	var a admitted
	keys := slices.Sorted(maps.Keys(slots))
	var b []byte
	for _, key := range keys {
		sl := slots[key]
		if free := sl.max - sl.active; free > 0 {
			if b != nil {
				b = append(b, " UNION ALL "...)
			}
			b = appendSQL(b, s.q.waiting, key, free)
		}
	}
	if b != nil {
		rows, err := q.QueryContext(ctx, string(b))
		if err != nil {
			return a, err
		}
		for rows.Next() {
			var id int64
			var queue, key string
			if err := rows.Scan(&id, &queue, &key); err != nil {
				rows.Close()
				return a, err
			}
			a.ids = append(a.ids, id)
			if !slices.Contains(a.queues, queue) {
				a.queues = append(a.queues, queue)
			}
			sl := slots[key]
			sl.active++
			sl.dirty = true
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return a, err
		}
	}
	if len(a.ids) > 0 {
		slices.Sort(a.ids)
		if _, err := q.ExecContext(ctx, render(s.q.enqueue, a.ids)); err != nil {
			return a, err
		}
	}
	var dirty []string
	for _, key := range keys {
		if slots[key].dirty {
			dirty = append(dirty, key)
		}
	}
	if len(dirty) == 0 {
		return a, nil
	}
	b = append(b[:0], "UPDATE "...)
	b = append(b, s.prefix...)
	b = append(b, "limits SET active = CASE limit_key"...)
	for _, key := range dirty {
		b = appendSQL(b, " WHEN ? THEN ?", key, max(slots[key].active, 0))
	}
	b = appendSQL(b, " END WHERE limit_key IN (?)", dirty)
	_, err := q.ExecContext(ctx, string(b))
	return a, err
}

func (s *Store) declare(ctx context.Context, keys []string, maxes map[string]int, external bool) error {
	if !external {
		rows, err := s.db.QueryContext(ctx, render(s.q.declared, keys))
		if err != nil {
			return err
		}
		same := make(map[string]bool, len(keys))
		for rows.Next() {
			var (
				key string
				n   int
			)
			if err := rows.Scan(&key, &n); err != nil {
				rows.Close()
				return err
			}
			same[key] = n == maxes[key]
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		keys = slices.DeleteFunc(slices.Clone(keys), func(k string) bool { return same[k] })
		if len(keys) == 0 {
			return nil
		}
	}
	if external {
		_, err := s.side.exec(ctx, s.limitRows(s.q.declareTail, keys, maxes, "UTC_TIMESTAMP(6)"))
		return err
	}
	_, err := s.db.ExecContext(ctx, s.limitRows(s.q.declareTail, keys, maxes, "NULL"))
	return err
}

func (s *Store) limitRows(tail string, keys []string, maxes map[string]int, at raw) string {
	b := make([]byte, 0, 48*len(keys)+160)
	b = append(b, s.q.declare...)
	for i, key := range keys {
		if i > 0 {
			b = append(b, ',')
		}
		b = appendSQL(b, "(?, ?, ?)", key, maxes[key], at)
	}
	return string(append(b, tail...))
}

func (s *Store) restore(ctx context.Context, q querier, keys []string, maxes map[string]int, slots map[string]*slot) error {
	var gone []string
	for _, key := range keys {
		if slots[key] == nil {
			gone = append(gone, key)
		}
	}
	if len(gone) == 0 {
		return nil
	}
	rows, err := q.QueryContext(ctx, render(s.q.declared, gone))
	if err != nil {
		return err
	}
	held := make(map[string]bool, len(gone))
	for rows.Next() {
		var (
			key string
			n   int
		)
		if err := rows.Scan(&key, &n); err != nil {
			rows.Close()
			return err
		}
		held[key] = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	if gone = slices.DeleteFunc(gone, func(k string) bool { return held[k] }); len(gone) == 0 {
		return nil
	}
	if _, err := q.ExecContext(ctx, s.limitRows(s.q.ensureTail, gone, maxes, "NULL")); err != nil {
		return err
	}
	more, err := s.lockLimits(ctx, q, gone, false)
	if err != nil {
		return err
	}
	maps.Copy(slots, more)
	return nil
}

func (s *Store) admitKeys(ctx context.Context, maxes map[string]int) (admitted, error) {
	keys := slices.Sorted(maps.Keys(maxes))
	var a admitted
	err := s.txn(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, s.limitRows(s.q.ensureTail, keys, maxes, "NULL")); err != nil {
			return err
		}
		slots, err := s.lockLimits(ctx, tx, keys, false)
		if err != nil {
			return err
		}
		a, err = s.fill(ctx, tx, slots)
		return err
	})
	return a, err
}

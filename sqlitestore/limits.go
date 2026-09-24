package sqlitestore

import (
	"context"
	"maps"
	"slices"
	"strconv"
)

const sqlSlots = `SELECT limit_key, max, active FROM {p}limits WHERE limit_key IN (SELECT value FROM json_each(?))`

const sqlAdmit = `UPDATE {p}jobs SET state = 'enqueued'
WHERE id IN (
	SELECT id FROM {p}jobs WHERE state = 'throttled' AND limit_key = ? ORDER BY priority DESC, id LIMIT ?)
RETURNING id, queue`

const sqlActivate = `UPDATE {p}limits AS l SET active = v.value FROM json_each(?) AS v WHERE l.limit_key = v.key`

const sqlDeclare = `INSERT INTO {p}limits (limit_key, max) SELECT key, value FROM json_each(?) WHERE true
ON CONFLICT (limit_key) DO UPDATE SET max = excluded.max WHERE max <> excluded.max`

const sqlRestore = `INSERT INTO {p}limits (limit_key, max) SELECT value, 1 FROM json_each(?) WHERE true
ON CONFLICT (limit_key) DO NOTHING`

type slot struct {
	max    int
	active int
	dirty  bool
}

func (s *Store) slots(ctx context.Context, q querier, keys []string) (map[string]*slot, error) {
	slots := make(map[string]*slot, len(keys))
	rows, err := q.QueryContext(ctx, s.q.slots, nameList(keys))
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
	return slots, err
}

func (s *Store) fill(ctx context.Context, q querier, slots map[string]*slot, f *fallout) error {
	var active map[string]int
	for _, key := range slices.Sorted(maps.Keys(slots)) {
		sl := slots[key]
		if free := sl.max - sl.active; free > 0 {
			rows, err := q.QueryContext(ctx, s.q.admit, key, free)
			err = each(rows, err, func() error {
				var (
					id    int64
					queue string
				)
				if err := rows.Scan(&id, &queue); err != nil {
					return err
				}
				f.admitted = append(f.admitted, id)
				f.queue(queue)
				f.changed++
				sl.active++
				sl.dirty = true
				return nil
			})
			if err != nil {
				return err
			}
		}
		if sl.dirty {
			if active == nil {
				active = make(map[string]int)
			}
			active[key] = sl.active
		}
	}
	if active == nil {
		return nil
	}
	_, err := q.ExecContext(ctx, s.q.activate, object(active))
	return err
}

func object(m map[string]int) string {
	b := make([]byte, 0, 24*len(m)+2)
	b = append(b, '{')
	for i, k := range slices.Sorted(maps.Keys(m)) {
		if i > 0 {
			b = append(b, ',')
		}
		b = appendJSONString(b, k)
		b = append(b, ':')
		b = strconv.AppendInt(b, int64(m[k]), 10)
	}
	return string(append(b, '}'))
}

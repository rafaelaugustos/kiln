package sqlitestore

import (
	"context"
	"database/sql"
	"maps"
	"slices"
	"strconv"

	"github.com/rafaelaugustos/kiln/driver"
)

const paceLimit = 1000

const slotColumns = `limit_key, max, active, rate, per_us, burst, tat`

const sqlSlots = `SELECT ` + slotColumns + ` FROM {p}limits WHERE limit_key IN (SELECT value FROM json_each(?))`

const sqlAdmit = `UPDATE {p}jobs SET state = 'enqueued', granted = 0
WHERE id IN (
	SELECT id FROM {p}jobs WHERE state = 'throttled' AND limit_key = ? ORDER BY priority DESC, id LIMIT ?)
RETURNING id, queue`

const sqlWaiting = `SELECT id, queue, granted FROM {p}jobs WHERE state = 'throttled' AND limit_key = ?
ORDER BY priority DESC, id
LIMIT ?`

const sqlEnqueue = `UPDATE {p}jobs SET state = 'enqueued', granted = 0 WHERE id IN (SELECT value FROM json_each(?))`

const sqlReserve = `UPDATE {p}jobs SET state = 'scheduled', run_at = ?, granted = 1 WHERE id = ?`

const sqlActivate = `UPDATE {p}limits AS l SET active = v.value ->> 0, tat = v.value ->> 1
FROM json_each(?) AS v WHERE l.limit_key = v.key`

const sqlDeclare = `INSERT INTO {p}limits (limit_key, max, rate, per_us, burst)
SELECT key, value ->> 0, value ->> 1, value ->> 2, value ->> 3 FROM json_each(?) WHERE true
ON CONFLICT (limit_key) DO UPDATE SET max = excluded.max, rate = excluded.rate, per_us = excluded.per_us,
	burst = excluded.burst
WHERE max <> excluded.max OR rate <> excluded.rate OR per_us <> excluded.per_us OR burst <> excluded.burst`

const sqlRestore = `INSERT INTO {p}limits (limit_key, max) SELECT value, 1 FROM json_each(?) WHERE true
ON CONFLICT (limit_key) DO NOTHING`

type rule struct {
	max   int
	rate  int
	per   int64
	burst int
}

func ruleOf(p *driver.InsertParams) rule {
	return rule{max: p.LimitMax, rate: p.LimitRate, per: micros(p.LimitPer), burst: p.LimitBurst}
}

type slot struct {
	rule
	active   int
	tat      sql.NullInt64
	dirty    bool
	admitted []int64
	reserved []int64
}

func (sl *slot) full() bool {
	return sl.max > 0 && sl.active >= sl.max
}

func scanSlot(rows *sql.Rows) (string, *slot, error) {
	var (
		key string
		sl  slot
	)
	err := rows.Scan(&key, &sl.max, &sl.active, &sl.rate, &sl.per, &sl.burst, &sl.tat)
	return key, &sl, err
}

func (s *Store) slots(ctx context.Context, q querier, keys []string) (map[string]*slot, error) {
	slots := make(map[string]*slot, len(keys))
	rows, err := q.QueryContext(ctx, s.q.slots, nameList(keys))
	err = each(rows, err, func() error {
		key, sl, err := scanSlot(rows)
		if err == nil {
			slots[key] = sl
		}
		return err
	})
	return slots, err
}

func (s *Store) fill(ctx context.Context, q querier, slots map[string]*slot, f *fallout) error {
	var dirty []string
	for _, key := range slices.Sorted(maps.Keys(slots)) {
		sl := slots[key]
		var err error
		switch {
		case sl.rate > 0:
			err = s.pace(ctx, q, key, sl, f)
		case sl.active < sl.max:
			err = s.admit(ctx, q, key, sl, f)
		}
		if err != nil {
			return err
		}
		if sl.dirty {
			dirty = append(dirty, key)
		}
	}
	if dirty == nil {
		return nil
	}
	_, err := q.ExecContext(ctx, s.q.activate, activation(slots, dirty))
	return err
}

func (s *Store) admit(ctx context.Context, q querier, key string, sl *slot, f *fallout) error {
	rows, err := q.QueryContext(ctx, s.q.admit, key, sl.max-sl.active)
	return each(rows, err, func() error {
		var (
			id    int64
			queue string
		)
		if err := rows.Scan(&id, &queue); err != nil {
			return err
		}
		sl.admitted = append(sl.admitted, id)
		f.queue(queue)
		f.changed++
		sl.active++
		sl.dirty = true
		return nil
	})
}

func (s *Store) pace(ctx context.Context, q querier, key string, sl *slot, f *fallout) error {
	reserved, err := s.walk(ctx, q, key, sl, f)
	if err != nil {
		return err
	}
	if sl.admitted != nil {
		if _, err := q.ExecContext(ctx, s.q.enqueue, idList(sl.admitted)); err != nil {
			return err
		}
	}
	return execEach(ctx, q, s.q.reserve, 2, reserved)
}

func (s *Store) walk(ctx context.Context, q querier, key string, sl *slot, f *fallout) ([]any, error) {
	rows, err := q.QueryContext(ctx, s.q.waiting, key, paceLimit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var reserved []any
	gap := sl.per / int64(sl.rate)
	tau := int64(sl.burst-1) * gap
	tat := sl.tat.Int64
	if !sl.tat.Valid {
		tat = f.now
	}
	for rows.Next() {
		var (
			id      int64
			queue   string
			granted bool
		)
		if err := rows.Scan(&id, &queue, &granted); err != nil {
			return nil, err
		}
		at := f.now
		if !granted {
			at = max(at, tat-tau)
		}
		if at <= f.now && sl.full() {
			break
		}
		if !granted {
			tat = max(tat, at) + gap
			sl.tat = sql.NullInt64{Int64: tat, Valid: true}
		}
		if at > f.now {
			reserved = append(reserved, at, id)
			sl.reserved = append(sl.reserved, id)
		} else {
			sl.admitted = append(sl.admitted, id)
			sl.active++
		}
		sl.dirty = true
		f.queue(queue)
		f.changed++
	}
	return reserved, rows.Err()
}

func activation(slots map[string]*slot, keys []string) string {
	b := make([]byte, 0, 40*len(keys)+2)
	b = append(b, '{')
	for i, key := range keys {
		sl := slots[key]
		if i > 0 {
			b = append(b, ',')
		}
		b = appendJSONString(b, key)
		b = append(b, ":["...)
		b = strconv.AppendInt(b, int64(sl.active), 10)
		b = append(b, ',')
		if sl.tat.Valid {
			b = strconv.AppendInt(b, sl.tat.Int64, 10)
		} else {
			b = append(b, "null"...)
		}
		b = append(b, ']')
	}
	return string(append(b, '}'))
}

func declaration(rules map[string]rule, keys []string) string {
	b := make([]byte, 0, 48*len(keys)+2)
	b = append(b, '{')
	for i, key := range keys {
		r := rules[key]
		if i > 0 {
			b = append(b, ',')
		}
		b = appendJSONString(b, key)
		b = append(b, ":["...)
		for j, n := range [...]int64{int64(r.max), int64(r.rate), r.per, int64(r.burst)} {
			if j > 0 {
				b = append(b, ',')
			}
			b = strconv.AppendInt(b, n, 10)
		}
		b = append(b, ']')
	}
	return string(append(b, '}'))
}

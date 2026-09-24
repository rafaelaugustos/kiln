package mysqlstore

import (
	"cmp"
	"context"
	"database/sql"
	"maps"
	"slices"
	"time"
)

const admitBatch = 1000

const sqlWaiting = `(SELECT UTC_TIMESTAMP(6), id, queue, limit_key, priority, granted FROM {p}jobs FORCE INDEX (jobs_throttled)
	WHERE state = 'throttled' AND limit_key = ??
	ORDER BY priority DESC, id LIMIT ? FOR UPDATE SKIP LOCKED)`

const sqlAfter = ` AND (priority < ? OR priority = ? AND id > ?)`

const sqlEnqueue = `UPDATE {p}jobs SET state = 'enqueued', granted = FALSE WHERE id IN (?)`

const sqlReserveTail = `) AS v (id, run_at) STRAIGHT_JOIN {p}jobs j FORCE INDEX (PRIMARY) ON j.id = v.id
SET j.state = IF(v.run_at IS NULL, 'enqueued', 'scheduled'), j.run_at = COALESCE(v.run_at, j.run_at),
	j.granted = v.run_at IS NOT NULL`

func (r rule) interval() time.Duration {
	return time.Duration(r.per) * time.Microsecond / time.Duration(r.rate)
}

type slot struct {
	rule
	active int
	tat    time.Time
	dirty  bool
	paced  bool
}

func (sl *slot) scan(rows *sql.Rows, key *string) error {
	var tat stamp
	if err := rows.Scan(key, &sl.max, &sl.active, &sl.rate, &sl.per, &sl.burst, &tat); err != nil {
		return err
	}
	sl.tat = tat.Time
	return nil
}

func (sl *slot) want() int {
	switch {
	case sl.max > 0 && sl.rate == 0:
		return sl.max - sl.active
	case sl.max > 0:
		return min(max(sl.max-sl.active, 0)+1, admitBatch)
	}
	return admitBatch
}

type verdict uint8

const (
	admit verdict = iota
	reserve
	hold
)

func (sl *slot) decide(now time.Time, granted bool) (verdict, time.Time) {
	paced := sl.rate > 0 && !granted
	at := now
	if paced {
		tau := time.Duration(max(sl.burst-1, 0)) * sl.interval()
		if t := sl.tat.Add(-tau); t.After(at) {
			at = t
		}
	}
	switch {
	case at.After(now):
		sl.take(at)
		return reserve, at
	case sl.max > 0 && sl.active >= sl.max:
		return hold, at
	}
	if paced {
		sl.take(at)
	}
	sl.active++
	sl.dirty = true
	return admit, at
}

func (sl *slot) take(at time.Time) {
	if at.After(sl.tat) {
		sl.tat = at
	}
	sl.tat = sl.tat.Add(sl.interval())
	sl.paced = true
}

type admitted struct {
	ids      []int64
	queues   []string
	reserved []move
}

func (a admitted) changed() int {
	return len(a.ids) + len(a.reserved)
}

type waiter struct {
	id       int64
	queue    string
	priority int16
	granted  bool
}

type walk struct {
	want int
	seen int
	now  time.Time
	last waiter
}

type move struct {
	id int64
	at time.Time
}

func (s *Store) fill(ctx context.Context, q querier, slots map[string]*slot) (admitted, error) {
	var a admitted
	pending := make(map[string]*walk, len(slots))
	for key, sl := range slots {
		if n := sl.want(); n > 0 {
			pending[key] = &walk{want: n}
		}
	}
	for len(pending) > 0 {
		got, err := s.waiting(ctx, q, pending)
		if err != nil {
			return a, err
		}
		more := make(map[string]*walk)
		for _, key := range slices.Sorted(maps.Keys(pending)) {
			w, sl, js := pending[key], slots[key], got[key]
			held := false
			for _, j := range js {
				v, at := sl.decide(w.now, j.granted)
				if v == hold {
					held = true
					break
				}
				if v == reserve {
					a.reserved = append(a.reserved, move{id: j.id, at: at})
				} else {
					a.ids = append(a.ids, j.id)
					if !slices.Contains(a.queues, j.queue) {
						a.queues = append(a.queues, j.queue)
					}
				}
				w.seen++
			}
			if sl.rate > 0 && !held && len(js) == w.want && w.seen < admitBatch {
				w.want, w.last = admitBatch-w.seen, js[len(js)-1]
				more[key] = w
			}
		}
		pending = more
	}
	slices.Sort(a.ids)
	if err := s.place(ctx, q, a); err != nil {
		return a, err
	}
	return a, s.account(ctx, q, slots)
}

func (s *Store) waiting(ctx context.Context, q querier, pending map[string]*walk) (map[string][]waiter, error) {
	var b []byte
	for i, key := range slices.Sorted(maps.Keys(pending)) {
		w := pending[key]
		if i > 0 {
			b = append(b, " UNION ALL "...)
		}
		var after raw
		if w.seen > 0 {
			after = raw(render(sqlAfter, w.last.priority, w.last.priority, w.last.id))
		}
		b = appendSQL(b, s.q.waiting, key, after, w.want)
	}
	rows, err := q.QueryContext(ctx, string(b))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	got := make(map[string][]waiter, len(pending))
	for rows.Next() {
		var (
			now stamp
			key string
			j   waiter
		)
		if err := rows.Scan(&now, &j.id, &j.queue, &key, &j.priority, &j.granted); err != nil {
			return nil, err
		}
		if w := pending[key]; w.now.IsZero() {
			w.now = now.Time
		}
		got[key] = append(got[key], j)
	}
	for _, js := range got {
		slices.SortFunc(js, func(a, b waiter) int {
			return cmp.Or(cmp.Compare(b.priority, a.priority), cmp.Compare(a.id, b.id))
		})
	}
	return got, rows.Err()
}

func (s *Store) place(ctx context.Context, q querier, a admitted) error {
	if len(a.reserved) == 0 {
		if len(a.ids) == 0 {
			return nil
		}
		_, err := q.ExecContext(ctx, render(s.q.enqueue, a.ids))
		return err
	}
	moves := make([]move, 0, len(a.ids)+len(a.reserved))
	for _, id := range a.ids {
		moves = append(moves, move{id: id})
	}
	moves = append(moves, a.reserved...)
	slices.SortFunc(moves, func(x, y move) int { return cmp.Compare(x.id, y.id) })
	c := &chunks{head: "UPDATE (VALUES ", tail: s.q.reserveTail, max: s.budget}
	for _, m := range moves {
		c.row()
		c.b = appendSQL(c.b, "ROW(?, ?)", m.id, m.at)
	}
	for _, stmt := range c.done() {
		if _, err := q.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) account(ctx context.Context, q querier, slots map[string]*slot) error {
	var (
		dirty []string
		paced bool
	)
	for _, key := range slices.Sorted(maps.Keys(slots)) {
		if sl := slots[key]; sl.dirty || sl.paced {
			dirty = append(dirty, key)
			paced = paced || sl.paced
		}
	}
	if len(dirty) == 0 {
		return nil
	}
	b := make([]byte, 0, 128+80*len(dirty))
	b = append(b, "UPDATE "...)
	b = append(b, s.prefix...)
	b = append(b, "limits SET active = CASE limit_key"...)
	for _, key := range dirty {
		b = appendSQL(b, " WHEN ? THEN ?", key, max(slots[key].active, 0))
	}
	b = append(b, " END"...)
	if paced {
		b = append(b, ", tat = CASE limit_key"...)
		for _, key := range dirty {
			if sl := slots[key]; sl.paced {
				b = appendSQL(b, " WHEN ? THEN ?", key, sl.tat)
			}
		}
		b = append(b, " ELSE tat END"...)
	}
	b = appendSQL(b, " WHERE limit_key IN (?)", dirty)
	_, err := q.ExecContext(ctx, string(b))
	return err
}

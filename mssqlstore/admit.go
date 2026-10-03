package mssqlstore

import (
	"cmp"
	"context"
	"database/sql"
	"maps"
	"slices"
	"time"
)

const admitBatch = 1000

const sqlWaiting = `SELECT CAST(SYSUTCDATETIME() AS DATETIME2(6)), j.id, j.queue, w.k, j.priority, j.granted
FROM OPENJSON(@walk) WITH (k NVARCHAR(255) '$.k', n INT '$.n', p SMALLINT '$.p', i BIGINT '$.i') w
CROSS APPLY (
	SELECT TOP (w.n) x.id, x.queue, x.priority, x.granted
	FROM {p}jobs x WITH (XLOCK, READPAST, ROWLOCK, INDEX(jobs_throttled))
	WHERE x.state = 'throttled' AND x.limit_key = w.k COLLATE Latin1_General_100_BIN2
		AND (w.i IS NULL OR x.priority < w.p OR x.priority = w.p AND x.id > w.i)
	ORDER BY x.priority DESC, x.id
) j`

const sqlPlace = `UPDATE j SET state = CASE WHEN v.at IS NULL THEN 'enqueued' ELSE 'scheduled' END,
	run_at = COALESCE(v.at, j.run_at), granted = CASE WHEN v.at IS NULL THEN 0 ELSE 1 END
FROM OPENJSON(@moves) WITH (id BIGINT '$.id', at DATETIME2(6) '$.at') v
JOIN {p}jobs j WITH (FORCESEEK) ON j.id = v.id;
`

const sqlAccount = `UPDATE l SET active = v.active, tat = CASE WHEN v.paced = 1 THEN v.tat ELSE l.tat END,
	admit_tat = CASE WHEN v.paced = 1 THEN v.admit ELSE l.admit_tat END
FROM OPENJSON(@slots) WITH (k NVARCHAR(255) '$.k', active INT '$.a', paced BIT '$.p', tat DATETIME2(6) '$.t',
	admit DATETIME2(6) '$.at') v
JOIN {p}limits l WITH (FORCESEEK) ON l.limit_key = v.k COLLATE Latin1_General_100_BIN2;
`

func (r rule) interval() time.Duration {
	return time.Duration(r.per) * time.Microsecond / time.Duration(r.rate)
}

type slot struct {
	rule
	active   int
	tat      time.Time
	admitTat time.Time
	dirty    bool
	paced    bool
}

func (sl *slot) scan(rows *sql.Rows, key *string) error {
	var tat, admitTat moment
	if err := rows.Scan(key, &sl.max, &sl.active, &sl.rate, &sl.per, &sl.burst, &tat, &admitTat); err != nil {
		return err
	}
	sl.tat, sl.admitTat = tat.Time, admitTat.Time
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
	full := sl.max > 0 && sl.active >= sl.max
	if granted && full {
		return hold, now
	}
	paced := sl.rate > 0 && (!granted || !sl.open(now))
	at := now
	if paced {
		if t := sl.tat.Add(-sl.tau()); t.After(at) {
			at = t
		}
	}
	switch {
	case at.After(now):
		sl.take(at)
		return reserve, at
	case full:
		return hold, at
	}
	if paced {
		sl.take(at)
	}
	if sl.rate > 0 {
		sl.pass(now)
	}
	sl.active++
	sl.dirty = true
	return admit, at
}

func (sl *slot) tau() time.Duration {
	return time.Duration(max(sl.burst-1, 0)) * sl.interval()
}

func (sl *slot) open(now time.Time) bool {
	return !sl.admitTat.Add(-sl.tau() - sl.interval()/2).After(now)
}

func (sl *slot) take(at time.Time) {
	if at.After(sl.tat) {
		sl.tat = at
	}
	sl.tat = sl.tat.Add(sl.interval())
	sl.paced = true
}

func (sl *slot) pass(now time.Time) {
	if now.After(sl.admitTat) {
		sl.admitTat = now
	}
	sl.admitTat = sl.admitTat.Add(sl.interval())
	if sl.admitTat.After(sl.tat) {
		sl.tat = sl.admitTat
	}
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
				}
				a.queues = merge(a.queues, j.queue)
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
	if err := s.place(ctx, q, a, slots); err != nil {
		return a, err
	}
	return a, nil
}

func (s *Store) waiting(ctx context.Context, q querier, pending map[string]*walk) (map[string][]waiter, error) {
	var t table
	for _, key := range slices.Sorted(maps.Keys(pending)) {
		w := pending[key]
		t.row()
		t.str("k", key)
		t.int("n", int64(w.want))
		if w.seen > 0 {
			t.int("p", int64(w.last.priority))
			t.int("i", w.last.id)
		}
	}
	rows, err := q.QueryContext(ctx, s.q.waiting, sql.Named("walk", t.String()))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	got := make(map[string][]waiter, len(pending))
	for rows.Next() {
		var (
			now moment
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

func (s *Store) place(ctx context.Context, q querier, a admitted, slots map[string]*slot) error {
	var (
		stmt  string
		moves table
		acct  table
	)
	if a.changed() > 0 {
		all := make([]move, 0, a.changed())
		for _, id := range a.ids {
			all = append(all, move{id: id})
		}
		all = append(all, a.reserved...)
		slices.SortFunc(all, func(x, y move) int { return cmp.Compare(x.id, y.id) })
		for _, m := range all {
			moves.row()
			moves.int("id", m.id)
			moves.time("at", m.at)
		}
		stmt = s.q.place
	}
	for _, key := range slices.Sorted(maps.Keys(slots)) {
		sl := slots[key]
		if !sl.dirty && !sl.paced {
			continue
		}
		acct.row()
		acct.str("k", key)
		acct.int("a", int64(max(sl.active, 0)))
		if sl.paced {
			acct.int("p", 1)
			acct.time("t", sl.tat)
			acct.time("at", sl.admitTat)
		}
	}
	if acct.n > 0 {
		stmt += s.q.account
	}
	if stmt == "" {
		return nil
	}
	_, err := q.ExecContext(ctx, stmt, sql.Named("moves", moves.String()), sql.Named("slots", acct.String()))
	return err
}

package mysqlstore

import (
	"context"
	"database/sql"
	"maps"
	"slices"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

const sqlLockLimits = `SELECT limit_key, max, active, rate, per_us, burst, tat, admit_tat FROM {p}limits FORCE INDEX (PRIMARY)
WHERE limit_key IN (?) ORDER BY limit_key FOR UPDATE`

const sqlDeclared = `SELECT limit_key, max, rate, per_us, burst FROM {p}limits WHERE limit_key IN (?)`

const sqlRecent = sqlDeclared + ` AND declared_at > UTC_TIMESTAMP(6) - INTERVAL 1 MINUTE`

const sqlDeclare = `INSERT INTO {p}limits (limit_key, max, rate, per_us, burst) VALUES `

const sqlDeclareTail = ` AS n ON DUPLICATE KEY UPDATE max = n.max, rate = n.rate, per_us = n.per_us, burst = n.burst`

const sqlEnsureTail = ` AS n ON DUPLICATE KEY UPDATE limit_key = {p}limits.limit_key`

const sqlTouch = `INSERT INTO {p}limits (limit_key, max, rate, per_us, burst, declared_at)
SELECT v.k, v.max, v.rate, v.per_us, v.burst, UTC_TIMESTAMP(6) FROM (VALUES `

const sqlTouchTail = `) AS v (k, max, rate, per_us, burst) LEFT JOIN {p}limits l ON l.limit_key = v.k
FOR UPDATE OF l NOWAIT
ON DUPLICATE KEY UPDATE max = v.max, rate = v.rate, per_us = v.per_us, burst = v.burst, declared_at = UTC_TIMESTAMP(6)`

type rule struct {
	max   int
	rate  int
	per   int64
	burst int
}

func ruleOf(p *driver.InsertParams) rule {
	r := rule{max: p.LimitMax}
	if p.LimitRate > 0 {
		r.rate, r.per, r.burst = p.LimitRate, max(micros(p.LimitPer), 1), p.LimitBurst
	}
	return r
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
		var key string
		sl := &slot{}
		if err := sl.scan(rows, &key); err != nil {
			return nil, err
		}
		slots[key] = sl
	}
	return slots, rows.Err()
}

func (s *Store) declared(ctx context.Context, q querier, stmt string, keys []string) (map[string]rule, error) {
	rows, err := q.QueryContext(ctx, render(stmt, keys))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	rules := make(map[string]rule, len(keys))
	for rows.Next() {
		var (
			key string
			r   rule
		)
		if err := rows.Scan(&key, &r.max, &r.rate, &r.per, &r.burst); err != nil {
			return nil, err
		}
		rules[key] = r
	}
	return rules, rows.Err()
}

func (s *Store) declare(ctx context.Context, keys []string, rules map[string]rule, external bool) error {
	var (
		stored map[string]rule
		err    error
	)
	if external {
		err = s.side.run(ctx, func(conn *sql.Conn) (err error) {
			stored, err = s.declared(ctx, conn, s.q.recent, keys)
			return err
		})
	} else {
		stored, err = s.declared(ctx, s.db, s.q.declared, keys)
	}
	if err != nil {
		return err
	}
	keys = slices.DeleteFunc(slices.Clone(keys), func(k string) bool {
		r, ok := stored[k]
		return ok && r == rules[k]
	})
	switch {
	case len(keys) == 0:
		return nil
	case external:
		return s.touch(ctx, keys, rules)
	}
	_, err = s.db.ExecContext(ctx, s.limitRows(s.q.declareTail, keys, rules))
	return err
}

func (s *Store) touch(ctx context.Context, keys []string, rules map[string]rule) error {
	b := make([]byte, 0, 64*len(keys)+384)
	b = append(b, s.q.touch...)
	for i, key := range keys {
		if i > 0 {
			b = append(b, ',')
		}
		r := rules[key]
		b = appendSQL(b, "ROW(?, ?, ?, ?, ?)", key, r.max, r.rate, r.per, r.burst)
	}
	stmt := string(append(b, s.q.touchTail...))
	for try := 1; ; try++ {
		_, err := s.side.exec(ctx, stmt)
		if !retryable(err) {
			return err
		}
		t := time.NewTimer(time.Duration(min(try, 10)) * time.Millisecond)
		select {
		case <-ctx.Done():
			t.Stop()
			return err
		case <-t.C:
		}
	}
}

func (s *Store) limitRows(tail string, keys []string, rules map[string]rule) string {
	b := make([]byte, 0, 64*len(keys)+192)
	b = append(b, s.q.declare...)
	for i, key := range keys {
		if i > 0 {
			b = append(b, ',')
		}
		r := rules[key]
		b = appendSQL(b, "(?, ?, ?, ?, ?)", key, r.max, r.rate, r.per, r.burst)
	}
	return string(append(b, tail...))
}

func (s *Store) restore(ctx context.Context, q querier, keys []string, rules map[string]rule, slots map[string]*slot) error {
	var gone []string
	for _, key := range keys {
		if slots[key] == nil {
			gone = append(gone, key)
		}
	}
	if len(gone) == 0 {
		return nil
	}
	held, err := s.declared(ctx, q, s.q.declared, gone)
	if err != nil {
		return err
	}
	gone = slices.DeleteFunc(gone, func(k string) bool {
		_, ok := held[k]
		return ok
	})
	if len(gone) == 0 {
		return nil
	}
	if _, err := q.ExecContext(ctx, s.limitRows(s.q.ensureTail, gone, rules)); err != nil {
		return err
	}
	more, err := s.lockLimits(ctx, q, gone, false)
	if err != nil {
		return err
	}
	maps.Copy(slots, more)
	return nil
}

func (s *Store) admitKeys(ctx context.Context, rules map[string]rule) (admitted, error) {
	keys := slices.Sorted(maps.Keys(rules))
	var a admitted
	err := s.txn(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, s.limitRows(s.q.ensureTail, keys, rules)); err != nil {
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

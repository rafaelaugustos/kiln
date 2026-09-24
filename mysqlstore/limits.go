package mysqlstore

import (
	"context"
	"database/sql"
	"maps"
	"slices"

	"github.com/rafaelaugustos/kiln/driver"
)

const sqlLockLimits = `SELECT limit_key, max, active, rate, per_us, burst, tat FROM {p}limits FORCE INDEX (PRIMARY)
WHERE limit_key IN (?) ORDER BY limit_key FOR UPDATE`

const sqlDeclared = `SELECT limit_key, max, rate, per_us, burst FROM {p}limits WHERE limit_key IN (?)`

const sqlDeclare = `INSERT INTO {p}limits (limit_key, max, rate, per_us, burst, declared_at) VALUES `

const sqlDeclareTail = ` AS n ON DUPLICATE KEY UPDATE max = n.max, rate = n.rate, per_us = n.per_us, burst = n.burst,
	declared_at = COALESCE(n.declared_at, {p}limits.declared_at)`

const sqlEnsureTail = ` AS n ON DUPLICATE KEY UPDATE limit_key = {p}limits.limit_key`

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

func (s *Store) declared(ctx context.Context, q querier, keys []string) (map[string]rule, error) {
	rows, err := q.QueryContext(ctx, render(s.q.declared, keys))
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
	if external {
		_, err := s.side.exec(ctx, s.limitRows(s.q.declareTail, keys, rules, "UTC_TIMESTAMP(6)"))
		return err
	}
	stored, err := s.declared(ctx, s.db, keys)
	if err != nil {
		return err
	}
	keys = slices.DeleteFunc(slices.Clone(keys), func(k string) bool {
		r, ok := stored[k]
		return ok && r == rules[k]
	})
	if len(keys) == 0 {
		return nil
	}
	_, err = s.db.ExecContext(ctx, s.limitRows(s.q.declareTail, keys, rules, "NULL"))
	return err
}

func (s *Store) limitRows(tail string, keys []string, rules map[string]rule, at raw) string {
	b := make([]byte, 0, 64*len(keys)+192)
	b = append(b, s.q.declare...)
	for i, key := range keys {
		if i > 0 {
			b = append(b, ',')
		}
		r := rules[key]
		b = appendSQL(b, "(?, ?, ?, ?, ?, ?)", key, r.max, r.rate, r.per, r.burst, at)
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
	held, err := s.declared(ctx, q, gone)
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
	if _, err := q.ExecContext(ctx, s.limitRows(s.q.ensureTail, gone, rules, "NULL")); err != nil {
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
		if _, err := tx.ExecContext(ctx, s.limitRows(s.q.ensureTail, keys, rules, "NULL")); err != nil {
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

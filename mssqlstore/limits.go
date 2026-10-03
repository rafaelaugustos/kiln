package mssqlstore

import (
	"context"
	"database/sql"
	"maps"
	"slices"

	"github.com/rafaelaugustos/kiln/driver"
)

const limitColumns = `l.limit_key, l.[max], l.active, l.rate, l.per_us, l.burst, l.tat, l.admit_tat`

const keyRows = `OPENJSON(@keys) WITH (k NVARCHAR(255) '$') v`

const sqlLockLimits = `SELECT ` + limitColumns + ` FROM ` + keyRows + `
JOIN {p}limits l WITH (UPDLOCK, ROWLOCK, FORCESEEK) ON l.limit_key = v.k COLLATE Latin1_General_100_BIN2
ORDER BY l.limit_key`

const sqlSkipLimits = `SELECT ` + limitColumns + ` FROM ` + keyRows + `
JOIN {p}limits l WITH (UPDLOCK, READPAST, ROWLOCK, FORCESEEK) ON l.limit_key = v.k COLLATE Latin1_General_100_BIN2
ORDER BY l.limit_key`

const sqlDeclared = `SELECT l.limit_key, l.[max], l.rate, l.per_us, l.burst FROM ` + keyRows + `
JOIN {p}limits l ON l.limit_key = v.k COLLATE Latin1_General_100_BIN2`

const sqlRecent = sqlDeclared + `
WHERE l.declared_at > DATEADD(MINUTE, -1, SYSUTCDATETIME())`

const ruleRows = `SELECT k COLLATE Latin1_General_100_BIN2 AS k, [max], rate, per_us, burst
FROM OPENJSON(@rules) WITH (k NVARCHAR(255) '$.k', [max] INT '$.max', rate INT '$.rate', per_us BIGINT '$.per',
	burst INT '$.burst')`

const sqlDeclare = `MERGE {p}limits WITH (HOLDLOCK) AS l
USING (` + ruleRows + `) AS r ON l.limit_key = r.k
WHEN MATCHED THEN UPDATE SET [max] = r.[max], rate = r.rate, per_us = r.per_us, burst = r.burst
WHEN NOT MATCHED THEN INSERT (limit_key, [max], rate, per_us, burst) VALUES (r.k, r.[max], r.rate, r.per_us, r.burst);`

const sqlTouch = `MERGE {p}limits WITH (HOLDLOCK) AS l
USING (` + ruleRows + `) AS r ON l.limit_key = r.k
WHEN MATCHED THEN UPDATE SET [max] = r.[max], rate = r.rate, per_us = r.per_us, burst = r.burst,
	declared_at = SYSUTCDATETIME()
WHEN NOT MATCHED THEN INSERT (limit_key, [max], rate, per_us, burst, declared_at)
	VALUES (r.k, r.[max], r.rate, r.per_us, r.burst, SYSUTCDATETIME());`

const sqlEnsure = `INSERT INTO {p}limits (limit_key, [max], rate, per_us, burst)
SELECT r.k, r.[max], r.rate, r.per_us, r.burst FROM (` + ruleRows + `) r
WHERE NOT EXISTS (SELECT 1 FROM {p}limits l WHERE l.limit_key = r.k)`

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

func ruleList(keys []string, rules map[string]rule) string {
	var t table
	for _, key := range keys {
		r := rules[key]
		t.row()
		t.str("k", key)
		t.int("max", int64(r.max))
		t.int("rate", int64(r.rate))
		t.int("per", r.per)
		t.int("burst", int64(r.burst))
	}
	return t.String()
}

func (s *Store) lockLimits(ctx context.Context, q querier, keys []string, skip bool) (map[string]*slot, error) {
	stmt := s.q.lockLimits
	if skip {
		stmt = s.q.skipLimits
	}
	rows, err := q.QueryContext(ctx, stmt, sql.Named("keys", stringList(keys)))
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
	rows, err := q.QueryContext(ctx, stmt, sql.Named("keys", stringList(keys)))
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
	read, write := s.q.declared, s.q.declare
	if external {
		read, write = s.q.recent, s.q.touch
	}
	stored, err := s.declared(ctx, s.db, read, keys)
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
	return s.retry(ctx, func() error {
		_, err := s.db.ExecContext(ctx, write, sql.Named("rules", ruleList(keys, rules)))
		return err
	})
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
	if _, err := q.ExecContext(ctx, s.q.ensure, sql.Named("rules", ruleList(gone, rules))); err != nil {
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
		if _, err := tx.ExecContext(ctx, s.q.ensure, sql.Named("rules", ruleList(keys, rules))); err != nil {
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

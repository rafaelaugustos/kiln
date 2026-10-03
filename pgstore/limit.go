package pgstore

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/rafaelaugustos/kiln/driver"
)

const sqlLimits = `WITH l AS MATERIALIZED (
	SELECT key FROM {s}.limits WHERE key = ANY($1) ORDER BY key FOR KEY SHARE
)
INSERT INTO {s}.limits (key, max, rate, per_us, burst)
SELECT t.key, t.max, t.rate, t.per_us, t.burst
FROM unnest($1::text[], $2::bigint[], $3::bigint[], $4::bigint[], $5::bigint[]) AS t(key, max, rate, per_us, burst)
WHERE t.key NOT IN (SELECT key FROM l)
ORDER BY t.key
ON CONFLICT (key) DO NOTHING`

const admitTail = `, g AS (
	SELECT k.key, k.max, k.rate, k.per_us, k.burst,
		CASE WHEN k.max > 0 THEN greatest(k.max::bigint - k.active, 0) END AS free,
		k.per_us::float8 / nullif(k.rate, 0) AS gap, (k.burst - 1) * k.per_us::float8 / nullif(k.rate, 0) AS tau,
		greatest(extract(epoch FROM k.tat - statement_timestamp()) * 1000000, 0)::float8 AS tat,
		greatest(extract(epoch FROM k.admit_tat - statement_timestamp()) * 1000000, 0)::float8 AS admit
	FROM k
	WHERE EXISTS (SELECT 1 FROM {s}.limits l WHERE l.ctid = k.ctid)
), h AS (
	SELECT g.key, g.max, g.rate, g.per_us, g.burst, g.free, g.gap, g.tau, g.tat, g.admit, CASE
		WHEN g.rate = 0 THEN g.free
		WHEN g.free < 1000 AND (p.open IS NULL OR g.admit + p.open * g.gap <= g.tau + g.gap / 2)
			AND (p.soon IS NULL OR greatest(g.tat + p.taken * g.gap, g.admit + p.soon * g.gap) <= g.tau) THEN g.free
		ELSE 1000
	END AS take
	FROM g CROSS JOIN LATERAL (
		SELECT max(t.i) FILTER (WHERE t.granted AND t.i < g.free) AS open, max(t.i) FILTER (WHERE NOT t.granted) AS soon,
			count(*) FILTER (WHERE NOT t.granted) - 1 AS taken
		FROM (
			SELECT j.granted, row_number() OVER (ORDER BY j.priority DESC, j.id) - 1 AS i FROM {s}.jobs j
			WHERE j.state = 'throttled' AND j.limit_key = g.key
			ORDER BY j.priority DESC, j.id
			LIMIT CASE WHEN g.rate > 0 AND g.free < 1000 THEN g.free + 1 ELSE 0 END
		) t
	) p
), c AS MATERIALIZED (
	SELECT h.key, h.rate, v.id, v.granted, (row_number() OVER (ORDER BY h.key, v.priority DESC, v.id))::int AS i
	FROM h CROSS JOIN LATERAL (
		SELECT j.id, j.priority, j.granted FROM {s}.jobs j
		WHERE j.state = 'throttled' AND j.limit_key = h.key
		ORDER BY j.priority DESC, j.id
		LIMIT h.take
		FOR NO KEY UPDATE SKIP LOCKED
	) v
), a AS MATERIALIZED (
	SELECT array_agg(c.id ORDER BY c.i) AS id, array_agg(c.granted ORDER BY c.i) AS granted FROM c
), w(key, i, last, free, gap, tau, tat, admit, id, at) AS (
	SELECT h.key, min(c.i) - 1, max(c.i), h.free, h.gap, h.tau, h.tat, h.admit, NULL::bigint, NULL::float8
	FROM h JOIN c ON c.key = h.key
	WHERE h.rate > 0
	GROUP BY h.key, h.free, h.gap, h.tau, h.tat, h.admit
	UNION ALL
	SELECT w.key, w.i + 1, w.last, w.free - d.soon::int, w.gap, w.tau,
		CASE WHEN d.open THEN greatest(w.tat, w.admit + w.gap) WHEN d.soon THEN greatest(w.tat, w.admit) + w.gap ELSE w.tat + w.gap END,
		CASE WHEN d.soon THEN w.admit + w.gap ELSE w.admit END,
		a.id[w.i + 1], CASE WHEN NOT d.soon THEN w.tat - w.tau END
	FROM w CROSS JOIN a CROSS JOIN LATERAL (
		SELECT o.open, o.open OR w.tat <= w.tau AS soon
		FROM (SELECT a.granted[w.i + 1] AND w.admit <= w.tau + w.gap / 2 AS open) o
	) d
	WHERE w.i < w.last AND (w.free IS DISTINCT FROM 0 OR NOT a.granted[w.i + 1] AND w.tat > w.tau)
), x AS (
	SELECT c.id, NULL::float8 AS at FROM c WHERE c.rate = 0
	UNION ALL
	SELECT w.id, w.at FROM w WHERE w.id IS NOT NULL
), e AS (
	UPDATE {s}.jobs j SET state = CASE WHEN x.at IS NULL THEN 'enqueued' ELSE 'scheduled' END::{s}.state,
		run_at = CASE WHEN x.at IS NULL THEN j.run_at ELSE statement_timestamp() + x.at * interval '1 microsecond' END,
		granted = x.at IS NOT NULL
	FROM x
	WHERE j.id = ANY(ARRAY(SELECT id FROM x)) AND j.id = x.id
	RETURNING j.queue, j.limit_key, x.at IS NULL AS soon
), n AS (
	UPDATE {s}.limits l SET max = h.max, rate = h.rate, per_us = h.per_us, burst = h.burst,
		active = l.active + coalesce(y.admitted, 0),
		tat = CASE WHEN y.limit_key IS NOT NULL AND h.rate > 0 THEN statement_timestamp() + z.tat * interval '1 microsecond' ELSE l.tat END,
		admit_tat = CASE WHEN y.admitted > 0 AND h.rate > 0 THEN statement_timestamp() + z.admit * interval '1 microsecond' ELSE l.admit_tat END
	FROM h LEFT JOIN (
		SELECT limit_key, count(*) FILTER (WHERE soon) AS admitted FROM e GROUP BY limit_key
	) y ON y.limit_key = h.key LEFT JOIN (
		SELECT DISTINCT ON (w.key) w.key, w.tat, w.admit FROM w ORDER BY w.key, w.i DESC
	) z ON z.key = h.key
	WHERE l.key = h.key AND (y.limit_key IS NOT NULL OR (l.max, l.rate, l.per_us, l.burst) <> (h.max, h.rate, h.per_us, h.burst))
)`

const sqlAdmit = `WITH RECURSIVE k AS MATERIALIZED (
	SELECT l.ctid, l.key, coalesce(r.max, l.max) AS max, coalesce(r.rate, l.rate) AS rate,
		coalesce(r.per_us, l.per_us) AS per_us, coalesce(r.burst, l.burst) AS burst, l.active, l.tat, l.admit_tat
	FROM {s}.limits l
	LEFT JOIN unnest($1::text[], $2::bigint[], $3::bigint[], $4::bigint[], $5::bigint[])
		AS r(key, max, rate, per_us, burst) ON r.key = l.key
	WHERE l.key = ANY($1)
	ORDER BY l.key
	FOR NO KEY UPDATE OF l SKIP LOCKED
)` + admitTail + `
SELECT (SELECT count(*) FROM e), (SELECT coalesce(array_agg(DISTINCT queue), '{}') FROM e),
	array_agg(l.key), array_agg(l.max), array_agg(l.rate), array_agg(l.per_us), array_agg(l.burst)
FROM {s}.limits l
WHERE l.key = ANY($1) AND l.key NOT IN (SELECT key FROM g)`

const sqlAdmitFinished = `WITH RECURSIVE k AS MATERIALIZED (
	SELECT l.ctid, l.key, l.max, l.rate, l.per_us, l.burst, l.active, l.tat, l.admit_tat FROM {s}.limits l
	WHERE l.key = ANY(ARRAY(
		SELECT j.limit_key FROM {s}.jobs j WHERE j.id = ANY($1) AND j.limit_key IS NOT NULL
		UNION SELECT a.limit_key FROM {s}.archive a WHERE a.id = ANY($1) AND a.limit_key IS NOT NULL
		UNION SELECT j.limit_key FROM {s}.jobs j WHERE j.id = ANY(ARRAY(` + children + `)) AND j.state = 'throttled'))
	ORDER BY l.key
	FOR NO KEY UPDATE SKIP LOCKED
)` + admitTail + `
SELECT queue, count(*) FROM e GROUP BY queue`

const changedRules = `unnest($1::text[], $2::bigint[], $3::bigint[], $4::bigint[], $5::bigint[]) AS r(key, max, rate, per_us, burst)
JOIN unnest($6::text[], $7::bigint[], $8::bigint[], $9::bigint[], $10::bigint[]) AS o(key, max, rate, per_us, burst) ON o.key = r.key
WHERE l.key = r.key AND (l.max, l.rate, l.per_us, l.burst) = (o.max, o.rate, o.per_us, o.burst)
	AND (l.max, l.rate, l.per_us, l.burst) <> (r.max, r.rate, r.per_us, r.burst)`

const sqlLockRules = `SELECT l.key FROM {s}.limits l, ` + changedRules + `
ORDER BY l.key
FOR NO KEY UPDATE OF l`

const sqlSetRules = `UPDATE {s}.limits l SET max = r.max, rate = r.rate, per_us = r.per_us, burst = r.burst
FROM ` + changedRules

var backoff = [...]time.Duration{2 * time.Millisecond, 5 * time.Millisecond, 10 * time.Millisecond}

type rules struct {
	keys  []string
	max   []int64
	rate  []int64
	per   []int64
	burst []int64
}

func (r *rules) add(p *driver.InsertParams) {
	r.put(p.LimitKey, int64(p.LimitMax), int64(p.LimitRate), micros(p.LimitPer), int64(p.LimitBurst))
}

func (r *rules) put(key string, max, rate, per, burst int64) {
	if i := slices.Index(r.keys, key); i >= 0 {
		r.max[i], r.rate[i], r.per[i], r.burst[i] = max, rate, per, burst
		return
	}
	r.keys = append(r.keys, key)
	r.max = append(r.max, max)
	r.rate = append(r.rate, rate)
	r.per = append(r.per, per)
	r.burst = append(r.burst, burst)
}

func (r *rules) merge(o rules) {
	for i, key := range o.keys {
		r.put(key, o.max[i], o.rate[i], o.per[i], o.burst[i])
	}
}

func (r rules) only(keys []string) rules {
	var o rules
	for i, key := range r.keys {
		switch {
		case !slices.Contains(keys, key):
		case r.max == nil:
			o.keys = append(o.keys, key)
		default:
			o.put(key, r.max[i], r.rate[i], r.per[i], r.burst[i])
		}
	}
	return o
}

func (r rules) args() []any {
	return []any{r.keys, r.max, r.rate, r.per, r.burst}
}

func (w *wake) scanAdmitted(rows pgx.Rows) error {
	var (
		q string
		n int
	)
	for rows.Next() {
		if err := rows.Scan(&q, &n); err != nil {
			return err
		}
		w.queue(q)
		w.moved += n
	}
	return rows.Err()
}

func (w *wake) admitted(r rules) func(pgx.Row) error {
	return func(row pgx.Row) error {
		var (
			n      int
			queues []string
			seen   rules
		)
		if err := row.Scan(&n, &queues, &seen.keys, &seen.max, &seen.rate, &seen.per, &seen.burst); err != nil {
			return err
		}
		w.moved += n
		for _, q := range queues {
			w.queue(q)
		}
		w.rules = r.only(seen.keys)
		w.seen = seen
		return nil
	}
}

func (s *Store) admit(ctx context.Context, r rules, w *wake) error {
	err := w.admitted(r)(s.pool.QueryRow(ctx, s.q.admit, r.args()...))
	if err == nil {
		err = s.readmit(ctx, w)
	}
	if err != nil {
		return fmt.Errorf("kiln: admit: %w", err)
	}
	return nil
}

func (s *Store) admitLate(ctx context.Context, w *wake) error {
	held := slices.DeleteFunc(w.held, func(key string) bool { return slices.Contains(w.keys, key) })
	if len(w.keys) > 0 {
		if err := s.admit(ctx, w.rules, w); err != nil {
			return err
		}
	}
	if len(held) == 0 {
		return nil
	}
	return s.admit(ctx, rules{keys: held}, w)
}

func (s *Store) readmit(ctx context.Context, w *wake) error {
	seen := w.seen
	for _, d := range backoff {
		if len(w.keys) == 0 {
			return nil
		}
		t := time.NewTimer(d)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}
		if err := w.admitted(w.rules)(s.pool.QueryRow(ctx, s.q.admit, w.rules.args()...)); err != nil {
			return err
		}
	}
	if len(w.keys) == 0 || w.max == nil {
		return nil
	}
	args := append(w.rules.args(), seen.args()...)
	b := &pgx.Batch{}
	b.Queue(s.q.lockRules, args...)
	b.Queue(s.q.setRules, args...)
	return s.pool.SendBatch(ctx, b).Close()
}

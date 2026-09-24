package pgstore

import (
	"context"
	"fmt"
	"slices"

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
	SELECT k.key, k.max, k.rate, k.per_us, k.burst, greatest(k.tat, statement_timestamp()) AS base,
		CASE WHEN k.max > 0 THEN greatest(k.max::bigint - k.active, 0) END AS free
	FROM k
), h AS (
	SELECT g.key, g.max, g.rate, g.per_us, g.burst, g.base, g.free, CASE
		WHEN g.rate = 0 THEN g.free
		WHEN g.free < 1000 AND (p.n = 0 OR g.base + (p.n - g.burst)::float8 * g.per_us / g.rate * interval '1 microsecond' <= statement_timestamp()) THEN g.free
		ELSE 1000
	END AS take
	FROM g CROSS JOIN LATERAL (
		SELECT count(*) FILTER (WHERE NOT t.granted) AS n FROM (
			SELECT j.granted FROM {s}.jobs j
			WHERE j.state = 'throttled' AND j.limit_key = g.key
			ORDER BY j.priority DESC, j.id
			LIMIT CASE WHEN g.rate > 0 AND g.free < 1000 THEN g.free + 1 ELSE 0 END
		) t
	) p
), c AS MATERIALIZED (
	SELECT h.key, h.rate, h.per_us, h.burst, h.base, h.free, w.id, w.priority, h.rate > 0 AND NOT w.granted AS rated
	FROM h CROSS JOIN LATERAL (
		SELECT j.id, j.priority, j.granted FROM {s}.jobs j
		WHERE j.state = 'throttled' AND j.limit_key = h.key
		ORDER BY j.priority DESC, j.id
		LIMIT h.take
		FOR NO KEY UPDATE SKIP LOCKED
	) w
), r AS (
	SELECT c.key, c.free, c.id, c.priority, c.rated, CASE WHEN c.rated THEN
		c.base + (count(*) FILTER (WHERE c.rated) OVER w - c.burst)::float8 * c.per_us / c.rate * interval '1 microsecond'
	END AS at
	FROM c
	WINDOW w AS (PARTITION BY c.key ORDER BY c.priority DESC, c.id)
), x AS MATERIALIZED (
	SELECT r.id, r.rated, r.at, r.at IS NULL OR r.at <= statement_timestamp() AS soon,
		r.free IS NULL OR count(*) FILTER (WHERE r.at IS NULL OR r.at <= statement_timestamp())
			OVER (PARTITION BY r.key ORDER BY r.priority DESC, r.id) <= r.free AS go
	FROM r
), e AS (
	UPDATE {s}.jobs j SET state = CASE WHEN x.soon THEN 'enqueued' ELSE 'scheduled' END::{s}.state,
		run_at = CASE WHEN x.soon THEN j.run_at ELSE x.at END, granted = NOT x.soon
	FROM x
	WHERE j.id = ANY(ARRAY(SELECT id FROM x WHERE go)) AND j.id = x.id
	RETURNING j.queue, j.limit_key, x.soon, x.rated
), n AS (
	UPDATE {s}.limits l SET max = h.max, rate = h.rate, per_us = h.per_us, burst = h.burst,
		active = l.active + coalesce(y.admitted, 0),
		tat = CASE WHEN y.taken > 0 THEN h.base + y.taken::float8 * h.per_us / h.rate * interval '1 microsecond' ELSE l.tat END
	FROM h LEFT JOIN (
		SELECT limit_key, count(*) FILTER (WHERE soon) AS admitted, count(*) FILTER (WHERE rated) AS taken
		FROM e GROUP BY limit_key
	) y ON y.limit_key = h.key
	WHERE l.key = h.key AND (y.limit_key IS NOT NULL OR (l.max, l.rate, l.per_us, l.burst) <> (h.max, h.rate, h.per_us, h.burst))
)
SELECT queue, count(*) FROM e GROUP BY queue`

const sqlAdmit = `WITH k AS MATERIALIZED (
	SELECT l.key, coalesce(r.max, l.max) AS max, coalesce(r.rate, l.rate) AS rate,
		coalesce(r.per_us, l.per_us) AS per_us, coalesce(r.burst, l.burst) AS burst, l.active, l.tat
	FROM {s}.limits l
	LEFT JOIN unnest($1::text[], $2::bigint[], $3::bigint[], $4::bigint[], $5::bigint[])
		AS r(key, max, rate, per_us, burst) ON r.key = l.key
	WHERE l.key = ANY($1)
	ORDER BY l.key
	FOR NO KEY UPDATE OF l
)` + admitTail

const sqlAdmitSkip = `WITH k AS MATERIALIZED (
	SELECT key, max, rate, per_us, burst, active, tat FROM {s}.limits
	WHERE key = ANY($1)
	ORDER BY key
	FOR NO KEY UPDATE SKIP LOCKED
)` + admitTail

const sqlAdmitFinished = `WITH k AS MATERIALIZED (
	SELECT l.key, l.max, l.rate, l.per_us, l.burst, l.active, l.tat FROM {s}.limits l
	WHERE l.key = ANY(ARRAY(
		SELECT j.limit_key FROM {s}.jobs j WHERE j.id = ANY($1) AND j.limit_key IS NOT NULL
		UNION SELECT a.limit_key FROM {s}.archive a WHERE a.id = ANY($1) AND a.limit_key IS NOT NULL
		UNION SELECT j.limit_key FROM {s}.jobs j WHERE j.id = ANY(ARRAY(` + children + `)) AND j.state = 'throttled'))
	ORDER BY l.key
	FOR NO KEY UPDATE SKIP LOCKED
)` + admitTail

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

func (s *Store) admit(ctx context.Context, r rules, w *wake) error {
	b := &pgx.Batch{}
	b.Queue(s.q.admit, r.args()...).Query(w.scanAdmitted)
	if err := s.pool.SendBatch(ctx, b).Close(); err != nil {
		return fmt.Errorf("kiln: admit: %w", err)
	}
	return nil
}

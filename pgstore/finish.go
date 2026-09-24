package pgstore

import (
	"bytes"
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/rafaelaugustos/kiln/driver"
)

const archiveColumns = `id, state, queue, kind, priority, attempt, max_attempts, claim, timeout_ms, run_at, created_at,
		attempted_at, finalized_at, server, batch_id, after_batch, parents, recurring_id, unique_key, limit_key,
		args, meta, tags, history, output`

const movedColumns = `j.id, j.queue, j.kind, j.priority, j.attempt, j.max_attempts, j.claim, j.timeout_ms, j.run_at,
		j.created_at, j.attempted_at, j.server, j.batch_id, j.after_batch, j.parents, j.recurring_id, j.unique_key,
		j.limit_key, j.args, j.meta, j.tags, j.history`

const sqlFinish = `WITH t AS MATERIALIZED (
	SELECT j.id, j.claim, j.queue, greatest(j.attempt - o.refund::int, 0) AS attempt, j.unique_key, j.limit_key,
		o.want, o.delay, o.refund, o.err, o.trace, o.output,
		j.cancel_requested AND o.want NOT IN ('succeeded', 'deleted') AS canceled,
		CASE
			WHEN j.cancel_requested AND o.want <> 'succeeded' THEN 'deleted'
			WHEN o.want = 'scheduled' AND o.delay > 0 THEN 'scheduled'
			WHEN o.want IN ('scheduled', 'enqueued') AND j.limit_key IS NOT NULL THEN 'throttled'
			WHEN o.want = 'scheduled' THEN 'enqueued'
			ELSE o.want
		END::{s}.state AS fin,
		CASE WHEN j.cancel_requested AND o.want NOT IN ('succeeded', 'deleted') THEN 'canceled' ELSE o.reason END AS reason
	FROM (
		SELECT DISTINCT ON (id, claim) id, claim, want, delay, refund, reason, err, trace, output
		FROM unnest($1::bigint[], $2::int[], $3::text[], $4::bigint[], $5::bool[], $6::text[], $7::text[],
			$8::text[], $9::text[]) WITH ORDINALITY AS o(id, claim, want, delay, refund, reason, err, trace, output, ord)
		ORDER BY id, claim, ord
	) o
	JOIN {s}.jobs j ON j.id = o.id AND j.claim = o.claim AND j.state = 'processing'
	ORDER BY j.id
	FOR NO KEY UPDATE OF j SKIP LOCKED
), a AS (
	DELETE FROM {s}.jobs j USING t
	WHERE j.id = t.id AND t.fin IN ('succeeded', 'deleted')
	RETURNING ` + movedColumns + `, t.fin, t.attempt AS next, t.reason, t.err, t.trace, t.output
), ar AS (
	INSERT INTO {s}.archive (` + archiveColumns + `)
	SELECT a.id, a.fin, a.queue, a.kind, a.priority, a.next, a.max_attempts, a.claim, a.timeout_ms, a.run_at,
		a.created_at, a.attempted_at, now(), a.server, a.batch_id, a.after_batch, a.parents, a.recurring_id,
		a.unique_key, a.limit_key, a.args, a.meta, a.tags,
		{s}.push(a.history, {s}.entry(a.fin, a.next, a.reason, a.err, a.trace, a.server)),
		nullif(a.output, '')::json
	FROM a
), u AS (
	UPDATE {s}.jobs j SET state = t.fin, attempt = t.attempt,
		run_at = CASE WHEN t.fin = 'failed' THEN j.run_at ELSE now() + greatest(t.delay, 0) * interval '1 microsecond' END,
		finalized_at = CASE WHEN t.fin = 'failed' THEN now() END, granted = false,
		history = {s}.push(j.history, {s}.entry(t.fin, t.attempt, t.reason, t.err, t.trace, j.server))
	FROM t
	WHERE j.id = t.id AND t.fin NOT IN ('succeeded', 'deleted')
), r AS (
	DELETE FROM {s}.uniques q WHERE q.key = ANY(ARRAY(
		SELECT h.key FROM {s}.uniques h JOIN t ON h.key = t.unique_key AND h.job_id = t.id
		WHERE t.fin IN ('succeeded', 'failed', 'deleted')
		ORDER BY h.key
		FOR UPDATE OF h))
	RETURNING q.key
), k AS MATERIALIZED (
	SELECT l.key FROM {s}.limits l
	WHERE l.key IN (SELECT limit_key FROM t) AND (SELECT count(*) FROM r) >= 0
	ORDER BY l.key
	FOR NO KEY UPDATE
), m AS (
	UPDATE {s}.limits l SET active = greatest(l.active - n.n, 0)
	FROM (SELECT limit_key, count(*) AS n FROM t WHERE limit_key IS NOT NULL GROUP BY limit_key) n
	WHERE l.key = n.limit_key AND l.key IN (SELECT key FROM k)
	RETURNING l.key
), st AS (
	INSERT INTO {s}.stats AS x (bucket, server, succeeded, failed, deleted, retried)
	SELECT date_trunc('minute', now()), $10, count(*) FILTER (WHERE fin = 'succeeded'),
		count(*) FILTER (WHERE fin = 'failed'), count(*) FILTER (WHERE fin = 'deleted'),
		count(*) FILTER (WHERE want = 'scheduled' AND NOT refund AND NOT canceled)
	FROM t
	WHERE (SELECT count(*) FROM m) >= 0
	HAVING count(*) > 0
	ON CONFLICT (bucket, server) DO UPDATE SET succeeded = x.succeeded + excluded.succeeded,
		failed = x.failed + excluded.failed, deleted = x.deleted + excluded.deleted,
		retried = x.retried + excluded.retried
)
SELECT t.id, t.claim, t.fin::text, t.queue FROM t ORDER BY t.id`

const resolveDeps = `WITH p AS MATERIALIZED (
	SELECT x.id, coalesce(
		(SELECT a.state FROM {s}.archive a WHERE a.id = x.id),
		(SELECT j.state FROM {s}.jobs j WHERE j.id = x.id)) AS state
	FROM (SELECT DISTINCT unnest($1::bigint[]) AS id) x
	WHERE EXISTS (SELECT 1 FROM {s}.deps d WHERE NOT d.batch AND NOT d.resolved AND d.parent_id = x.id)
), r AS MATERIALIZED (
	SELECT id, state = 'failed' AS failed FROM p WHERE coalesce(state, 'deleted') IN ('succeeded', 'failed', 'deleted')
), d AS MATERIALIZED (
	SELECT d.parent_id, d.job_id, d.mask FROM {s}.deps d
	WHERE NOT d.batch AND NOT d.resolved AND d.parent_id = ANY(ARRAY(SELECT id FROM r))
		AND (d.mask & 2 <> 0 OR d.parent_id <> ALL(ARRAY(SELECT id FROM r WHERE failed)))
	ORDER BY d.parent_id, d.job_id
	LIMIT 5000
	FOR NO KEY UPDATE OF d
), x AS (
	UPDATE {s}.deps y SET resolved = true FROM d
	WHERE NOT y.batch AND y.parent_id = d.parent_id AND y.job_id = d.job_id
), o AS (
	SELECT d.parent_id, d.job_id, d.mask & {s}.bit(coalesce(p.state, 'deleted')) <> 0 AS ok,
		coalesce(p.state::text, 'pruned') AS how
	FROM d JOIN p ON p.id = d.parent_id
), c AS MATERIALIZED (
	SELECT o.job_id, count(*) FILTER (WHERE o.ok) AS n,
		(array_agg('parent ' || o.parent_id || ' ' || o.how ORDER BY o.parent_id) FILTER (WHERE NOT o.ok))[1] AS doom
	FROM o GROUP BY o.job_id
), k AS MATERIALIZED (
	SELECT j.id, c.n, c.doom FROM {s}.jobs j JOIN c ON c.job_id = j.id
	WHERE j.state = 'awaiting'
	ORDER BY j.id
	FOR NO KEY UPDATE OF j
), a AS (
	DELETE FROM {s}.jobs j USING k
	WHERE j.id = k.id AND k.doom IS NOT NULL
	RETURNING ` + movedColumns + `, k.doom
), ar AS (
	INSERT INTO {s}.archive (` + archiveColumns + `)
	SELECT a.id, 'deleted', a.queue, a.kind, a.priority, a.attempt, a.max_attempts, a.claim, a.timeout_ms, a.run_at,
		a.created_at, a.attempted_at, now(), a.server, a.batch_id, a.after_batch, a.parents, a.recurring_id,
		a.unique_key, a.limit_key, a.args, a.meta, a.tags,
		{s}.push(a.history, {s}.entry('deleted', a.attempt, a.doom, '', '', NULL)), NULL
	FROM a
), u AS (
	UPDATE {s}.jobs j SET deps_pending = greatest(j.deps_pending - k.n, 0),
		state = CASE WHEN j.deps_pending - k.n > 0 THEN 'awaiting' ELSE {s}.ready(j.run_at, j.limit_key) END
	FROM k
	WHERE j.id = k.id AND k.doom IS NULL
	RETURNING j.queue, j.state
)`

const sqlResolve = resolveDeps + `
SELECT u.queue, u.state::text, 0::bigint FROM u
UNION ALL SELECT a.queue, 'deleted', coalesce(a.batch_id, 0) FROM a`

const sqlFinishDeps = resolveDeps + `, st AS (
	INSERT INTO {s}.stats AS x (bucket, server, deleted)
	SELECT date_trunc('minute', now()), $2, count(*) FROM a HAVING count(*) > 0
	ON CONFLICT (bucket, server) DO UPDATE SET deleted = x.deleted + excluded.deleted
)
SELECT DISTINCT u.queue FROM u WHERE u.state = 'enqueued'`

const children = `SELECT d.job_id FROM {s}.deps d WHERE NOT d.batch AND d.parent_id = ANY($1)
	ORDER BY d.parent_id, d.job_id LIMIT 5000`

const finishedBatches = `SELECT a.batch_id FROM {s}.archive a WHERE a.id = ANY($1)
	UNION SELECT a.batch_id FROM {s}.archive a WHERE a.id = ANY(ARRAY(` + children + `))`

const sqlLockFinishedBatches = `SELECT b.id FROM {s}.batches b
WHERE b.id = ANY(ARRAY(` + finishedBatches + `)) AND b.finished_at IS NULL
ORDER BY b.id
FOR NO KEY UPDATE SKIP LOCKED`

const batchDeps = `, r AS MATERIALIZED (
	SELECT y.parent_id, y.job_id FROM {s}.deps y
	WHERE y.batch AND NOT y.resolved AND y.parent_id = ANY(ARRAY(SELECT id FROM b))
	ORDER BY y.parent_id, y.job_id
	LIMIT 5000
	FOR NO KEY UPDATE
), d AS (
	UPDATE {s}.deps y SET resolved = true FROM r
	WHERE y.batch AND y.parent_id = r.parent_id AND y.job_id = r.job_id
	RETURNING y.job_id
), c AS MATERIALIZED (
	SELECT job_id, count(*) AS n FROM d GROUP BY job_id
), k AS MATERIALIZED (
	SELECT j.id, c.n FROM {s}.jobs j JOIN c ON c.job_id = j.id
	WHERE j.state = 'awaiting'
	ORDER BY j.id
	FOR NO KEY UPDATE OF j
), u AS (
	UPDATE {s}.jobs j SET deps_pending = greatest(j.deps_pending - k.n, 0),
		state = CASE WHEN j.deps_pending - k.n > 0 THEN 'awaiting' ELSE {s}.ready(j.run_at, j.limit_key) END
	FROM k WHERE j.id = k.id
	RETURNING j.queue, j.state
)
SELECT u.queue, u.state::text FROM u`

const completeWhere = `) AND b.sealed AND b.finished_at IS NULL
		AND NOT EXISTS (SELECT 1 FROM {s}.jobs j WHERE j.batch_id = b.id)
	RETURNING b.id
)`

const sqlCompleteFinished = `WITH b AS (
	UPDATE {s}.batches b SET finished_at = now()
	WHERE b.id = ANY(ARRAY(` + sqlLockFinishedBatches + `)` + completeWhere + batchDeps

const sqlComplete = `WITH b AS (
	UPDATE {s}.batches b SET finished_at = now()
	WHERE b.id = ANY($1::bigint[]` + completeWhere + batchDeps

const sqlReleaseBatches = `WITH b AS (
	SELECT t.id FROM unnest($1::bigint[]) AS t(id)
	WHERE NOT EXISTS (SELECT 1 FROM {s}.batches x WHERE x.id = t.id AND x.finished_at IS NULL)
)` + batchDeps

const sqlBusy = `SELECT o.id, o.claim FROM unnest($1::bigint[], $2::int[]) AS o(id, claim)
JOIN {s}.jobs j ON j.id = o.id AND j.claim = o.claim AND j.state = 'processing'`

func (s *Store) Finish(ctx context.Context, server string, outs []driver.Outcome) ([]driver.Result, error) {
	res := make([]driver.Result, len(outs))
	idx := make([]int, 0, len(outs))
	for i := range outs {
		switch outs[i].State {
		case driver.Succeeded, driver.Failed, driver.Deleted, driver.Scheduled, driver.Enqueued, driver.Throttled:
			if bytes.IndexByte(outs[i].Output, 0) < 0 {
				idx = append(idx, i)
				continue
			}
		}
		res[i] = driver.Rejected
	}
	var w wake
	if err := s.finish(ctx, server, outs, idx, res, &w); err != nil {
		return nil, fmt.Errorf("kiln: finish: %w", err)
	}
	s.nt.jobs(w.queues...)
	return res, nil
}

func (s *Store) finish(ctx context.Context, server string, outs []driver.Outcome, idx []int, res []driver.Result, w *wake) error {
	if len(idx) == 0 {
		return nil
	}
	err := s.finishOnce(ctx, server, outs, idx, res, w)
	if err == nil || !dataError(err) {
		return err
	}
	if len(idx) == 1 {
		res[idx[0]] = driver.Rejected
		return nil
	}
	h := len(idx) / 2
	if err := s.finish(ctx, server, outs, idx[:h], res, w); err != nil {
		return err
	}
	return s.finish(ctx, server, outs, idx[h:], res, w)
}

func (s *Store) finishOnce(ctx context.Context, server string, outs []driver.Outcome, idx []int, res []driver.Result, w *wake) error {
	n := len(idx)
	var (
		ids     = make([]int64, n)
		claims  = make([]int32, n)
		wants   = make([]string, n)
		delays  = make([]int64, n)
		refunds = make([]bool, n)
		reasons = make([]string, n)
		errs    = make([]string, n)
		traces  = make([]string, n)
		outputs = make([]string, n)
	)
	for k, i := range idx {
		o := &outs[i]
		ids[k], claims[k] = o.ID, o.Claim
		wants[k] = string(o.State)
		if o.State == driver.Throttled {
			wants[k] = string(driver.Enqueued)
		}
		delays[k] = micros(o.Delay)
		refunds[k] = o.Refund
		reasons[k] = clean(o.Reason, 256)
		errs[k] = clean(o.Error, 2<<10)
		traces[k] = clean(o.Trace, 8<<10)
		outputs[k] = string(o.Output)
	}
	applied := make(map[int64]int32, n)
	busy := make(map[int64]int32)
	var local wake
	b := &pgx.Batch{}
	b.Queue(s.q.finish, ids, claims, wants, delays, refunds, reasons, errs, traces, outputs, server).
		Query(func(rows pgx.Rows) error {
			var (
				id     int64
				claim  int32
				fin, q string
			)
			for rows.Next() {
				if err := rows.Scan(&id, &claim, &fin, &q); err != nil {
					return err
				}
				applied[id] = claim
				if fin == string(driver.Enqueued) {
					local.queue(q)
				}
			}
			return rows.Err()
		})
	b.Queue(s.q.finishDeps, ids, server).Query(local.scanQueues)
	b.Queue(s.q.admitFinished, ids).Query(local.scanAdmitted)
	b.Queue(s.q.lockFinishedBatches, ids)
	b.Queue(s.q.completeFinished, ids).Query(local.scanStates)
	b.Queue(s.q.busy, ids, claims).Query(func(rows pgx.Rows) error {
		var (
			id    int64
			claim int32
		)
		for rows.Next() {
			if err := rows.Scan(&id, &claim); err != nil {
				return err
			}
			busy[id] = claim
		}
		return rows.Err()
	})
	if err := s.pool.SendBatch(ctx, b).Close(); err != nil {
		return err
	}
	for k, i := range idx {
		id, claim := ids[k], claims[k]
		if c, ok := applied[id]; ok && c == claim {
			res[i] = driver.Applied
			delete(applied, id)
			continue
		}
		if c, ok := busy[id]; ok && c == claim {
			res[i] = driver.Busy
			continue
		}
		res[i] = driver.Stale
	}
	w.merge(local)
	return nil
}

func (w *wake) scanQueues(rows pgx.Rows) error {
	var q string
	for rows.Next() {
		if err := rows.Scan(&q); err != nil {
			return err
		}
		w.queue(q)
	}
	return rows.Err()
}

func (w *wake) scanStates(rows pgx.Rows) error {
	var q, state string
	for rows.Next() {
		if err := rows.Scan(&q, &state); err != nil {
			return err
		}
		if state == string(driver.Enqueued) {
			w.queue(q)
		}
	}
	return rows.Err()
}

package pgstore

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rafaelaugustos/kiln/driver"
)

const chunk = 1000

const sqlDelete = `WITH w AS MATERIALIZED (
	SELECT j.id, j.state = 'awaiting' AS late FROM {s}.jobs j WHERE %s AND j.id > $1
	ORDER BY j.id
	LIMIT 1000
), t AS MATERIALIZED (
	SELECT j.id, j.state, j.unique_key, j.limit_key, j.batch_id, j.cancel_requested
	FROM {s}.jobs j WHERE j.id = ANY(ARRAY(SELECT id FROM w WHERE NOT late)) AND %s
	ORDER BY j.id
	FOR NO KEY UPDATE OF j
), r AS (
	DELETE FROM {s}.uniques q WHERE q.key = ANY(ARRAY(
		SELECT h.key FROM {s}.uniques h JOIN t ON h.key = t.unique_key AND h.job_id = t.id
		WHERE t.state <> 'processing'
		ORDER BY h.key
		FOR UPDATE OF h))
	RETURNING q.key
), ks AS MATERIALIZED (
	SELECT l.key FROM {s}.limits l
	WHERE l.key IN (SELECT limit_key FROM t WHERE state = 'enqueued') AND (SELECT count(*) FROM r) >= 0
	ORDER BY l.key
	FOR KEY SHARE
), k AS MATERIALIZED (
	SELECT l.key FROM {s}.limits l
	WHERE l.key IN (SELECT key FROM ks)
	ORDER BY l.key
	FOR NO KEY UPDATE
), m AS (
	UPDATE {s}.limits l SET active = greatest(l.active - n.n, 0)
	FROM (SELECT limit_key, count(*) AS n FROM t WHERE state = 'enqueued' AND limit_key IS NOT NULL GROUP BY limit_key) n
	WHERE l.key = n.limit_key AND l.key IN (SELECT key FROM k)
), v AS MATERIALIZED (
	SELECT j.id, j.state, j.unique_key, j.limit_key, j.batch_id, j.cancel_requested
	FROM {s}.jobs j WHERE j.id = ANY(ARRAY(SELECT id FROM w WHERE late)) AND %s
		AND (j.state <> 'enqueued' OR j.limit_key IS NULL) AND (SELECT count(*) FROM k) >= 0
	ORDER BY j.id
	FOR NO KEY UPDATE OF j
), rv AS (
	DELETE FROM {s}.uniques q WHERE q.key = ANY(ARRAY(
		SELECT h.key FROM {s}.uniques h JOIN v ON h.key = v.unique_key AND h.job_id = v.id
		WHERE v.state <> 'processing'
		ORDER BY h.key
		FOR UPDATE OF h))
), x AS (
	SELECT id, state, batch_id, cancel_requested FROM t
	UNION ALL SELECT id, state, batch_id, cancel_requested FROM v
), c AS (
	UPDATE {s}.jobs j SET cancel_requested = true
	WHERE j.id = ANY(ARRAY(SELECT id FROM x WHERE state = 'processing' AND NOT cancel_requested))
	RETURNING j.id
), a AS (
	DELETE FROM {s}.jobs j WHERE j.id = ANY(ARRAY(SELECT id FROM x WHERE state <> 'processing'))
	RETURNING ` + movedColumns + `
), ar AS (
	INSERT INTO {s}.archive (` + archiveColumns + `)
	SELECT a.id, 'deleted', a.queue, a.kind, a.priority, a.attempt, a.max_attempts, a.claim, a.timeout_ms, a.run_at,
		a.created_at, a.attempted_at, now(), a.server, a.batch_id, a.after_batch, a.parents, a.recurring_id,
		a.unique_key, a.limit_key, a.args, a.meta, a.tags,
		{s}.push(a.history, {s}.entry('deleted', a.attempt, 'deleted', '', '', NULL)), NULL, a.progress
	FROM a
)
SELECT w.id, coalesce(x.state::text, ''), coalesce(x.batch_id, 0), w.id IN (SELECT id FROM c)
FROM w LEFT JOIN x ON x.id = w.id ORDER BY w.id`

const sqlLockBatches = `SELECT id FROM {s}.batches WHERE id = ANY($1) AND finished_at IS NULL ORDER BY id FOR NO KEY UPDATE`

const sqlCountDeleted = `INSERT INTO {s}.stats AS x (bucket, server, deleted) VALUES (date_trunc('minute', now()), $1, $2)
ON CONFLICT (bucket, server) DO UPDATE SET deleted = x.deleted + excluded.deleted`

const requeueClaim = `, c AS (
	INSERT INTO {s}.uniques AS u (key, job_id)
	SELECT DISTINCT ON (t.unique_key) t.unique_key, t.id FROM t WHERE t.unique_key IS NOT NULL
	ORDER BY t.unique_key, t.id
	ON CONFLICT (key) DO UPDATE SET job_id = excluded.job_id, expires_at = NULL
	WHERE u.job_id = excluded.job_id OR u.expires_at <= now() OR u.expires_at IS NULL AND (
		EXISTS (SELECT 1 FROM {s}.archive a WHERE a.id = u.job_id)
		OR EXISTS (SELECT 1 FROM {s}.jobs h WHERE h.id = u.job_id AND h.state = 'failed'))
	RETURNING u.job_id
)`

const requeueResult = `
SELECT (SELECT max(id) FROM t), (SELECT count(*) FROM t), count(*),
	coalesce(array_agg(DISTINCT queue) FILTER (WHERE state = 'enqueued'), '{}'),
	coalesce(array_agg(DISTINCT limit_key) FILTER (WHERE limit_key IS NOT NULL), '{}')
FROM u`

const sqlRequeueLive = `WITH t AS MATERIALIZED (
	SELECT j.id, j.unique_key FROM {s}.jobs j
	WHERE %s AND j.state IN ('failed', 'scheduled') AND j.id > $1
	ORDER BY j.id
	LIMIT 1000
	FOR NO KEY UPDATE OF j
)` + requeueClaim + `, u AS (
	UPDATE {s}.jobs j SET state = {s}.ready(now(), j.limit_key), run_at = now(), finalized_at = NULL,
		cancel_requested = false, granted = false, attempt = CASE WHEN j.state = 'scheduled' THEN j.attempt ELSE 0 END,
		max_attempts = CASE WHEN j.state = 'scheduled' THEN greatest(j.max_attempts, j.attempt + 1) ELSE j.max_attempts END,
		history = {s}.push(j.history, {s}.entry({s}.ready(now(), j.limit_key),
			CASE WHEN j.state = 'scheduled' THEN j.attempt ELSE 0 END, 'requeued', '', '', NULL))
	WHERE j.id = ANY(ARRAY(SELECT id FROM t WHERE unique_key IS NULL UNION ALL SELECT job_id FROM c))
	RETURNING j.queue, j.state, j.limit_key
)` + requeueResult

const sqlRequeueArchived = `WITH t AS MATERIALIZED (
	SELECT j.id, j.unique_key FROM {s}.archive j
	WHERE %s AND j.id > $1
	ORDER BY j.id
	LIMIT 1000
	FOR UPDATE OF j
)` + requeueClaim + `, m AS (
	DELETE FROM {s}.archive a
	WHERE a.id = ANY(ARRAY(SELECT id FROM t WHERE unique_key IS NULL UNION ALL SELECT job_id FROM c))
	RETURNING a.id, a.queue, a.kind, a.priority, a.attempt, a.max_attempts, a.claim, a.timeout_ms, a.created_at,
		a.attempted_at, a.server, a.batch_id, a.after_batch, a.parents, a.recurring_id, a.unique_key, a.limit_key,
		a.args, a.meta, a.tags, a.history, a.progress
), u AS (
	INSERT INTO {s}.jobs (id, state, queue, kind, priority, attempt, max_attempts, claim, timeout_ms, run_at,
		created_at, attempted_at, server, batch_id, after_batch, parents, recurring_id, unique_key, limit_key,
		args, meta, tags, history, progress)
	SELECT m.id, {s}.ready(now(), m.limit_key), m.queue, m.kind, m.priority, 0, m.max_attempts, m.claim,
		m.timeout_ms, now(), m.created_at, m.attempted_at, m.server, m.batch_id, m.after_batch, m.parents,
		m.recurring_id, m.unique_key, m.limit_key, m.args, m.meta, m.tags,
		{s}.push(m.history, {s}.entry({s}.ready(now(), m.limit_key), 0, 'requeued', '', '', NULL)), m.progress
	FROM m
	RETURNING queue, state, limit_key
)` + requeueResult

const sqlPause = `INSERT INTO {s}.queues (name, paused, updated_at) VALUES ($1, $2, now())
ON CONFLICT (name) DO UPDATE SET paused = excluded.paused, updated_at = excluded.updated_at`

func filterSQL(f driver.Filter, args []any) (string, []any) {
	var conds []string
	add := func(cond string, v any) {
		args = append(args, v)
		conds = append(conds, strings.ReplaceAll(cond, "?", "$"+strconv.Itoa(len(args))))
	}
	if len(f.IDs) > 0 {
		add("j.id = ANY(?)", f.IDs)
	}
	if f.State != "" {
		conds = append(conds, "j.state = '"+string(f.State)+"'")
	}
	if f.Queue != "" {
		add("j.queue = ?", f.Queue)
	}
	if f.Kind != "" {
		add("j.kind = ?", f.Kind)
	}
	if f.BatchID != 0 {
		add("j.batch_id = ?", f.BatchID)
	}
	if f.RecurringID != "" {
		add("j.recurring_id = ?", f.RecurringID)
	}
	if len(conds) == 0 {
		return "true", args
	}
	return strings.Join(conds, " AND "), args
}

// Delete deletes the jobs that match f, as [driver.Admin.Delete] describes, in transactions of up
// to 1000 jobs taken in order of id, so an error leaves the earlier transactions applied. The
// servers running jobs it marks for cancellation are told on the <schema>_cancel channel.
func (s *Store) Delete(ctx context.Context, f driver.Filter) (int, error) {
	if err := driver.CheckFilter(f); err != nil {
		return 0, err
	}
	if f.State.Archived() {
		return 0, nil
	}
	cond, args := filterSQL(f, []any{int64(0)})
	q := fmt.Sprintf(s.q.delete, cond, cond, cond)
	total := 0
	for {
		n, last, err := s.deleteChunk(ctx, q, args)
		if err != nil {
			return total, err
		}
		total += n
		if last == 0 {
			return total, nil
		}
		args[0] = last
	}
}

func (s *Store) deleteChunk(ctx context.Context, q string, args []any) (int, int64, error) {
	var (
		rows                       int
		last                       int64
		deleted, canceled, batches []int64
		w                          wake
	)
	err := s.txn(ctx, func(c *pgxpool.Conn) error {
		b := &pgx.Batch{}
		b.Queue("begin")
		b.Queue(q, args...).Query(func(r pgx.Rows) error {
			var (
				id, batch int64
				state     string
				cancel    bool
			)
			for r.Next() {
				if err := r.Scan(&id, &state, &batch, &cancel); err != nil {
					return err
				}
				rows++
				last = id
				switch {
				case state == "":
				case state != string(driver.Processing):
					deleted = append(deleted, id)
					if batch != 0 && !slices.Contains(batches, batch) {
						batches = append(batches, batch)
					}
				case cancel:
					canceled = append(canceled, id)
				}
			}
			return r.Err()
		})
		if err := c.SendBatch(ctx, b).Close(); err != nil {
			return err
		}
		_, err := s.cascade(ctx, c, &pgx.Batch{}, deleted, batches, len(deleted), &w)
		return err
	})
	if err != nil {
		return 0, 0, wrap("delete", err)
	}
	s.nt.cancel(canceled...)
	s.nt.jobs(w.queues...)
	if rows < chunk {
		last = 0
	}
	return len(deleted) + len(canceled), last, nil
}

// Requeue moves the jobs that match f back to their queues, as [driver.Admin.Requeue] describes,
// in statements of up to 1000 jobs, failed and scheduled ones before archived ones, so an error
// leaves the earlier statements applied.
func (s *Store) Requeue(ctx context.Context, f driver.Filter) (int, error) {
	if err := driver.CheckFilter(f); err != nil {
		return 0, err
	}
	total := 0
	live := f.State == "" || f.State == driver.Failed || f.State == driver.Scheduled
	if live {
		n, err := s.requeue(ctx, s.q.requeueLive, f)
		if err != nil {
			return total, err
		}
		total += n
	}
	if f.State == "" || f.State.Archived() {
		n, err := s.requeue(ctx, s.q.requeueArchived, f)
		if err != nil {
			return total, err
		}
		total += n
	}
	return total, nil
}

func (s *Store) requeue(ctx context.Context, tmpl string, f driver.Filter) (int, error) {
	cond, args := filterSQL(f, []any{int64(0)})
	q := fmt.Sprintf(tmpl, cond)
	total := 0
	for {
		var (
			last         *int64
			seen, moved  int
			queues, keys []string
		)
		err := s.pool.QueryRow(ctx, q, args...).Scan(&last, &seen, &moved, &queues, &keys)
		if err != nil {
			return total, wrap("requeue", err)
		}
		total += moved
		w := wake{queues: queues}
		if len(keys) > 0 {
			s.admit(ctx, rules{keys: keys}, &w)
		}
		s.nt.jobs(w.queues...)
		if seen < chunk || last == nil {
			return total, nil
		}
		args[0] = *last
	}
}

// PauseQueue pauses or resumes queue and tells the servers on the <schema>_queue channel. An
// empty queue name is [driver.ErrInvalid].
func (s *Store) PauseQueue(ctx context.Context, queue string, paused bool) error {
	if queue == "" {
		return fmt.Errorf("%w: empty queue", driver.ErrInvalid)
	}
	if _, err := s.pool.Exec(ctx, s.q.pause, queue, paused); err != nil {
		return wrap("pause queue", err)
	}
	s.nt.queue(queue)
	return nil
}

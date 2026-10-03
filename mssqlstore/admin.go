package mssqlstore

import (
	"bytes"
	"cmp"
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

const chunk = 1000

const sqlLockTargets = `SELECT TOP (1000) CAST(SYSUTCDATETIME() AS DATETIME2(6)), j.id, j.state, j.attempt,
	COALESCE(j.limit_key, N''), COALESCE(j.batch_id, 0), j.cancel_requested, j.unique_key,
	CASE WHEN EXISTS (SELECT 1 FROM {p}deps d WHERE d.batch = 0 AND d.parent_id = j.id AND d.resolved = 0) THEN 1 ELSE 0 END
FROM {p}jobs j WITH (XLOCK, READPAST, ROWLOCK)
WHERE j.id > @after AND `

const sqlCancel = `UPDATE j SET cancel_requested = 1
FROM OPENJSON(@cancel) WITH (id BIGINT '$') v JOIN {p}jobs j WITH (FORCESEEK) ON j.id = v.id;
`

const sqlLockFailed = `SELECT TOP (1000) CAST(SYSUTCDATETIME() AS DATETIME2(6)), j.id,
	CASE WHEN j.state = 'scheduled' THEN j.attempt ELSE 0 END, j.queue, j.unique_key, COALESCE(j.limit_key, N''), NULL
FROM {p}jobs j WITH (UPDLOCK, ROWLOCK)
WHERE j.state IN ('failed', 'scheduled') AND j.id > @after AND `

const sqlLockArchived = `SELECT TOP (1000) CAST(SYSUTCDATETIME() AS DATETIME2(6)), j.id, 0, j.queue, j.unique_key,
	COALESCE(j.limit_key, N''), j.finalized_at
FROM {p}archive j WITH (UPDLOCK, ROWLOCK)
WHERE `

const sqlReclaim = `MERGE {p}uniques AS u
USING (
	SELECT CONVERT(VARBINARY(64), v.k, 2) AS k, v.id, o.job_id AS seen,
		CASE WHEN o.expires_at <= SYSUTCDATETIME() OR o.expires_at IS NULL AND h.id IS NULL THEN 1 ELSE 0 END AS released
	FROM OPENJSON(@claims) WITH (k VARCHAR(130) '$.k', id BIGINT '$.id') v
	LEFT JOIN {p}uniques o ON o.unique_key = CONVERT(VARBINARY(64), v.k, 2)
	LEFT JOIN {p}jobs h ON h.id = o.job_id AND h.state <> 'failed'
) AS v
ON u.unique_key = v.k
WHEN MATCHED AND (u.job_id = v.id OR v.released = 1) AND u.job_id = v.seen THEN UPDATE SET job_id = v.id, expires_at = NULL
WHEN NOT MATCHED THEN INSERT (unique_key, job_id, expires_at) VALUES (v.k, v.id, NULL);
SELECT u.unique_key, u.job_id FROM OPENJSON(@claims) WITH (k VARCHAR(130) '$.k') v
JOIN {p}uniques u ON u.unique_key = CONVERT(VARBINARY(64), v.k, 2)`

const requeueRows = `
FROM OPENJSON(@rows) WITH (id BIGINT '$.id', state VARCHAR(10) '$.s', entry NVARCHAR(MAX) '$.e' AS JSON,
	attempt INT '$.a') v`

const sqlRequeueLive = `UPDATE j SET state = v.state, run_at = @now, finalized_at = NULL, cancel_requested = 0, granted = 0,
	attempt = v.attempt, max_attempts = CASE WHEN j.max_attempts < v.attempt + 1 THEN v.attempt + 1 ELSE j.max_attempts END,
	history = ` + pushHistory + requeueRows + `
JOIN {p}jobs j WITH (FORCESEEK) ON j.id = v.id`

const sqlRequeueArchived = `INSERT INTO {p}jobs (id, state, queue, kind, priority, attempt, max_attempts, claim, timeout_ms,
	deps_pending, run_at, created_at, attempted_at, server, batch_id, after_batch, parents, recurring_id, unique_key,
	limit_key, args, meta, tags, history, progress)
SELECT j.id, v.state, j.queue, j.kind, j.priority, v.attempt, j.max_attempts, j.claim, j.timeout_ms, 0, @now,
	j.created_at, j.attempted_at, j.server, j.batch_id, j.after_batch, j.parents, j.recurring_id, j.unique_key,
	j.limit_key, j.args, j.meta, j.tags, ` + pushHistory + `, j.progress` + requeueRows + `
JOIN {p}archive j WITH (FORCESEEK) ON j.id = v.id;
DELETE j FROM OPENJSON(@rows) WITH (id BIGINT '$.id') v JOIN {p}archive j WITH (FORCESEEK) ON j.id = v.id`

const sqlPause = `MERGE {p}queues WITH (HOLDLOCK) AS q
USING (SELECT @name COLLATE Latin1_General_100_BIN2 AS name) AS n ON q.name = n.name
WHEN MATCHED THEN UPDATE SET paused = @paused, updated_at = SYSUTCDATETIME()
WHEN NOT MATCHED THEN INSERT (name, paused, updated_at) VALUES (n.name, @paused, SYSUTCDATETIME());`

func filterSQL(f driver.Filter) (string, []any) {
	var (
		conds []string
		args  []any
	)
	if len(f.IDs) > 0 {
		conds = append(conds, "j.id IN (SELECT id FROM OPENJSON(@ids) WITH (id BIGINT '$'))")
		args = append(args, sql.Named("ids", idList(f.IDs)))
	}
	if f.State != "" {
		conds = append(conds, "j.state = CAST(@state AS VARCHAR(10))")
		args = append(args, sql.Named("state", string(f.State)))
	}
	if f.Queue != "" {
		conds = append(conds, "j.queue = @queue")
		args = append(args, sql.Named("queue", f.Queue))
	}
	if f.Kind != "" {
		conds = append(conds, "j.kind = @kind")
		args = append(args, sql.Named("kind", f.Kind))
	}
	if f.BatchID != 0 {
		conds = append(conds, "j.batch_id = @batch")
		args = append(args, sql.Named("batch", f.BatchID))
	}
	if f.RecurringID != "" {
		conds = append(conds, "j.recurring_id = @recurring")
		args = append(args, sql.Named("recurring", f.RecurringID))
	}
	return strings.Join(conds, " AND "), args
}

// Delete deletes the jobs that match f, as [driver.Admin.Delete] describes, in transactions of up
// to 1000 jobs taken in order of id, so an error leaves the earlier transactions applied, and the
// count it returns with the error covers only those. It skips jobs whose rows other transactions
// hold. The servers running jobs it marks for cancellation hear
// of it through the bus, or at their next heartbeat.
func (s *Store) Delete(ctx context.Context, f driver.Filter) (int, error) {
	if err := driver.CheckFilter(f); err != nil {
		return 0, err
	}
	if f.State.Archived() {
		return 0, nil
	}
	cond, args := filterSQL(f)
	stmt := s.q.lockTargets + cond + " ORDER BY j.id"
	total := 0
	var after int64
	for {
		n, seen, last, err := s.deleteChunk(ctx, stmt, append(args, sql.Named("after", after)))
		if err != nil {
			return total, fmt.Errorf("kiln: delete: %w", err)
		}
		total += n
		if seen < chunk {
			return total, nil
		}
		after = last
	}
}

func (s *Store) deleteChunk(ctx context.Context, stmt string, args []any) (n, seen int, last int64, err error) {
	var (
		fo       *fallout
		canceled []int64
	)
	err = s.txn(ctx, func(tx *sql.Tx) error {
		n, seen, last, canceled = 0, 0, 0, nil
		fo = &fallout{}
		rows, err := tx.QueryContext(ctx, stmt, args...)
		if err != nil {
			return err
		}
		var gone table
		for rows.Next() {
			var (
				now      moment
				id       int64
				state    string
				attempt  int
				limit    string
				batch    int64
				cancel   bool
				key      []byte
				children bool
			)
			if err := rows.Scan(&now, &id, &state, &attempt, &limit, &batch, &cancel, &key, &children); err != nil {
				rows.Close()
				return err
			}
			seen++
			last = id
			fo.now = now.Time
			switch driver.State(state) {
			case driver.Processing:
				if !cancel {
					canceled = append(canceled, id)
				}
				continue
			case driver.Enqueued:
				if limit != "" {
					fo.free(limit)
				}
			}
			e := entry{state: driver.Deleted, attempt: attempt, reason: "deleted"}
			gone.row()
			gone.int("id", id)
			gone.str("s", string(driver.Deleted))
			gone.int("a", int64(attempt))
			gone.json("e", e.encode(fo.now))
			fo.hold(key, id)
			if children {
				fo.parents = append(fo.parents, parent{id: id, state: driver.Deleted, label: string(driver.Deleted)})
			}
			fo.batch(batch)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		var write string
		if len(canceled) > 0 {
			write = s.q.cancel
		}
		if gone.n > 0 {
			write += s.q.archive
			fo.stats.deleted = gone.n
		}
		if write != "" {
			_, err := tx.ExecContext(ctx, write, sql.Named("now", stamp(fo.now)), sql.Named("cancel", idList(canceled)),
				sql.Named("gone", gone.String()))
			if err != nil {
				return err
			}
		}
		n = len(canceled) + gone.n
		return s.settle(ctx, tx, fo)
	})
	if err != nil {
		return n, seen, last, err
	}
	s.nt.cancel(canceled)
	s.nt.ready(fo.queues)
	return n, seen, last, nil
}

type requeued struct {
	id      int64
	attempt int
	queue   string
	key     []byte
	limit   string
}

type requeueCursor struct {
	id int64
	at time.Time
}

// Requeue moves the jobs that match f back to their queues, as [driver.Admin.Requeue] describes,
// in transactions of up to 1000 jobs, failed and scheduled ones before archived ones, so an error
// leaves the earlier transactions applied, and the count it returns with the error covers only
// those.
func (s *Store) Requeue(ctx context.Context, f driver.Filter) (int, error) {
	if err := driver.CheckFilter(f); err != nil {
		return 0, err
	}
	total := 0
	cond, args := filterSQL(f)
	if f.State == "" || f.State == driver.Failed || f.State == driver.Scheduled {
		stmt := s.q.lockFailed + cond + " ORDER BY j.id"
		var after int64
		for {
			n, seen, last, err := s.requeueChunk(ctx, stmt, append(args, sql.Named("after", after)), false)
			if err != nil {
				return total, fmt.Errorf("kiln: requeue: %w", err)
			}
			total += n
			if seen < chunk {
				break
			}
			after = last.id
		}
	}
	if f.State == "" || f.State.Archived() {
		var cursor *requeueCursor
		for {
			where, more := cond, args
			if cursor != nil {
				where += " AND (j.finalized_at < @at OR j.finalized_at = @at AND j.id < @after)"
				more = append(slices.Clone(args), sql.Named("at", stamp(cursor.at)), sql.Named("after", cursor.id))
			}
			stmt := s.q.lockArchived + where + " ORDER BY j.finalized_at DESC, j.id DESC"
			n, seen, last, err := s.requeueChunk(ctx, stmt, more, true)
			if err != nil {
				return total, fmt.Errorf("kiln: requeue: %w", err)
			}
			total += n
			if seen < chunk {
				break
			}
			cursor = &last
		}
	}
	return total, nil
}

func (s *Store) requeueChunk(ctx context.Context, stmt string, args []any, archived bool) (n, seen int, last requeueCursor, err error) {
	var queues []string
	err = s.txn(ctx, func(tx *sql.Tx) error {
		n, seen, last, queues = 0, 0, requeueCursor{}, nil
		rows, err := tx.QueryContext(ctx, stmt, args...)
		if err != nil {
			return err
		}
		var (
			now  moment
			jobs []requeued
		)
		for rows.Next() {
			var (
				r  requeued
				at moment
			)
			if err := rows.Scan(&now, &r.id, &r.attempt, &r.queue, &r.key, &r.limit, &at); err != nil {
				rows.Close()
				return err
			}
			seen++
			last = requeueCursor{id: r.id, at: at.Time}
			jobs = append(jobs, r)
		}
		rows.Close()
		if err := rows.Err(); err != nil || len(jobs) == 0 {
			return err
		}
		if jobs, err = s.reclaim(ctx, tx, jobs); err != nil || len(jobs) == 0 {
			return err
		}
		slices.SortFunc(jobs, func(a, b requeued) int { return cmp.Compare(a.id, b.id) })
		var (
			t    table
			keys []string
		)
		for _, r := range jobs {
			st := driver.Enqueued
			if r.limit != "" {
				st = driver.Throttled
				keys = merge(keys, r.limit)
			} else {
				queues = merge(queues, r.queue)
			}
			e := entry{state: st, attempt: r.attempt, reason: "requeued"}
			t.row()
			t.int("id", r.id)
			t.str("s", string(st))
			t.json("e", e.encode(now.Time))
			t.int("a", int64(r.attempt))
		}
		write := s.q.requeueLive
		if archived {
			write = s.q.requeueArchived
		}
		if _, err := tx.ExecContext(ctx, write, sql.Named("now", stamp(now.Time)), sql.Named("rows", t.String())); err != nil {
			return err
		}
		n = len(jobs)
		if len(keys) == 0 {
			return nil
		}
		slices.Sort(keys)
		slots, err := s.lockLimits(ctx, tx, keys, false)
		if err != nil {
			return err
		}
		a, err := s.fill(ctx, tx, slots)
		queues = merge(queues, a.queues...)
		return err
	})
	if err != nil {
		return n, seen, last, err
	}
	s.nt.ready(queues)
	return n, seen, last, nil
}

func (s *Store) reclaim(ctx context.Context, tx *sql.Tx, jobs []requeued) ([]requeued, error) {
	var claims []requeued
	first := make(map[string]bool)
	for _, r := range jobs {
		if r.key != nil && !first[string(r.key)] {
			first[string(r.key)] = true
			claims = append(claims, r)
		}
	}
	if len(claims) == 0 {
		return jobs, nil
	}
	slices.SortFunc(claims, func(a, b requeued) int { return bytes.Compare(a.key, b.key) })
	var t table
	for _, c := range claims {
		t.row()
		t.bin("k", c.key)
		t.int("id", c.id)
	}
	rows, err := tx.QueryContext(ctx, s.q.reclaim, sql.Named("claims", t.String()))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	holders := make(map[string]int64, len(claims))
	for rows.Next() {
		var (
			key []byte
			id  int64
		)
		if err := rows.Scan(&key, &id); err != nil {
			return nil, err
		}
		holders[string(key)] = id
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return slices.DeleteFunc(jobs, func(r requeued) bool {
		return r.key != nil && holders[string(r.key)] != r.id
	}), nil
}

// PauseQueue pauses or resumes queue. Claim skips a paused queue as soon as PauseQueue returns,
// whoever calls it; servers learn of the change, and start claiming from a resumed queue again,
// when the store's bus tells them or at their next heartbeat. An empty queue name is
// [driver.ErrInvalid].
func (s *Store) PauseQueue(ctx context.Context, queue string, paused bool) error {
	if queue == "" {
		return fmt.Errorf("%w: empty queue", driver.ErrInvalid)
	}
	err := s.retry(ctx, func() error {
		_, err := s.db.ExecContext(ctx, s.q.pause, sql.Named("name", queue), sql.Named("paused", paused))
		return err
	})
	if err != nil {
		return wrap("pause queue", err)
	}
	s.nt.changed(queue)
	return nil
}

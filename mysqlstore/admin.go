package mysqlstore

import (
	"cmp"
	"context"
	"database/sql"
	"fmt"
	"slices"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

const chunk = 1000

const sqlLockTargets = `SELECT UTC_TIMESTAMP(6), j.id, j.state, j.attempt, COALESCE(j.limit_key, ''), COALESCE(j.batch_id, 0),
	j.cancel_requested, u.unique_key,
	EXISTS (SELECT 1 FROM {p}deps d WHERE d.batch = FALSE AND d.parent_id = j.id AND d.resolved = FALSE)
FROM {p}jobs j FORCE INDEX (?)
LEFT JOIN {p}uniques u FORCE INDEX (PRIMARY) ON u.unique_key = j.unique_key AND u.job_id = j.id
WHERE ? AND j.id > ?
ORDER BY j.id
LIMIT 1000
FOR UPDATE OF j FOR UPDATE OF u SKIP LOCKED`

const sqlCancel = `UPDATE {p}jobs SET cancel_requested = TRUE WHERE id IN (?)`

const sqlLockFailed = `SELECT UTC_TIMESTAMP(6), j.id, j.attempt, j.unique_key, COALESCE(j.limit_key, '')
FROM {p}jobs j FORCE INDEX (?)
WHERE ? AND j.state IN ('failed', 'scheduled') AND j.id > ?
ORDER BY j.id
LIMIT 1000
FOR UPDATE`

const sqlLockArchived = `SELECT UTC_TIMESTAMP(6), j.id, j.attempt, j.unique_key, COALESCE(j.limit_key, ''), j.finalized_at
FROM {p}archive j FORCE INDEX (?)
WHERE ?
ORDER BY j.finalized_at DESC, j.id DESC
LIMIT 1000
FOR UPDATE`

const released = `(o.expires_at <= UTC_TIMESTAMP(6) OR o.expires_at IS NULL AND h.id IS NULL)`

const sqlReclaimTail = `) AS v (k, id, exp)
LEFT JOIN {p}uniques o ON o.unique_key = v.k
LEFT JOIN {p}jobs h ON h.id = o.job_id AND h.state <> 'failed'
ORDER BY v.k
ON DUPLICATE KEY UPDATE
	job_id = IF(({p}uniques.job_id = v.id OR ` + released + `) AND {p}uniques.job_id <=> o.job_id, v.id, {p}uniques.job_id),
	expires_at = IF({p}uniques.job_id = v.id, NULL, {p}uniques.expires_at)`

const sqlKeyHolders = `SELECT unique_key, job_id FROM {p}uniques WHERE unique_key IN (?)`

const sqlRequeueLive = `UPDATE (VALUES `

const sqlRequeueLiveTail = `) AS v (id, state, entry) STRAIGHT_JOIN {p}jobs j FORCE INDEX (PRIMARY) ON j.id = v.id
SET j.state = v.state, j.run_at = ?, j.finalized_at = NULL, j.cancel_requested = FALSE,
	j.max_attempts = GREATEST(j.max_attempts, j.attempt + 1), j.history = ` + pushHistory

const sqlRequeueArchived = `INSERT INTO {p}jobs (id, state, queue, kind, priority, attempt, max_attempts, claim, timeout_ms,
	deps_pending, run_at, created_at, attempted_at, server, batch_id, after_batch, parents, recurring_id, unique_key,
	limit_key, args, meta, tags, history)
SELECT j.id, v.state, j.queue, j.kind, j.priority, j.attempt, GREATEST(j.max_attempts, j.attempt + 1), j.claim,
	j.timeout_ms, 0, ?, j.created_at, j.attempted_at, j.server, j.batch_id, j.after_batch, j.parents, j.recurring_id,
	j.unique_key, j.limit_key, j.args, j.meta, j.tags, ` + pushHistory + `
FROM (VALUES `

const sqlRequeueArchivedTail = `) AS v (id, state, entry) STRAIGHT_JOIN {p}archive j FORCE INDEX (PRIMARY) ON j.id = v.id`

const sqlDropArchived = `DELETE FROM {p}archive WHERE id IN (?)`

const sqlPause = `INSERT INTO {p}queues (name, paused, updated_at) VALUES (?, ?, UTC_TIMESTAMP(6)) AS n
ON DUPLICATE KEY UPDATE paused = n.paused, updated_at = n.updated_at`

func checkFilter(f driver.Filter) error {
	if len(f.IDs) == 0 && f.State == "" {
		return fmt.Errorf("%w: filter needs ids or a state", driver.ErrInvalid)
	}
	if f.State != "" && !f.State.Valid() {
		return fmt.Errorf("%w: state %q", driver.ErrInvalid, f.State)
	}
	return nil
}

func filterSQL(f driver.Filter) (index, cond raw) {
	var b []byte
	and := func(q string, v any) {
		if b != nil {
			b = append(b, " AND "...)
		}
		b = appendSQL(b, q, v)
	}
	index = "jobs_state"
	if len(f.IDs) > 0 {
		index = "PRIMARY"
		and("j.id IN (?)", f.IDs)
	}
	if f.State != "" {
		and("j.state = ?", f.State)
	}
	if f.Queue != "" {
		and("j.queue = ?", f.Queue)
	}
	if f.Kind != "" {
		and("j.kind = ?", f.Kind)
	}
	if f.BatchID != 0 {
		and("j.batch_id = ?", f.BatchID)
	}
	if f.RecurringID != "" {
		and("j.recurring_id = ?", f.RecurringID)
	}
	return index, raw(b)
}

func (s *Store) Delete(ctx context.Context, f driver.Filter) (int, error) {
	if err := checkFilter(f); err != nil {
		return 0, err
	}
	if f.State.Archived() {
		return 0, nil
	}
	index, cond := filterSQL(f)
	total := 0
	var after int64
	for {
		n, seen, last, err := s.deleteChunk(ctx, render(s.q.lockTargets, index, cond, after))
		total += n
		if err != nil {
			return total, fmt.Errorf("kiln: delete: %w", err)
		}
		if seen < chunk {
			return total, nil
		}
		after = last
	}
}

func (s *Store) deleteChunk(ctx context.Context, stmt string) (n, seen int, last int64, err error) {
	err = s.txn(ctx, func(tx *sql.Tx) error {
		n, seen, last = 0, 0, 0
		rows, err := tx.QueryContext(ctx, stmt)
		if err != nil {
			return err
		}
		fo := &fallout{}
		var (
			canceled, gone []int64
			archive        []byte
		)
		for rows.Next() {
			var (
				now      stamp
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
			if archive == nil {
				archive = appendSQL(archive, s.q.archive, fo.now)
			} else {
				archive = append(archive, ',')
			}
			archive = appendSQL(archive, "ROW(?, 'deleted', ?, ?, NULL)", id, attempt, e.encode(fo.now))
			gone = append(gone, id)
			if key != nil {
				fo.keys = append(fo.keys, key)
			}
			if children {
				fo.parents = append(fo.parents, parent{id: id, state: driver.Deleted, label: string(driver.Deleted)})
			}
			fo.batch(batch)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		if len(canceled) > 0 {
			if _, err := tx.ExecContext(ctx, render(s.q.cancel, canceled)); err != nil {
				return err
			}
		}
		if len(gone) > 0 {
			if _, err := tx.ExecContext(ctx, string(append(archive, s.q.archiveTail...))); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, render(s.q.remove, gone)); err != nil {
				return err
			}
			fo.stats.deleted = len(gone)
		}
		n = len(canceled) + len(gone)
		return s.settle(ctx, tx, fo)
	})
	return n, seen, last, err
}

type requeued struct {
	id      int64
	attempt int
	key     []byte
	limit   string
}

func (s *Store) Requeue(ctx context.Context, f driver.Filter) (int, error) {
	if err := checkFilter(f); err != nil {
		return 0, err
	}
	total := 0
	if f.State == "" || f.State == driver.Failed || f.State == driver.Scheduled {
		index, cond := filterSQL(f)
		var after int64
		for {
			n, seen, last, err := s.requeueChunk(ctx, render(s.q.lockFailed, index, cond, after), false)
			total += n
			if err != nil {
				return total, fmt.Errorf("kiln: requeue: %w", err)
			}
			if seen < chunk {
				break
			}
			after = last.id
		}
	}
	if f.State == "" || f.State.Archived() {
		index, cond := filterSQL(f)
		if index != "PRIMARY" {
			index = "archive_state"
		}
		var cursor *requeueCursor
		for {
			where := cond
			if cursor != nil {
				where += raw(render(" AND (j.finalized_at < ? OR j.finalized_at = ? AND j.id < ?)", cursor.at, cursor.at, cursor.id))
			}
			n, seen, last, err := s.requeueChunk(ctx, render(s.q.lockArchived, index, where), true)
			total += n
			if err != nil {
				return total, fmt.Errorf("kiln: requeue: %w", err)
			}
			if seen < chunk {
				break
			}
			cursor = &last
		}
	}
	return total, nil
}

type requeueCursor struct {
	id int64
	at time.Time
}

func (s *Store) requeueChunk(ctx context.Context, stmt string, archived bool) (n, seen int, last requeueCursor, err error) {
	err = s.txn(ctx, func(tx *sql.Tx) error {
		n, seen, last = 0, 0, requeueCursor{}
		rows, err := tx.QueryContext(ctx, stmt)
		if err != nil {
			return err
		}
		var (
			now  stamp
			jobs []requeued
		)
		for rows.Next() {
			var (
				r   requeued
				dst = []any{&now, &r.id, &r.attempt, &r.key, &r.limit}
				at  stamp
			)
			if archived {
				dst = append(dst, &at)
			}
			if err := rows.Scan(dst...); err != nil {
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
			b    []byte
			ids  []int64
			keys []string
		)
		if archived {
			b = appendSQL(b, s.q.requeueArchived, now.Time)
		} else {
			b = append(b, s.q.requeueLive...)
		}
		for i, r := range jobs {
			st := driver.Enqueued
			if r.limit != "" {
				st = driver.Throttled
				keys = merge(keys, []string{r.limit})
			}
			if i > 0 {
				b = append(b, ',')
			}
			e := entry{state: st, attempt: r.attempt, reason: "requeued"}
			b = appendSQL(b, "ROW(?, ?, ?)", r.id, st, e.encode(now.Time))
			ids = append(ids, r.id)
		}
		if archived {
			b = append(b, s.q.requeueArchivedTail...)
		} else {
			b = appendSQL(b, s.q.requeueLiveTail, now.Time)
		}
		if _, err := tx.ExecContext(ctx, string(b)); err != nil {
			return err
		}
		if archived {
			if _, err := tx.ExecContext(ctx, render(s.q.dropArchived, ids)); err != nil {
				return err
			}
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
		_, err = s.fill(ctx, tx, slots)
		return err
	})
	return n, seen, last, err
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
	b := append([]byte(nil), s.q.claimUniques...)
	keys := make([][]byte, len(claims))
	for i, c := range claims {
		if i > 0 {
			b = append(b, ',')
		}
		b = appendSQL(b, "ROW(?, ?, NULL)", c.key, c.id)
		keys[i] = c.key
	}
	if _, err := tx.ExecContext(ctx, string(append(b, s.q.reclaimTail...))); err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, render(s.q.keyHolders, keys))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	holders := make(map[string]int64, len(keys))
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

func (s *Store) PauseQueue(ctx context.Context, queue string, paused bool) error {
	if queue == "" {
		return fmt.Errorf("%w: empty queue", driver.ErrInvalid)
	}
	if _, err := s.db.ExecContext(ctx, render(s.q.pause, queue, paused)); err != nil {
		return wrap("pause queue", err)
	}
	return nil
}

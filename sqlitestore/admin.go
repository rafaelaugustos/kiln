package sqlitestore

import (
	"cmp"
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"

	"github.com/rafaelaugustos/kiln/driver"
)

const chunk = 1000

const sqlTargets = `SELECT {now}, j.id, j.state, j.attempt, COALESCE(j.limit_key, ''), COALESCE(j.batch_id, 0),
	j.cancel_requested, j.unique_key,
	EXISTS (SELECT 1 FROM {p}deps d WHERE d.batch = 0 AND d.parent_id = j.id AND d.resolved = 0)
FROM {p}jobs j
WHERE j.id > ?`

const sqlCancel = `UPDATE {p}jobs SET cancel_requested = 1 WHERE id IN (SELECT value FROM json_each(?))`

const sqlFailed = `SELECT {now}, j.id, j.attempt, j.queue, j.unique_key, COALESCE(j.limit_key, ''), 0
FROM {p}jobs j
WHERE j.state IN ('failed', 'scheduled') AND j.id > ?`

const sqlArchived = `SELECT {now}, j.id, j.attempt, j.queue, j.unique_key, COALESCE(j.limit_key, ''), j.finalized_at
FROM {p}archive j
WHERE (j.finalized_at < ? OR j.finalized_at = ? AND j.id < ?)`

const sqlRequeueLive = `UPDATE {p}jobs AS j SET state = ?, run_at = ?, finalized_at = NULL, cancel_requested = 0,
	granted = 0, max_attempts = max(j.max_attempts, j.attempt + 1), history = ` + pushHistory + `
WHERE j.id = ?`

const sqlRequeueArchived = `INSERT INTO {p}jobs (id, state, queue, kind, priority, attempt, max_attempts, claim,
	timeout_ms, deps_pending, run_at, created_at, attempted_at, server, batch_id, after_batch, parents, recurring_id,
	unique_key, limit_key, args, meta, tags, history)
SELECT j.id, ?, j.queue, j.kind, j.priority, j.attempt, max(j.max_attempts, j.attempt + 1), j.claim,
	j.timeout_ms, 0, ?, j.created_at, j.attempted_at, j.server, j.batch_id, j.after_batch, j.parents, j.recurring_id,
	j.unique_key, j.limit_key, j.args, j.meta, j.tags, ` + pushHistory + `
FROM {p}archive j WHERE j.id = ?`

const sqlDropArchived = `DELETE FROM {p}archive WHERE id IN (SELECT value FROM json_each(?))`

const sqlPause = `INSERT INTO {p}queues (name, paused, updated_at) VALUES (?, ?, {now})
ON CONFLICT (name) DO UPDATE SET paused = excluded.paused, updated_at = excluded.updated_at`

func where(f driver.Filter) (string, []any) {
	var (
		b    strings.Builder
		args []any
	)
	and := func(cond string, v any) {
		b.WriteString(" AND ")
		b.WriteString(cond)
		args = append(args, v)
	}
	if len(f.IDs) > 0 {
		and("j.id IN (SELECT value FROM json_each(?))", idList(f.IDs))
	}
	if f.State != "" {
		and("j.state = ?", string(f.State))
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
	return b.String(), args
}

func (s *Store) Delete(ctx context.Context, f driver.Filter) (int, error) {
	if err := driver.CheckFilter(f); err != nil {
		return 0, err
	}
	if f.State.Archived() {
		return 0, nil
	}
	cond, args := where(f)
	stmt := s.q.targets + cond + " ORDER BY j.id LIMIT ?"
	total := 0
	var after int64
	for {
		n, seen, last, err := s.deleteChunk(ctx, stmt, append(append([]any{after}, args...), chunk))
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

type target struct {
	id       int64
	state    driver.State
	attempt  int
	limit    string
	batch    int64
	cancel   bool
	key      []byte
	children bool
}

func (s *Store) deleteChunk(ctx context.Context, stmt string, args []any) (n, seen int, last int64, err error) {
	var (
		fo       *fallout
		canceled []int64
	)
	err = s.write(ctx, func(ctx context.Context, q querier) error {
		n, seen, last, canceled = 0, 0, 0, nil
		fo = &fallout{}
		var targets []target
		rows, err := q.QueryContext(ctx, stmt, args...)
		err = each(rows, err, func() error {
			var t target
			if err := rows.Scan(&fo.now, &t.id, &t.state, &t.attempt, &t.limit, &t.batch, &t.cancel, &t.key, &t.children); err != nil {
				return err
			}
			targets = append(targets, t)
			return nil
		})
		if err != nil || len(targets) == 0 {
			return err
		}
		seen, last = len(targets), targets[len(targets)-1].id
		gone := archiver{now: fo.now}
		for _, t := range targets {
			switch t.state {
			case driver.Processing:
				if !t.cancel {
					canceled = append(canceled, t.id)
				}
				continue
			case driver.Enqueued:
				if t.limit != "" {
					fo.free(t.limit)
				}
			}
			e := entry{state: driver.Deleted, attempt: t.attempt, reason: "deleted"}
			gone.add(t.id, driver.Deleted, t.attempt, e.encode(fo.now), nil)
			fo.hold(t.key, t.id)
			if t.children {
				fo.parents = append(fo.parents, parent{id: t.id, state: driver.Deleted, label: string(driver.Deleted)})
			}
			fo.batch(t.batch)
		}
		if len(canceled) > 0 {
			if _, err := q.ExecContext(ctx, s.q.cancel, idList(canceled)); err != nil {
				return err
			}
		}
		if err := s.archive(ctx, q, &gone); err != nil {
			return err
		}
		fo.stats.deleted += len(gone.ids)
		n = len(canceled) + len(gone.ids)
		return s.settle(ctx, q, fo)
	})
	if err != nil {
		return 0, seen, last, err
	}
	s.hub.cancel(canceled)
	s.hub.ready(fo.queues)
	return n, seen, last, nil
}

type requeued struct {
	id      int64
	attempt int
	queue   string
	key     []byte
	limit   string
	at      int64
}

func (s *Store) Requeue(ctx context.Context, f driver.Filter) (int, error) {
	if err := driver.CheckFilter(f); err != nil {
		return 0, err
	}
	cond, args := where(f)
	total := 0
	if f.State == "" || f.State == driver.Failed || f.State == driver.Scheduled {
		stmt := s.q.failed + cond + " ORDER BY j.id LIMIT ?"
		var after int64
		for {
			n, seen, last, err := s.requeueChunk(ctx, stmt, append(append([]any{after}, args...), chunk), false)
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
		stmt := s.q.archived + cond + " ORDER BY j.finalized_at DESC, j.id DESC LIMIT ?"
		cursor := requeued{id: 1<<63 - 1, at: 1<<63 - 1}
		for {
			n, seen, last, err := s.requeueChunk(ctx, stmt, append(append([]any{cursor.at, cursor.at, cursor.id}, args...), chunk), true)
			total += n
			if err != nil {
				return total, fmt.Errorf("kiln: requeue: %w", err)
			}
			if seen < chunk {
				break
			}
			cursor = last
		}
	}
	return total, nil
}

func (s *Store) requeueChunk(ctx context.Context, stmt string, args []any, archived bool) (n, seen int, last requeued, err error) {
	var fo *fallout
	err = s.write(ctx, func(ctx context.Context, q querier) error {
		n, seen, last = 0, 0, requeued{}
		fo = &fallout{}
		var jobs []requeued
		rows, err := q.QueryContext(ctx, stmt, args...)
		err = each(rows, err, func() error {
			var (
				r  requeued
				at sql.NullInt64
			)
			if err := rows.Scan(&fo.now, &r.id, &r.attempt, &r.queue, &r.key, &r.limit, &at); err != nil {
				return err
			}
			r.at = at.Int64
			seen++
			last = r
			jobs = append(jobs, r)
			return nil
		})
		if err != nil || len(jobs) == 0 {
			return err
		}
		if jobs, err = s.reclaim(ctx, q, fo.now, jobs); err != nil || len(jobs) == 0 {
			return err
		}
		slices.SortFunc(jobs, func(a, b requeued) int { return cmp.Compare(a.id, b.id) })
		rows2 := make([]any, 0, 3*len(jobs))
		ids := make([]int64, 0, len(jobs))
		for _, r := range jobs {
			st := driver.Enqueued
			if r.limit != "" {
				st = driver.Throttled
				fo.throttled(r.limit)
			} else {
				fo.queue(r.queue)
			}
			e := entry{state: st, attempt: r.attempt, reason: "requeued"}.encode(fo.now)
			rows2 = append(rows2, string(st), fo.now, e, e, r.id)
			ids = append(ids, r.id)
		}
		stmt := s.q.requeueLive
		if archived {
			stmt = s.q.requeueArchived
		}
		if err := execEach(ctx, q, stmt, 5, rows2); err != nil {
			return err
		}
		if archived {
			if _, err := q.ExecContext(ctx, s.q.dropArchived, idList(ids)); err != nil {
				return err
			}
		}
		n = len(jobs)
		return s.settle(ctx, q, fo)
	})
	if err != nil {
		return 0, seen, last, err
	}
	s.hub.ready(fo.queues)
	return n, seen, last, nil
}

func (s *Store) reclaim(ctx context.Context, q querier, now int64, jobs []requeued) ([]requeued, error) {
	var keys [][]byte
	first := make(map[string]int64)
	for _, r := range jobs {
		if r.key != nil {
			if _, ok := first[string(r.key)]; !ok {
				first[string(r.key)] = r.id
				keys = append(keys, r.key)
			}
		}
	}
	if len(keys) == 0 {
		return jobs, nil
	}
	taken, err := s.holding(ctx, q, now, keys)
	if err != nil {
		return nil, err
	}
	var hold []any
	jobs = slices.DeleteFunc(jobs, func(r requeued) bool {
		if r.key == nil {
			return false
		}
		if h, ok := taken[string(r.key)]; ok {
			return h.id != r.id
		}
		if first[string(r.key)] != r.id {
			return true
		}
		hold = append(hold, r.key, r.id, nil)
		return false
	})
	if err := insertRows(ctx, q, s.q.hold, sqlHoldTail, 3, hold); err != nil {
		return nil, err
	}
	return jobs, nil
}

func (s *Store) PauseQueue(ctx context.Context, queue string, paused bool) error {
	if queue == "" {
		return fmt.Errorf("%w: empty queue", driver.ErrInvalid)
	}
	err := s.write(ctx, func(ctx context.Context, q querier) error {
		_, err := q.ExecContext(ctx, s.q.pause, queue, paused)
		return err
	})
	if err != nil {
		return wrap("pause queue", err)
	}
	s.hub.publish(driver.Event{Kind: driver.QueueChanged, Queue: queue})
	return nil
}

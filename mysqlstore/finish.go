package mysqlstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

const sqlLockRunning = `SELECT UTC_TIMESTAMP(6), j.id, j.claim, j.attempt, j.queue, COALESCE(j.server, ''),
	COALESCE(j.limit_key, ''), COALESCE(j.batch_id, 0), j.cancel_requested, u.unique_key,
	EXISTS (SELECT 1 FROM {p}deps d WHERE d.batch = FALSE AND d.parent_id = j.id AND d.resolved = FALSE)
FROM {p}jobs j FORCE INDEX (PRIMARY)
LEFT JOIN {p}uniques u FORCE INDEX (PRIMARY) ON u.unique_key = j.unique_key AND u.job_id = j.id
WHERE j.id IN (?) AND j.state = 'processing'
ORDER BY j.id
FOR UPDATE OF j, u SKIP LOCKED`

const sqlUpdateLive = `UPDATE (VALUES `

const sqlUpdateLiveTail = `) AS v (id, state, attempt, delay, entry) STRAIGHT_JOIN {p}jobs j FORCE INDEX (PRIMARY) ON j.id = v.id
SET j.state = v.state, j.attempt = v.attempt,
	j.run_at = IF(v.state = 'failed', j.run_at, ? + INTERVAL v.delay MICROSECOND),
	j.finalized_at = IF(v.state = 'failed', ?, NULL), j.granted = FALSE,
	j.history = ` + pushHistory

const sqlBusy = `SELECT id, claim FROM {p}jobs WHERE id IN (?) AND state = 'processing'`

type running struct {
	id       int64
	claim    int32
	attempt  int
	queue    string
	server   string
	limit    string
	batch    int64
	cancel   bool
	key      []byte
	children bool
}

func (s *Store) Finish(ctx context.Context, server string, outs []driver.Outcome) ([]driver.Result, error) {
	res := make([]driver.Result, len(outs))
	idx := make([]int, 0, len(outs))
	for i := range outs {
		switch o := &outs[i]; o.State {
		case driver.Succeeded, driver.Failed, driver.Deleted, driver.Scheduled, driver.Enqueued, driver.Throttled:
			if len(o.Output) == 0 || json.Valid(o.Output) {
				idx = append(idx, i)
				continue
			}
		}
		res[i] = driver.Rejected
	}
	if err := s.finish(ctx, server, outs, idx, res); err != nil {
		return nil, fmt.Errorf("kiln: finish: %w", err)
	}
	return res, nil
}

func (s *Store) finish(ctx context.Context, server string, outs []driver.Outcome, idx []int, res []driver.Result) error {
	if len(idx) == 0 {
		return nil
	}
	err := s.finishOnce(ctx, server, outs, idx, res)
	if err == nil || !dataError(err) && !tooLarge(err) {
		return err
	}
	if len(idx) == 1 {
		res[idx[0]] = driver.Rejected
		return nil
	}
	h := len(idx) / 2
	if err := s.finish(ctx, server, outs, idx[:h], res); err != nil {
		return err
	}
	return s.finish(ctx, server, outs, idx[h:], res)
}

func (s *Store) finishOnce(ctx context.Context, server string, outs []driver.Outcome, idx []int, res []driver.Result) error {
	ids := make([]int64, 0, len(idx))
	for _, i := range idx {
		ids = append(ids, outs[i].ID)
	}
	slices.Sort(ids)
	ids = slices.Compact(ids)
	var (
		locked  map[int64]*running
		applied map[int64]int
	)
	err := s.txn(ctx, func(tx *sql.Tx) error {
		var (
			now time.Time
			err error
		)
		if now, locked, err = s.lockRunning(ctx, tx, ids); err != nil || len(locked) == 0 {
			return err
		}
		applied = make(map[int64]int, len(locked))
		for _, i := range idx {
			o := &outs[i]
			if r := locked[o.ID]; r != nil && r.claim == o.Claim {
				if _, dup := applied[o.ID]; !dup {
					applied[o.ID] = i
				}
			}
		}
		return s.apply(ctx, tx, &fallout{now: now, server: server}, outs, applied, locked)
	})
	if err != nil {
		return err
	}
	var missing []int64
	for _, id := range ids {
		if locked[id] == nil {
			missing = append(missing, id)
		}
	}
	busy, err := s.busy(ctx, missing)
	if err != nil {
		return err
	}
	for _, i := range idx {
		o := &outs[i]
		j, done := applied[o.ID]
		c, held := busy[o.ID]
		switch {
		case done && j == i:
			res[i] = driver.Applied
		case held && c == o.Claim:
			res[i] = driver.Busy
		default:
			res[i] = driver.Stale
		}
	}
	return nil
}

func (s *Store) lockRunning(ctx context.Context, tx *sql.Tx, ids []int64) (time.Time, map[int64]*running, error) {
	var now stamp
	rows, err := tx.QueryContext(ctx, render(s.q.lockRunning, ids))
	if err != nil {
		return now.Time, nil, err
	}
	defer rows.Close()
	locked := make(map[int64]*running, len(ids))
	for rows.Next() {
		r := &running{}
		err := rows.Scan(&now, &r.id, &r.claim, &r.attempt, &r.queue, &r.server, &r.limit, &r.batch, &r.cancel, &r.key, &r.children)
		if err != nil {
			return now.Time, nil, err
		}
		locked[r.id] = r
	}
	return now.Time, locked, rows.Err()
}

func (s *Store) busy(ctx context.Context, ids []int64) (map[int64]int32, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	rows, err := s.db.QueryContext(ctx, render(s.q.busy, ids))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	busy := make(map[int64]int32, len(ids))
	for rows.Next() {
		var (
			id    int64
			claim int32
		)
		if err := rows.Scan(&id, &claim); err != nil {
			return nil, err
		}
		busy[id] = claim
	}
	return busy, rows.Err()
}

func (s *Store) apply(ctx context.Context, q querier, f *fallout, outs []driver.Outcome, applied map[int64]int, locked map[int64]*running) error {
	order := make([]int64, 0, len(applied))
	for id := range applied {
		order = append(order, id)
	}
	slices.Sort(order)
	archive := &chunks{head: render(s.q.archive, f.now), tail: s.q.archiveTail, max: s.budget}
	live := &chunks{head: s.q.updateLive, tail: render(s.q.updateLiveTail, f.now, f.now), max: s.budget}
	var gone []int64
	for _, id := range order {
		o, r := &outs[applied[id]], locked[id]
		attempt := r.attempt
		if o.Refund {
			attempt = max(attempt-1, 0)
		}
		want := o.State
		if want == driver.Throttled {
			want = driver.Enqueued
		}
		canceled := r.cancel && want != driver.Succeeded && want != driver.Deleted
		delay := max(micros(o.Delay), 0)
		var fin driver.State
		switch {
		case r.cancel && want != driver.Succeeded:
			fin = driver.Deleted
		case want == driver.Scheduled && delay > 0:
			fin = driver.Scheduled
		case (want == driver.Scheduled || want == driver.Enqueued) && r.limit != "":
			fin = driver.Throttled
		case want == driver.Scheduled:
			fin = driver.Enqueued
		default:
			fin = want
		}
		e := entry{state: fin, attempt: attempt, reason: clean(o.Reason, 256), server: r.server}
		if canceled {
			e.reason = "canceled"
		}
		if e.reason != "" {
			e.err, e.trace = clean(o.Error, 2<<10), clean(o.Trace, 8<<10)
		}
		switch fin {
		case driver.Succeeded:
			f.stats.succeeded++
		case driver.Failed:
			f.stats.failed++
		case driver.Deleted:
			f.stats.deleted++
		}
		if want == driver.Scheduled && !o.Refund && !canceled {
			f.stats.retried++
		}
		final := fin == driver.Succeeded || fin == driver.Failed || fin == driver.Deleted
		if final && r.key != nil {
			f.keys = append(f.keys, r.key)
		}
		if final && r.children {
			f.parents = append(f.parents, parent{id: id, state: fin, label: string(fin)})
		}
		if r.limit != "" {
			f.free(r.limit)
		}
		if fin == driver.Succeeded || fin == driver.Deleted {
			var output []byte
			if len(o.Output) > 0 {
				output = o.Output
			}
			archive.row()
			archive.b = appendSQL(archive.b, "ROW(?, ?, ?, ?, ?)", id, fin, attempt, e.encode(f.now), output)
			gone = append(gone, id)
			f.batch(r.batch)
			continue
		}
		if fin != driver.Scheduled {
			delay = 0
		}
		live.row()
		live.b = appendSQL(live.b, "ROW(?, ?, ?, ?, ?)", id, fin, attempt, delay, e.encode(f.now))
	}
	if len(gone) > 0 {
		for _, stmt := range archive.done() {
			if _, err := q.ExecContext(ctx, stmt); err != nil {
				return err
			}
		}
		if _, err := q.ExecContext(ctx, render(s.q.remove, gone)); err != nil {
			return err
		}
	}
	for _, stmt := range live.done() {
		if _, err := q.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return s.settle(ctx, q, f)
}

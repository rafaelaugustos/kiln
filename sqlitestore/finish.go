package sqlitestore

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"

	"github.com/rafaelaugustos/kiln/driver"
)

const sqlRunning = `SELECT {now}, j.id, j.claim, j.attempt, j.queue, COALESCE(j.server, ''), COALESCE(j.limit_key, ''),
	COALESCE(j.batch_id, 0), j.cancel_requested, u.unique_key,
	EXISTS (SELECT 1 FROM {p}deps d WHERE d.batch = 0 AND d.parent_id = j.id AND d.resolved = 0)
FROM {p}jobs j
LEFT JOIN {p}uniques u ON u.unique_key = j.unique_key AND u.job_id = j.id
WHERE j.id IN (SELECT value FROM json_each(?)) AND j.state = 'processing'`

const sqlUpdate = `UPDATE {p}jobs AS j SET state = ?, attempt = ?, run_at = COALESCE(?, j.run_at), finalized_at = ?,
	history = ` + pushHistory + `
WHERE j.id = ?`

type running struct {
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
	if err == nil || !dataError(err) {
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
	applied := make(map[int64]int, len(ids))
	var f *fallout
	err := s.write(ctx, func(ctx context.Context, q querier) error {
		clear(applied)
		f = &fallout{server: server}
		jobs := make(map[int64]*running, len(ids))
		rows, err := q.QueryContext(ctx, s.q.running, idList(ids))
		err = each(rows, err, func() error {
			var (
				id int64
				r  running
			)
			err := rows.Scan(&f.now, &id, &r.claim, &r.attempt, &r.queue, &r.server, &r.limit, &r.batch, &r.cancel,
				&r.key, &r.children)
			if err != nil {
				return err
			}
			jobs[id] = &r
			return nil
		})
		if err != nil || len(jobs) == 0 {
			return err
		}
		for _, i := range idx {
			o := &outs[i]
			if r := jobs[o.ID]; r != nil && r.claim == o.Claim {
				if _, dup := applied[o.ID]; !dup {
					applied[o.ID] = i
				}
			}
		}
		return s.apply(ctx, q, f, outs, applied, jobs)
	})
	if err != nil {
		return err
	}
	for _, i := range idx {
		if j, ok := applied[outs[i].ID]; ok && j == i {
			res[i] = driver.Applied
		} else {
			res[i] = driver.Stale
		}
	}
	s.hub.ready(f.queues)
	return nil
}

func (s *Store) apply(ctx context.Context, q querier, f *fallout, outs []driver.Outcome, applied map[int64]int, jobs map[int64]*running) error {
	var live []any
	gone := archiver{now: f.now}
	for _, id := range slices.Sorted(maps.Keys(applied)) {
		o, r := &outs[applied[id]], jobs[id]
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
		case driver.Enqueued:
			f.queue(r.queue)
		case driver.Throttled:
			f.throttled(r.limit)
		}
		if want == driver.Scheduled && !o.Refund && !canceled {
			f.stats.retried++
		}
		if fin == driver.Succeeded || fin == driver.Failed || fin == driver.Deleted {
			f.hold(r.key, id)
			if r.children {
				f.parents = append(f.parents, parent{id: id, state: fin, label: string(fin)})
			}
		}
		if r.limit != "" {
			f.free(r.limit)
		}
		if fin == driver.Succeeded || fin == driver.Deleted {
			if fin == driver.Succeeded && e.reason == "" && len(o.Output) == 0 && attempt == r.attempt {
				gone.succeed(id)
			} else {
				gone.add(id, fin, attempt, e.encode(f.now), o.Output)
			}
			f.batch(r.batch)
			continue
		}
		var runAt, finalized any
		switch fin {
		case driver.Failed:
			finalized = f.now
		case driver.Scheduled:
			runAt = f.now + delay
		default:
			runAt = f.now
		}
		entry := e.encode(f.now)
		live = append(live, string(fin), attempt, runAt, finalized, entry, entry, id)
	}
	if err := s.archive(ctx, q, &gone); err != nil {
		return err
	}
	if err := execEach(ctx, q, s.q.update, 7, live); err != nil {
		return err
	}
	return s.settle(ctx, q, f)
}

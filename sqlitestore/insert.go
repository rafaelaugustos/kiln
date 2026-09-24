package sqlitestore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/rafaelaugustos/kiln/driver"
)

const sqlAllocate = `UPDATE {p}sequences SET last_id = last_id + ? WHERE name = 'jobs' RETURNING last_id, {now}`

const sqlInsertJobs = `INSERT INTO {p}jobs (id, state, queue, kind, priority, max_attempts, timeout_ms, deps_pending,
	run_at, created_at, batch_id, after_batch, parents, recurring_id, unique_key, limit_key, args, meta, tags) VALUES `

const sqlInsertDoomed = `INSERT INTO {p}archive (id, state, queue, kind, priority, attempt, max_attempts, claim,
	timeout_ms, run_at, created_at, finalized_at, batch_id, after_batch, parents, recurring_id, unique_key, limit_key,
	args, meta, tags, history) VALUES `

const sqlInsertDeps = `INSERT INTO {p}deps (batch, parent_id, job_id, mask, resolved) VALUES `

const sqlHolder = `SELECT u.job_id, u.expires_at, COALESCE(j.state, a.state, '')
FROM {p}uniques u
LEFT JOIN {p}jobs j ON j.id = u.job_id
LEFT JOIN {p}archive a ON a.id = u.job_id
WHERE u.unique_key = ?`

const sqlHold = `INSERT INTO {p}uniques (unique_key, job_id, expires_at) VALUES `

const sqlHoldTail = `
ON CONFLICT (unique_key) DO UPDATE SET job_id = excluded.job_id, expires_at = excluded.expires_at`

const sqlOpenBatches = `SELECT b.id, b.finished_at IS NULL AND (NOT b.sealed OR EXISTS (
	SELECT 1 FROM {p}jobs j WHERE j.batch_id = b.id))
FROM {p}batches b WHERE b.id IN (SELECT value FROM json_each(?))`

const sqlAttach = `UPDATE {p}batches SET total = total + ? WHERE id = ?`

type holder struct {
	id    int64
	state driver.State
}

type inserter struct {
	s       *Store
	jobs    []driver.InsertParams
	res     []driver.Inserted
	first   []int
	live    []int
	pos     []int
	ids     []int64
	won     []bool
	keys    [][]byte
	holders map[string]holder
	linked  bool
	batched bool
	now     int64
	link    *linker
	f       *fallout
}

func (s *Store) Insert(ctx context.Context, jobs []driver.InsertParams) ([]driver.Inserted, error) {
	if len(jobs) == 0 {
		return nil, nil
	}
	if err := driver.CheckInsert(jobs); err != nil {
		return nil, err
	}
	in := s.plan(jobs)
	if err := s.write(ctx, in.write); err != nil {
		return nil, wrap("insert", err)
	}
	s.hub.ready(in.f.queues)
	return in.res, nil
}

func (s *Store) plan(jobs []driver.InsertParams) *inserter {
	in := &inserter{s: s, jobs: jobs, first: make([]int, len(jobs)), pos: make([]int, len(jobs))}
	var seen map[string]int
	for i := range jobs {
		p := &jobs[i]
		in.first[i], in.pos[i] = -1, -1
		if len(p.Parents) > 0 || p.AfterBatch != 0 {
			in.linked = true
		}
		if p.BatchID != 0 {
			in.batched = true
		}
		if len(p.UniqueKey) > 0 {
			if seen == nil {
				seen = make(map[string]int)
			}
			if f, ok := seen[string(p.UniqueKey)]; ok {
				in.first[i] = f
				continue
			}
			seen[string(p.UniqueKey)] = i
			in.first[i] = i
			in.keys = append(in.keys, p.UniqueKey)
		}
		in.pos[i] = len(in.live)
		in.live = append(in.live, i)
	}
	return in
}

func (in *inserter) write(ctx context.Context, q querier) error {
	in.res = make([]driver.Inserted, len(in.jobs))
	in.won = make([]bool, len(in.live))
	in.holders, in.link = nil, nil
	var last int64
	if err := q.QueryRowContext(ctx, in.s.q.allocate, len(in.live)).Scan(&last, &in.now); err != nil {
		return err
	}
	in.f = &fallout{now: in.now}
	in.ids = make([]int64, len(in.live))
	for r := range in.ids {
		in.ids[r] = last - int64(len(in.live)-r) + 1
		in.won[r] = len(in.jobs[in.live[r]].UniqueKey) == 0
	}
	if len(in.keys) > 0 {
		if err := in.claim(ctx, q); err != nil {
			return err
		}
	}
	if in.batched {
		if err := in.attach(ctx, q); err != nil {
			return err
		}
	}
	if in.linked {
		in.link = newLinker(in)
		if err := in.link.fetch(ctx, q); err != nil {
			return err
		}
		for r := range in.live {
			if in.won[r] {
				if err := in.link.eval(r); err != nil {
					return err
				}
			}
		}
	}
	if err := in.store(ctx, q); err != nil {
		return err
	}
	if err := in.limit(ctx, q); err != nil {
		return err
	}
	in.settle()
	return nil
}

func (in *inserter) claim(ctx context.Context, q querier) error {
	var err error
	if in.holders, err = in.s.holding(ctx, q, in.now, in.keys); err != nil {
		return err
	}
	for r, i := range in.live {
		if key := in.jobs[i].UniqueKey; len(key) > 0 {
			_, taken := in.holders[string(key)]
			in.won[r] = !taken
		}
	}
	return nil
}

func holds(st driver.State) bool {
	return st.Live() && st != driver.Failed
}

func (s *Store) holding(ctx context.Context, q querier, now int64, keys [][]byte) (map[string]holder, error) {
	st, done, err := prepare(ctx, q, s.q.holder)
	if err != nil {
		return nil, err
	}
	defer done()
	holders := make(map[string]holder, len(keys))
	for _, key := range keys {
		var (
			h       holder
			expires sql.NullInt64
			state   string
		)
		switch err := st.QueryRowContext(ctx, key).Scan(&h.id, &expires, &state); {
		case errors.Is(err, sql.ErrNoRows):
			continue
		case err != nil:
			return nil, err
		}
		h.state = driver.State(state)
		if expires.Valid && expires.Int64 > now || !expires.Valid && holds(h.state) {
			holders[string(key)] = h
		}
	}
	return holders, nil
}

func (in *inserter) attach(ctx context.Context, q querier) error {
	counts := make(map[int64]int64)
	for r, i := range in.live {
		if id := in.jobs[i].BatchID; id != 0 && in.won[r] {
			counts[id]++
		}
	}
	if len(counts) == 0 {
		return nil
	}
	ids := slices.Sorted(maps.Keys(counts))
	open := make(map[int64]bool, len(ids))
	rows, err := q.QueryContext(ctx, in.s.q.openBatches, idList(ids))
	err = each(rows, err, func() error {
		var (
			id int64
			ok bool
		)
		if err := rows.Scan(&id, &ok); err != nil {
			return err
		}
		open[id] = ok
		return nil
	})
	if err != nil {
		return err
	}
	for _, id := range ids {
		ok, found := open[id]
		switch {
		case !found:
			return fmt.Errorf("%w: batch %d", driver.ErrNotFound, id)
		case !ok:
			return fmt.Errorf("%w: batch %d", driver.ErrClosed, id)
		}
	}
	for _, id := range ids {
		if _, err := q.ExecContext(ctx, in.s.q.attach, counts[id], id); err != nil {
			return err
		}
	}
	return nil
}

func (in *inserter) runAt(p *driver.InsertParams) int64 {
	if !p.RunAt.IsZero() {
		return p.RunAt.UnixMicro()
	}
	return in.now + max(micros(p.Delay), 0)
}

func (in *inserter) state(p *driver.InsertParams, pending int, runAt int64) driver.State {
	switch {
	case pending > 0:
		return driver.Awaiting
	case runAt > in.now:
		return driver.Scheduled
	case p.LimitKey != "":
		return driver.Throttled
	}
	return driver.Enqueued
}

func (in *inserter) store(ctx context.Context, q querier) error {
	var jobs, doomed, keys []any
	for r, i := range in.live {
		if !in.won[r] {
			continue
		}
		p := &in.jobs[i]
		var (
			pending int
			parents []byte
			reason  string
		)
		if l := in.link; l != nil {
			pending, parents, reason = l.pending[r], l.lists[r], l.reason[r]
		}
		id, runAt := in.ids[r], in.runAt(p)
		var key any
		if len(p.UniqueKey) > 0 && micros(p.UniqueFor) <= 0 {
			key = p.UniqueKey
		}
		if reason != "" {
			in.res[i] = driver.Inserted{ID: id, State: driver.Deleted}
			e := entry{state: driver.Deleted, reason: reason}
			doomed = append(doomed, id, string(driver.Deleted), p.Queue, p.Kind, p.Priority, 0, p.MaxAttempts, 0,
				millis(p.Timeout), runAt, in.now, in.now, optInt(p.BatchID), optInt(p.AfterBatch), text(parents),
				optString(p.RecurringID), key, optString(p.LimitKey), string(p.Args), text(encodeMeta(p.Meta)),
				text(encodeStrings(p.Tags)), e.encode(in.now))
		} else {
			st := in.state(p, pending, runAt)
			in.res[i] = driver.Inserted{ID: id, State: st}
			if st == driver.Enqueued {
				in.f.queue(p.Queue)
			}
			jobs = append(jobs, id, string(st), p.Queue, p.Kind, p.Priority, p.MaxAttempts, millis(p.Timeout), pending,
				runAt, in.now, optInt(p.BatchID), optInt(p.AfterBatch), text(parents), optString(p.RecurringID), key,
				optString(p.LimitKey), string(p.Args), text(encodeMeta(p.Meta)), text(encodeStrings(p.Tags)))
		}
		if us := micros(p.UniqueFor); len(p.UniqueKey) > 0 && (reason == "" || us > 0) {
			var expires any
			if us > 0 {
				expires = in.now + us
			}
			keys = append(keys, p.UniqueKey, id, expires)
		}
	}
	if err := insertRows(ctx, q, in.s.q.insertJobs, "", 19, jobs); err != nil {
		return err
	}
	if err := insertRows(ctx, q, in.s.q.insertDoomed, "", 22, doomed); err != nil {
		return err
	}
	if l := in.link; l != nil && len(l.deps) > 0 {
		deps := make([]any, 0, 5*len(l.deps))
		for _, d := range l.deps {
			deps = append(deps, d.batch, d.parent, d.job, int64(d.mask), d.resolved)
		}
		if err := insertRows(ctx, q, in.s.q.insertDeps, "", 5, deps); err != nil {
			return err
		}
	}
	return insertRows(ctx, q, in.s.q.hold, sqlHoldTail, 3, keys)
}

func (in *inserter) limit(ctx context.Context, q querier) error {
	var rules map[string]rule
	for r, i := range in.live {
		if p := &in.jobs[i]; in.won[r] && p.LimitKey != "" {
			if rules == nil {
				rules = make(map[string]rule)
			}
			rules[p.LimitKey] = ruleOf(p)
		}
	}
	if rules == nil {
		return nil
	}
	keys := slices.Sorted(maps.Keys(rules))
	if _, err := q.ExecContext(ctx, in.s.q.declare, declaration(rules, keys)); err != nil {
		return err
	}
	slots, err := in.s.slots(ctx, q, keys)
	if err != nil {
		return err
	}
	if err := in.s.fill(ctx, q, slots, in.f); err != nil {
		return err
	}
	for _, sl := range slots {
		in.moved(sl.admitted, driver.Enqueued)
		in.moved(sl.reserved, driver.Scheduled)
	}
	return nil
}

func (in *inserter) moved(ids []int64, st driver.State) {
	for _, id := range ids {
		r, ok := slices.BinarySearch(in.ids, id)
		if !ok {
			continue
		}
		if i := in.live[r]; in.res[i].State == driver.Throttled {
			in.res[i].State = st
		}
	}
}

func (in *inserter) settle() {
	for i, f := range in.first {
		if f == i && in.res[i].ID == 0 {
			h := in.holders[string(in.jobs[i].UniqueKey)]
			in.res[i] = driver.Inserted{ID: h.id, State: h.state, Duplicate: true}
		}
	}
	for i, f := range in.first {
		if f >= 0 && f != i {
			r := in.res[f]
			r.Duplicate = true
			in.res[i] = r
		}
	}
}

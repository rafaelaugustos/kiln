package mysqlstore

import (
	"bytes"
	"context"
	"maps"
	"slices"
	"strconv"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

const pushHistory = `IF(v.entry IS NULL, j.history, JSON_ARRAY_APPEND(IF(JSON_LENGTH(j.history) >= 16,
	JSON_REMOVE(j.history, '$[0]'), COALESCE(j.history, JSON_ARRAY())), '$', CAST(v.entry AS JSON)))`

const sqlArchive = `INSERT INTO {p}archive (id, state, queue, kind, priority, attempt, max_attempts, claim, timeout_ms,
	run_at, created_at, attempted_at, finalized_at, server, batch_id, after_batch, parents, recurring_id, unique_key,
	limit_key, args, meta, tags, history, output)
SELECT j.id, v.state, j.queue, j.kind, j.priority, v.attempt, j.max_attempts, j.claim, j.timeout_ms, j.run_at,
	j.created_at, j.attempted_at, ?, j.server, j.batch_id, j.after_batch, j.parents, j.recurring_id, j.unique_key,
	j.limit_key, j.args, j.meta, j.tags, ` + pushHistory + `, v.output
FROM (VALUES `

const sqlArchiveTail = `) AS v (id, state, attempt, entry, output) STRAIGHT_JOIN {p}jobs j FORCE INDEX (PRIMARY) ON j.id = v.id`

const sqlRemove = `DELETE FROM {p}jobs WHERE id IN (?)`

const sqlReleaseUniques = `DELETE FROM {p}uniques WHERE unique_key IN (?)`

const sqlOpenDeps = `SELECT parent_id, job_id, mask FROM {p}deps FORCE INDEX (PRIMARY)
WHERE batch = FALSE AND resolved = FALSE AND parent_id IN (?)?
ORDER BY parent_id, job_id LIMIT 5000 FOR UPDATE`

const sqlOpenBatchDeps = `SELECT parent_id, job_id, mask FROM {p}deps FORCE INDEX (PRIMARY)
WHERE batch = TRUE AND resolved = FALSE AND parent_id IN (?)
ORDER BY parent_id, job_id LIMIT 5000 FOR UPDATE`

const sqlLockChildren = `SELECT id, deps_pending, attempt, run_at, COALESCE(limit_key, ''), COALESCE(batch_id, 0)
FROM {p}jobs FORCE INDEX (PRIMARY) WHERE id IN (?) AND state = 'awaiting' ORDER BY id FOR UPDATE`

const sqlAdvance = `UPDATE (VALUES `

const sqlAdvanceTail = `) AS v (id, pending, state) STRAIGHT_JOIN {p}jobs j FORCE INDEX (PRIMARY) ON j.id = v.id
SET j.deps_pending = v.pending, j.state = v.state`

const sqlResolved = `) AS v (parent_id, job_id) STRAIGHT_JOIN {p}deps d FORCE INDEX (PRIMARY)
	ON d.batch = ? AND d.parent_id = v.parent_id AND d.job_id = v.job_id
SET d.resolved = TRUE`

const sqlCompleteBatches = `SELECT b.id FROM {p}batches b FORCE INDEX (PRIMARY)
WHERE b.id IN (?) AND b.sealed AND b.finished_at IS NULL AND NOT EXISTS (SELECT 1 FROM {p}jobs j WHERE j.batch_id = b.id)
ORDER BY b.id FOR UPDATE SKIP LOCKED`

const sqlFinishBatches = `UPDATE {p}batches SET finished_at = ? WHERE id IN (?)`

const sqlCount = `INSERT INTO {p}stats (bucket, server, succeeded, failed, deleted, retried) VALUES (?, ?, ?, ?, ?, ?) AS n
ON DUPLICATE KEY UPDATE succeeded = {p}stats.succeeded + n.succeeded, failed = {p}stats.failed + n.failed,
	deleted = {p}stats.deleted + n.deleted, retried = {p}stats.retried + n.retried`

type parent struct {
	id    int64
	state driver.State
	label string
}

type tally struct {
	succeeded, failed, deleted, retried int
}

type fallout struct {
	now      time.Time
	server   string
	keys     [][]byte
	release  map[string]int
	parents  []parent
	batches  []int64
	throttle []string
	stats    tally
	changed  int
}

func (f *fallout) free(key string) {
	if f.release == nil {
		f.release = make(map[string]int)
	}
	f.release[key]++
}

func (f *fallout) batch(id int64) {
	if id != 0 && !slices.Contains(f.batches, id) {
		f.batches = append(f.batches, id)
	}
}

func (f *fallout) throttled(key string) {
	if !slices.Contains(f.throttle, key) {
		f.throttle = append(f.throttle, key)
	}
}

func (s *Store) settle(ctx context.Context, q querier, f *fallout) error {
	if len(f.keys) > 0 {
		slices.SortFunc(f.keys, bytes.Compare)
		if _, err := q.ExecContext(ctx, render(s.q.releaseUniques, f.keys)); err != nil {
			return err
		}
	}
	var slots map[string]*slot
	if len(f.release) > 0 {
		var err error
		if slots, err = s.lockLimits(ctx, q, slices.Sorted(maps.Keys(f.release)), false); err != nil {
			return err
		}
		for key, n := range f.release {
			if sl := slots[key]; sl != nil {
				sl.active = max(sl.active-n, 0)
				sl.dirty = true
			}
		}
	}
	if len(f.parents) > 0 {
		if err := s.resolve(ctx, q, f); err != nil {
			return err
		}
	}
	if len(f.batches) > 0 {
		if err := s.complete(ctx, q, f); err != nil {
			return err
		}
	}
	var extra []string
	for _, key := range f.throttle {
		if _, ok := slots[key]; !ok {
			extra = append(extra, key)
		}
	}
	if len(extra) > 0 {
		slices.Sort(extra)
		more, err := s.lockLimits(ctx, q, extra, true)
		if err != nil {
			return err
		}
		if slots == nil {
			slots = more
		} else {
			maps.Copy(slots, more)
		}
	}
	if len(slots) > 0 {
		a, err := s.fill(ctx, q, slots)
		if err != nil {
			return err
		}
		f.changed += len(a.ids)
	}
	if t := f.stats; t != (tally{}) {
		bucket := f.now.Truncate(time.Minute)
		stmt := render(s.q.count, bucket, f.server, t.succeeded, t.failed, t.deleted, t.retried)
		if _, err := q.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

type link struct {
	parent int64
	child  int64
	mask   driver.Mask
}

func (s *Store) openLinks(ctx context.Context, q querier, stmt string) ([]link, error) {
	rows, err := q.QueryContext(ctx, stmt)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []link
	for rows.Next() {
		var l link
		if err := rows.Scan(&l.parent, &l.child, &l.mask); err != nil {
			return nil, err
		}
		out = append(out, l)
	}
	return out, rows.Err()
}

func (s *Store) markResolved(ctx context.Context, q querier, batch bool, links []link) error {
	b := make([]byte, 0, 160+32*len(links))
	b = append(b, "UPDATE (VALUES "...)
	for i, l := range links {
		if i > 0 {
			b = append(b, ',')
		}
		b = appendSQL(b, "ROW(?, ?)", l.parent, l.child)
	}
	_, err := q.ExecContext(ctx, string(appendSQL(b, s.q.resolved, batch)))
	return err
}

func (s *Store) resolve(ctx context.Context, q querier, f *fallout) error {
	byID := make(map[int64]parent, len(f.parents))
	var ids, failed []int64
	for _, p := range f.parents {
		if _, ok := byID[p.id]; ok {
			continue
		}
		byID[p.id] = p
		ids = append(ids, p.id)
		if p.state == driver.Failed {
			failed = append(failed, p.id)
		}
	}
	slices.Sort(ids)
	var only raw
	if len(failed) > 0 {
		only = raw(render(" AND (mask & 2 <> 0 OR parent_id NOT IN (?))", failed))
	}
	links, err := s.openLinks(ctx, q, render(s.q.openDeps, ids, only))
	if err != nil || len(links) == 0 {
		return err
	}
	if err := s.markResolved(ctx, q, false, links); err != nil {
		return err
	}
	ok := make(map[int64]int)
	doom := make(map[int64]string)
	for _, l := range links {
		p := byID[l.parent]
		if l.mask.Has(p.state) {
			ok[l.child]++
			continue
		}
		if _, set := doom[l.child]; !set {
			doom[l.child] = "parent " + strconv.FormatInt(p.id, 10) + " " + p.label
		}
	}
	return s.advance(ctx, q, f, childIDs(links), ok, doom)
}

func childIDs(links []link) []int64 {
	ids := make([]int64, len(links))
	for i, l := range links {
		ids[i] = l.child
	}
	slices.Sort(ids)
	return slices.Compact(ids)
}

func (s *Store) advance(ctx context.Context, q querier, f *fallout, children []int64, ok map[int64]int, doom map[int64]string) error {
	rows, err := q.QueryContext(ctx, render(s.q.lockChildren, children))
	if err != nil {
		return err
	}
	type child struct {
		id      int64
		pending int
		attempt int
		runAt   stamp
		limit   string
		batch   int64
	}
	var locked []child
	for rows.Next() {
		var c child
		if err := rows.Scan(&c.id, &c.pending, &c.attempt, &c.runAt, &c.limit, &c.batch); err != nil {
			rows.Close()
			return err
		}
		locked = append(locked, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	var (
		doomed []int64
		arch   []byte
		upd    []byte
	)
	for _, c := range locked {
		if reason, bad := doom[c.id]; bad {
			e := entry{state: driver.Deleted, attempt: c.attempt, reason: reason}
			if arch == nil {
				arch = appendSQL(arch, s.q.archive, f.now)
			} else {
				arch = append(arch, ',')
			}
			arch = appendSQL(arch, "ROW(?, 'deleted', ?, ?, NULL)", c.id, c.attempt, e.encode(f.now))
			doomed = append(doomed, c.id)
			f.batch(c.batch)
			continue
		}
		pending := max(c.pending-ok[c.id], 0)
		st := driver.Awaiting
		switch {
		case pending > 0:
		case c.runAt.After(f.now):
			st = driver.Scheduled
		case c.limit != "":
			st = driver.Throttled
			f.throttled(c.limit)
		default:
			st = driver.Enqueued
		}
		if upd == nil {
			upd = append(upd, s.q.advance...)
		} else {
			upd = append(upd, ',')
		}
		upd = appendSQL(upd, "ROW(?, ?, ?)", c.id, pending, st)
		f.changed++
	}
	if arch != nil {
		arch = append(arch, s.q.archiveTail...)
		if _, err := q.ExecContext(ctx, string(arch)); err != nil {
			return err
		}
		if _, err := q.ExecContext(ctx, render(s.q.remove, doomed)); err != nil {
			return err
		}
		f.stats.deleted += len(doomed)
		f.changed += len(doomed)
	}
	if upd != nil {
		upd = append(upd, s.q.advanceTail...)
		if _, err := q.ExecContext(ctx, string(upd)); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) complete(ctx context.Context, q querier, f *fallout) error {
	ids := slices.Sorted(slices.Values(f.batches))
	f.batches = nil
	rows, err := q.QueryContext(ctx, render(s.q.completeBatches, ids))
	if err != nil {
		return err
	}
	var done []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		done = append(done, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil || len(done) == 0 {
		return err
	}
	if _, err := q.ExecContext(ctx, render(s.q.finishBatches, f.now, done)); err != nil {
		return err
	}
	f.changed += len(done)
	return s.releaseBatches(ctx, q, f, done)
}

func (s *Store) releaseBatches(ctx context.Context, q querier, f *fallout, ids []int64) error {
	links, err := s.openLinks(ctx, q, render(s.q.openBatchDeps, ids))
	if err != nil || len(links) == 0 {
		return err
	}
	if err := s.markResolved(ctx, q, true, links); err != nil {
		return err
	}
	ok := make(map[int64]int, len(links))
	for _, l := range links {
		ok[l.child]++
	}
	return s.advance(ctx, q, f, childIDs(links), ok, nil)
}

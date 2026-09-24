package sqlitestore

import (
	"context"
	"slices"
	"strconv"

	"github.com/rafaelaugustos/kiln/driver"
)

const fanout = 5000

const pushHistory = `CASE WHEN ? IS NULL THEN j.history ELSE json_insert(CASE WHEN json_array_length(j.history) >= 16
	THEN json_remove(j.history, '$[0]') ELSE COALESCE(j.history, '[]') END, '$[#]', json(?)) END`

const archiveColumns = `INSERT INTO {p}archive (id, state, queue, kind, priority, attempt, max_attempts, claim,
	timeout_ms, run_at, created_at, attempted_at, finalized_at, server, batch_id, after_batch, parents, recurring_id,
	unique_key, limit_key, args, meta, tags, history, output)`

const sqlArchive = archiveColumns + `
SELECT j.id, ?, j.queue, j.kind, j.priority, ?, j.max_attempts, j.claim, j.timeout_ms, j.run_at, j.created_at,
	j.attempted_at, ?, j.server, j.batch_id, j.after_batch, j.parents, j.recurring_id, j.unique_key, j.limit_key,
	j.args, j.meta, j.tags, ` + pushHistory + `, ?
FROM {p}jobs j WHERE j.id = ?`

const sqlArchiveSucceeded = archiveColumns + `
SELECT j.id, 'succeeded', j.queue, j.kind, j.priority, j.attempt, j.max_attempts, j.claim, j.timeout_ms, j.run_at,
	j.created_at, j.attempted_at, ?, j.server, j.batch_id, j.after_batch, j.parents, j.recurring_id, j.unique_key,
	j.limit_key, j.args, j.meta, j.tags, j.history, NULL
FROM {p}jobs j WHERE j.id IN (SELECT value FROM json_each(?))`

const sqlRemove = `DELETE FROM {p}jobs WHERE id IN (SELECT value FROM json_each(?))`

const sqlRelease = `DELETE FROM {p}uniques WHERE unique_key = ? AND job_id = ?`

const sqlResolve = `UPDATE {p}deps SET resolved = 1
WHERE batch = 0 AND (parent_id, job_id) IN (
	SELECT parent_id, job_id FROM {p}deps
	WHERE batch = 0 AND resolved = 0 AND parent_id IN (SELECT value FROM json_each(?))
		AND (mask & 2 <> 0 OR parent_id NOT IN (SELECT value FROM json_each(?)))
	ORDER BY parent_id, job_id
	LIMIT ?)
RETURNING parent_id, job_id, mask`

const sqlReleaseBatches = `UPDATE {p}deps SET resolved = 1
WHERE batch = 1 AND (parent_id, job_id) IN (
	SELECT parent_id, job_id FROM {p}deps
	WHERE batch = 1 AND resolved = 0 AND parent_id IN (SELECT value FROM json_each(?))
	ORDER BY parent_id, job_id
	LIMIT ?)
RETURNING parent_id, job_id, mask`

const sqlChildren = `SELECT j.id, j.deps_pending, j.attempt, j.run_at, j.queue, COALESCE(j.limit_key, ''),
	COALESCE(j.batch_id, 0), j.unique_key,
	EXISTS (SELECT 1 FROM {p}deps d WHERE d.batch = 0 AND d.parent_id = j.id AND d.resolved = 0)
FROM {p}jobs j
WHERE j.id IN (SELECT value FROM json_each(?)) AND j.state = 'awaiting'`

const sqlAdvance = `UPDATE {p}jobs SET deps_pending = ?, state = ? WHERE id = ?`

const sqlComplete = `UPDATE {p}batches AS b SET finished_at = ?
WHERE b.id IN (SELECT value FROM json_each(?)) AND b.sealed AND b.finished_at IS NULL
	AND NOT EXISTS (SELECT 1 FROM {p}jobs j WHERE j.batch_id = b.id)
RETURNING id`

const sqlCount = `INSERT INTO {p}stats (bucket, server, succeeded, failed, deleted, retried) VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT (bucket, server) DO UPDATE SET succeeded = succeeded + excluded.succeeded,
	failed = failed + excluded.failed, deleted = deleted + excluded.deleted, retried = retried + excluded.retried`

type parent struct {
	id    int64
	state driver.State
	label string
}

type held struct {
	key []byte
	id  int64
}

type tally struct {
	succeeded, failed, deleted, retried int
}

type fallout struct {
	now      int64
	server   string
	keys     []held
	release  map[string]int
	parents  []parent
	batches  []int64
	throttle []string
	queues   []string
	admitted []int64
	stats    tally
	changed  int
	links    int
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

func (f *fallout) queue(name string) {
	if !slices.Contains(f.queues, name) {
		f.queues = append(f.queues, name)
	}
}

func (f *fallout) hold(key []byte, id int64) {
	if key != nil {
		f.keys = append(f.keys, held{key, id})
	}
}

type archiver struct {
	now       int64
	rows      []any
	succeeded []int64
	ids       []int64
}

func (a *archiver) add(id int64, st driver.State, attempt int, e any, output []byte) {
	var out any
	if len(output) > 0 {
		out = string(output)
	}
	a.rows = append(a.rows, string(st), attempt, a.now, e, e, out, id)
	a.ids = append(a.ids, id)
}

func (a *archiver) succeed(id int64) {
	a.succeeded = append(a.succeeded, id)
	a.ids = append(a.ids, id)
}

func (s *Store) archive(ctx context.Context, q querier, a *archiver) error {
	if len(a.ids) == 0 {
		return nil
	}
	if len(a.succeeded) > 0 {
		if _, err := q.ExecContext(ctx, s.q.archiveSucceeded, a.now, idList(a.succeeded)); err != nil {
			return err
		}
	}
	if err := execEach(ctx, q, s.q.archive, 7, a.rows); err != nil {
		return err
	}
	_, err := q.ExecContext(ctx, s.q.remove, idList(a.ids))
	return err
}

func (s *Store) settle(ctx context.Context, q querier, f *fallout) error {
	for f.links < fanout && (len(f.parents) > 0 || len(f.batches) > 0) {
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
	}
	if len(f.keys) > 0 {
		rows := make([]any, 0, 2*len(f.keys))
		for _, k := range f.keys {
			rows = append(rows, k.key, k.id)
		}
		if err := execEach(ctx, q, s.q.release, 2, rows); err != nil {
			return err
		}
	}
	keys := slices.Clone(f.throttle)
	for key := range f.release {
		keys = append(keys, key)
	}
	if len(keys) > 0 {
		slices.Sort(keys)
		slots, err := s.slots(ctx, q, slices.Compact(keys))
		if err != nil {
			return err
		}
		for key, n := range f.release {
			if sl := slots[key]; sl != nil {
				sl.active, sl.dirty = max(sl.active-n, 0), true
			}
		}
		if err := s.fill(ctx, q, slots, f); err != nil {
			return err
		}
	}
	if t := f.stats; t != (tally{}) {
		bucket := f.now - f.now%60_000_000
		if _, err := q.ExecContext(ctx, s.q.count, bucket, f.server, t.succeeded, t.failed, t.deleted, t.retried); err != nil {
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

func (s *Store) links(ctx context.Context, q querier, stmt string, args ...any) ([]link, error) {
	var out []link
	rows, err := q.QueryContext(ctx, stmt, args...)
	err = each(rows, err, func() error {
		var l link
		if err := rows.Scan(&l.parent, &l.child, &l.mask); err != nil {
			return err
		}
		out = append(out, l)
		return nil
	})
	return out, err
}

func childIDs(links []link) []int64 {
	ids := make([]int64, len(links))
	for i, l := range links {
		ids[i] = l.child
	}
	slices.Sort(ids)
	return slices.Compact(ids)
}

func (s *Store) resolve(ctx context.Context, q querier, f *fallout) error {
	parents := f.parents
	f.parents = nil
	byID := make(map[int64]parent, len(parents))
	var ids, failed []int64
	for _, p := range parents {
		if _, ok := byID[p.id]; ok {
			continue
		}
		byID[p.id] = p
		ids = append(ids, p.id)
		if p.state == driver.Failed {
			failed = append(failed, p.id)
		}
	}
	links, err := s.links(ctx, q, s.q.resolve, idList(ids), idList(failed), max(fanout-f.links, 0))
	if err != nil || len(links) == 0 {
		return err
	}
	f.links += len(links)
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

func (s *Store) advance(ctx context.Context, q querier, f *fallout, children []int64, ok map[int64]int, doom map[int64]string) error {
	type child struct {
		id       int64
		pending  int
		attempt  int
		runAt    int64
		queue    string
		limit    string
		batch    int64
		key      []byte
		children bool
	}
	var found []child
	rows, err := q.QueryContext(ctx, s.q.children, idList(children))
	err = each(rows, err, func() error {
		var c child
		if err := rows.Scan(&c.id, &c.pending, &c.attempt, &c.runAt, &c.queue, &c.limit, &c.batch, &c.key, &c.children); err != nil {
			return err
		}
		found = append(found, c)
		return nil
	})
	if err != nil {
		return err
	}
	var upd []any
	gone := archiver{now: f.now}
	for _, c := range found {
		if reason, bad := doom[c.id]; bad {
			e := entry{state: driver.Deleted, attempt: c.attempt, reason: reason}
			gone.add(c.id, driver.Deleted, c.attempt, e.encode(f.now), nil)
			f.batch(c.batch)
			f.hold(c.key, c.id)
			if c.children {
				f.parents = append(f.parents, parent{id: c.id, state: driver.Deleted, label: string(driver.Deleted)})
			}
			continue
		}
		pending := max(c.pending-ok[c.id], 0)
		st := driver.Awaiting
		switch {
		case pending > 0:
		case c.runAt > f.now:
			st = driver.Scheduled
		case c.limit != "":
			st = driver.Throttled
			f.throttled(c.limit)
		default:
			st = driver.Enqueued
			f.queue(c.queue)
		}
		upd = append(upd, pending, string(st), c.id)
		f.changed++
	}
	if len(gone.ids) > 0 {
		if err := s.archive(ctx, q, &gone); err != nil {
			return err
		}
		f.stats.deleted += len(gone.ids)
		f.changed += len(gone.ids)
	}
	return execEach(ctx, q, s.q.advance, 3, upd)
}

func (s *Store) complete(ctx context.Context, q querier, f *fallout) error {
	ids := f.batches
	f.batches = nil
	var done []int64
	rows, err := q.QueryContext(ctx, s.q.complete, f.now, idList(ids))
	err = each(rows, err, func() error {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return err
		}
		done = append(done, id)
		return nil
	})
	if err != nil || len(done) == 0 {
		return err
	}
	f.changed += len(done)
	return s.releaseBatches(ctx, q, f, done)
}

func (s *Store) releaseBatches(ctx context.Context, q querier, f *fallout, ids []int64) error {
	links, err := s.links(ctx, q, s.q.releaseBatches, idList(ids), max(fanout-f.links, 0))
	if err != nil || len(links) == 0 {
		return err
	}
	f.links += len(links)
	ok := make(map[int64]int, len(links))
	for _, l := range links {
		ok[l.child]++
	}
	return s.advance(ctx, q, f, childIDs(links), ok, nil)
}

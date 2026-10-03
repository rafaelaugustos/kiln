package mssqlstore

import (
	"bytes"
	"context"
	"database/sql"
	"maps"
	"slices"
	"strconv"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

const pushHistory = `CASE WHEN v.entry IS NULL THEN j.history
	WHEN j.history IS NULL THEN N'[' + v.entry + N']'
	WHEN (SELECT COUNT(*) FROM OPENJSON(j.history)) < 16 THEN JSON_MODIFY(j.history, 'append $', JSON_QUERY(v.entry))
	ELSE (SELECT N'[' + STRING_AGG(h.value, N',') WITHIN GROUP (ORDER BY CAST(h.[key] AS INT)) + N',' + v.entry + N']'
		FROM OPENJSON(j.history) h WHERE CAST(h.[key] AS INT) > (SELECT COUNT(*) FROM OPENJSON(j.history)) - 16)
END`

const sqlArchive = `INSERT INTO {p}archive (id, state, queue, kind, priority, attempt, max_attempts, claim, timeout_ms,
	run_at, created_at, attempted_at, finalized_at, server, batch_id, after_batch, parents, recurring_id, unique_key,
	limit_key, args, meta, tags, history, [output], progress)
SELECT j.id, v.state, j.queue, j.kind, j.priority, v.attempt, j.max_attempts, j.claim, j.timeout_ms, j.run_at,
	j.created_at, j.attempted_at, @now, j.server, j.batch_id, j.after_batch, j.parents, j.recurring_id, j.unique_key,
	j.limit_key, j.args, j.meta, j.tags, ` + pushHistory + `, v.[output], j.progress
FROM OPENJSON(@gone) WITH (id BIGINT '$.id', state VARCHAR(10) '$.s', attempt INT '$.a',
	entry NVARCHAR(MAX) '$.e' AS JSON, [output] NVARCHAR(MAX) '$.o') v
JOIN {p}jobs j WITH (FORCESEEK) ON j.id = v.id;
DELETE j FROM OPENJSON(@gone) WITH (id BIGINT '$.id') v JOIN {p}jobs j WITH (FORCESEEK) ON j.id = v.id;
`

const sqlReleaseUniques = `DELETE u FROM OPENJSON(@keys) WITH (k VARCHAR(130) '$.k', id BIGINT '$.id') v
JOIN {p}uniques u WITH (READPAST, ROWLOCK, FORCESEEK) ON u.unique_key = CONVERT(VARBINARY(64), v.k, 2) AND u.job_id = v.id;
`

const sqlResolve = `UPDATE d SET resolved = 1
OUTPUT inserted.parent_id, inserted.job_id, inserted.mask
FROM {p}deps d JOIN (
	SELECT TOP (5000) x.batch, x.parent_id, x.job_id
	FROM OPENJSON(@parents) WITH (id BIGINT '$.id', failed BIT '$.f') p
	JOIN {p}deps x WITH (UPDLOCK, ROWLOCK, FORCESEEK) ON x.batch = @batch AND x.parent_id = p.id
	WHERE x.resolved = 0 AND (p.failed IS NULL OR x.mask & 2 <> 0)
	ORDER BY x.parent_id, x.job_id
) t ON d.batch = t.batch AND d.parent_id = t.parent_id AND d.job_id = t.job_id`

const sqlLockChildren = `SELECT j.id, j.deps_pending, j.attempt, j.run_at, j.queue, COALESCE(j.limit_key, N''),
	COALESCE(j.batch_id, 0)
FROM OPENJSON(@ids) WITH (id BIGINT '$') v
JOIN {p}jobs j WITH (UPDLOCK, ROWLOCK, FORCESEEK) ON j.id = v.id
WHERE j.state = 'awaiting'
ORDER BY j.id`

const sqlAdvance = `UPDATE j SET deps_pending = v.pending, state = v.state
FROM OPENJSON(@advance) WITH (id BIGINT '$.id', pending INT '$.n', state VARCHAR(10) '$.s') v
JOIN {p}jobs j WITH (FORCESEEK) ON j.id = v.id;
`

const sqlCompleteBatches = `UPDATE b SET finished_at = @now
OUTPUT inserted.id
FROM OPENJSON(@ids) WITH (id BIGINT '$') v
JOIN {p}batches b WITH (READPAST, ROWLOCK, FORCESEEK) ON b.id = v.id
WHERE b.sealed = 1 AND b.finished_at IS NULL AND NOT EXISTS (SELECT 1 FROM {p}jobs j WHERE j.batch_id = b.id)`

const sqlCount = `MERGE {p}stats WITH (HOLDLOCK) AS s
USING (SELECT @bucket AS bucket, @server COLLATE Latin1_General_100_BIN2 AS server) AS n
ON s.bucket = n.bucket AND s.server = n.server
WHEN MATCHED THEN UPDATE SET succeeded = s.succeeded + @succeeded, failed = s.failed + @failed,
	deleted = s.deleted + @deleted, retried = s.retried + @retried
WHEN NOT MATCHED THEN INSERT (bucket, server, succeeded, failed, deleted, retried)
	VALUES (n.bucket, n.server, @succeeded, @failed, @deleted, @retried);`

type parent struct {
	id    int64
	state driver.State
	label string
}

type tally struct {
	succeeded, failed, deleted, retried int
}

type heldKey struct {
	key []byte
	id  int64
}

type fallout struct {
	now      time.Time
	server   string
	keys     []heldKey
	release  map[string]int
	parents  []parent
	batches  []int64
	throttle []string
	queues   []string
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

func (f *fallout) queue(names ...string) {
	f.queues = merge(f.queues, names...)
}

func (f *fallout) hold(key []byte, id int64) {
	if key != nil {
		f.keys = append(f.keys, heldKey{key: key, id: id})
	}
}

func (f *fallout) keyList() string {
	slices.SortFunc(f.keys, func(a, b heldKey) int { return bytes.Compare(a.key, b.key) })
	var t table
	for _, k := range f.keys {
		t.row()
		t.bin("k", k.key)
		t.int("id", k.id)
	}
	return t.String()
}

func (s *Store) settle(ctx context.Context, q querier, f *fallout) error {
	if len(f.keys) > 0 {
		if _, err := q.ExecContext(ctx, s.q.releaseUniques, sql.Named("keys", f.keyList())); err != nil {
			return err
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
		f.changed += a.changed()
		f.queue(a.queues...)
	}
	if t := f.stats; t != (tally{}) {
		_, err := q.ExecContext(ctx, s.q.count, sql.Named("bucket", stamp(f.now.Truncate(time.Minute))),
			sql.Named("server", f.server), sql.Named("succeeded", t.succeeded), sql.Named("failed", t.failed),
			sql.Named("deleted", t.deleted), sql.Named("retried", t.retried))
		if err != nil {
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

func (s *Store) openLinks(ctx context.Context, q querier, batch bool, parents string) ([]link, error) {
	rows, err := q.QueryContext(ctx, s.q.resolve, sql.Named("batch", batch), sql.Named("parents", parents))
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

func (s *Store) resolve(ctx context.Context, q querier, f *fallout) error {
	byID := make(map[int64]parent, len(f.parents))
	var ids []int64
	for _, p := range f.parents {
		if _, ok := byID[p.id]; ok {
			continue
		}
		byID[p.id] = p
		ids = append(ids, p.id)
	}
	slices.Sort(ids)
	var t table
	for _, id := range ids {
		t.row()
		t.int("id", id)
		t.flag("f", byID[id].state == driver.Failed)
	}
	links, err := s.openLinks(ctx, q, false, t.String())
	if err != nil || len(links) == 0 {
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
	rows, err := q.QueryContext(ctx, s.q.lockChildren, sql.Named("ids", idList(children)))
	if err != nil {
		return err
	}
	type child struct {
		id      int64
		pending int
		attempt int
		runAt   moment
		queue   string
		limit   string
		batch   int64
	}
	var locked []child
	for rows.Next() {
		var c child
		if err := rows.Scan(&c.id, &c.pending, &c.attempt, &c.runAt, &c.queue, &c.limit, &c.batch); err != nil {
			rows.Close()
			return err
		}
		locked = append(locked, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	var gone, moved table
	for _, c := range locked {
		if reason, bad := doom[c.id]; bad {
			e := entry{state: driver.Deleted, attempt: c.attempt, reason: reason}
			gone.row()
			gone.int("id", c.id)
			gone.str("s", string(driver.Deleted))
			gone.int("a", int64(c.attempt))
			gone.json("e", e.encode(f.now))
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
			f.queue(c.queue)
		}
		moved.row()
		moved.int("id", c.id)
		moved.int("n", int64(pending))
		moved.str("s", string(st))
		f.changed++
	}
	var stmt string
	if gone.n > 0 {
		stmt = s.q.archive
		f.stats.deleted += gone.n
		f.changed += gone.n
	}
	if moved.n > 0 {
		stmt += s.q.advance
	}
	if stmt == "" {
		return nil
	}
	_, err = q.ExecContext(ctx, stmt, sql.Named("now", stamp(f.now)), sql.Named("gone", gone.String()),
		sql.Named("advance", moved.String()))
	return err
}

func (s *Store) complete(ctx context.Context, q querier, f *fallout) error {
	ids := slices.Sorted(slices.Values(f.batches))
	f.batches = nil
	rows, err := q.QueryContext(ctx, s.q.completeBatches, sql.Named("now", stamp(f.now)), sql.Named("ids", idList(ids)))
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
	slices.Sort(done)
	f.changed += len(done)
	return s.releaseBatches(ctx, q, f, done)
}

func (s *Store) releaseBatches(ctx context.Context, q querier, f *fallout, ids []int64) error {
	var t table
	for _, id := range ids {
		t.row()
		t.int("id", id)
	}
	links, err := s.openLinks(ctx, q, true, t.String())
	if err != nil || len(links) == 0 {
		return err
	}
	ok := make(map[int64]int, len(links))
	for _, l := range links {
		ok[l.child]++
	}
	return s.advance(ctx, q, f, childIDs(links), ok, nil)
}

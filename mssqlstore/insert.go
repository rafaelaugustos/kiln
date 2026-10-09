package mssqlstore

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

const maxTitle = 200

const sqlAllocate = `DECLARE @f SQL_VARIANT;
EXEC sp_sequence_get_range @sequence_name = N'{p}job_ids', @range_size = @n, @range_first_value = @f OUTPUT;
SELECT CAST(@f AS BIGINT), CAST(SYSUTCDATETIME() AS DATETIME2(6))`

const jobValues = `
FROM OPENJSON(@jobs) WITH (
	i BIGINT '$.i', queue NVARCHAR(MAX) '$.q', kind NVARCHAR(MAX) '$.k', priority SMALLINT '$.p',
	max_attempts INT '$.m', timeout_ms BIGINT '$.t', pending INT '$.d', run_at DATETIME2(6) '$.r', delay BIGINT '$.y',
	batch_id BIGINT '$.b', after_batch BIGINT '$.a', parents NVARCHAR(MAX) '$.pa' AS JSON,
	recurring_id NVARCHAR(MAX) '$.rc', unique_key VARCHAR(130) '$.u', limit_key NVARCHAR(MAX) '$.l',
	args NVARCHAR(MAX) '$.args', meta NVARCHAR(MAX) '$.meta' AS JSON, tags NVARCHAR(MAX) '$.tags' AS JSON,
	title NVARCHAR(200) '$.ti', doomed BIT '$.x', history NVARCHAR(MAX) '$.h' AS JSON
) v
CROSS APPLY (SELECT COALESCE(v.run_at, DATEADD(MICROSECOND, ISNULL(v.delay, 0) % 1000000,
	DATEADD(SECOND, ISNULL(v.delay, 0) / 1000000, @now))) AS run_at) r`

const sqlInsertJobs = `INSERT INTO {p}jobs (id, state, queue, kind, priority, max_attempts, timeout_ms, deps_pending,
	run_at, created_at, batch_id, after_batch, parents, recurring_id, unique_key, limit_key, args, meta, tags, title)
OUTPUT inserted.id, inserted.state
SELECT @base + v.i, CASE WHEN ISNULL(v.pending, 0) > 0 THEN 'awaiting' WHEN r.run_at > @now THEN 'scheduled'
		WHEN v.limit_key IS NOT NULL THEN 'throttled' ELSE 'enqueued' END,
	v.queue, v.kind, v.priority, v.max_attempts, v.timeout_ms, ISNULL(v.pending, 0), r.run_at, @now, v.batch_id,
	v.after_batch, v.parents, v.recurring_id, CONVERT(VARBINARY(64), v.unique_key, 2), v.limit_key, v.args, v.meta,
	v.tags, v.title` + jobValues + `
WHERE v.doomed IS NULL`

const sqlInsertPlain = `DECLARE @f SQL_VARIANT, @base BIGINT, @now DATETIME2(6) = SYSUTCDATETIME();
EXEC sp_sequence_get_range @sequence_name = N'{p}job_ids', @range_size = @n, @range_first_value = @f OUTPUT;
SET @base = CAST(@f AS BIGINT);
` + sqlInsertJobs

const sqlInsertDoomed = `INSERT INTO {p}archive (id, state, queue, kind, priority, attempt, max_attempts, claim, timeout_ms,
	run_at, created_at, finalized_at, batch_id, after_batch, parents, recurring_id, limit_key, args, meta, tags, title,
	history)
SELECT @base + v.i, 'deleted', v.queue, v.kind, v.priority, 0, v.max_attempts, 0, v.timeout_ms, r.run_at, @now, @now,
	v.batch_id, v.after_batch, v.parents, v.recurring_id, v.limit_key, v.args, v.meta, v.tags, v.title,
	v.history` + jobValues + `
WHERE v.doomed = 1;
`

const sqlInsertDeps = `INSERT INTO {p}deps (batch, parent_id, job_id, mask, resolved)
SELECT ISNULL(v.batch, 0), v.parent_id, v.job_id, v.mask, ISNULL(v.resolved, 0)
FROM OPENJSON(@deps) WITH (batch BIT '$.b', parent_id BIGINT '$.p', job_id BIGINT '$.j', mask SMALLINT '$.m',
	resolved BIT '$.r') v;
`

const window = `CASE WHEN v.us > 0 THEN DATEADD(MICROSECOND, v.us % 1000000, DATEADD(SECOND, v.us / 1000000, SYSUTCDATETIME())) END`

const sqlClaimUniques = `UPDATE u SET job_id = v.id, expires_at = ` + window + `
FROM (
	SELECT CONVERT(VARBINARY(64), v.k, 2) AS k, v.id, v.us, o.job_id AS seen, CASE WHEN h.id IS NULL THEN 1 ELSE 0 END AS gone
	FROM OPENJSON(@claims) WITH (k VARCHAR(130) '$.k', id BIGINT '$.id', us BIGINT '$.us') v
	LEFT JOIN {p}uniques o ON o.unique_key = CONVERT(VARBINARY(64), v.k, 2)
	LEFT JOIN {p}jobs h ON h.id = o.job_id AND h.state <> 'failed'
) AS v
JOIN {p}uniques u WITH (UPDLOCK, ROWLOCK, FORCESEEK) ON u.unique_key = v.k
WHERE u.job_id = v.seen AND (u.expires_at <= SYSUTCDATETIME() OR u.expires_at IS NULL AND v.gone = 1);
INSERT INTO {p}uniques (unique_key, job_id, expires_at)
SELECT CONVERT(VARBINARY(64), v.k, 2), v.id, ` + window + `
FROM OPENJSON(@claims) WITH (k VARCHAR(130) '$.k', id BIGINT '$.id', us BIGINT '$.us') v
WHERE NOT EXISTS (SELECT 1 FROM {p}uniques u WHERE u.unique_key = CONVERT(VARBINARY(64), v.k, 2));
SELECT u.unique_key, u.job_id, COALESCE(j.state, a.state, ''), COALESCE(j.granted, 0)
FROM OPENJSON(@claims) WITH (k VARCHAR(130) '$.k') v
JOIN {p}uniques u ON u.unique_key = CONVERT(VARBINARY(64), v.k, 2)
LEFT JOIN {p}jobs j ON j.id = u.job_id
LEFT JOIN {p}archive a ON a.id = u.job_id`

const sqlReplace = `UPDATE j SET args = v.args, meta = v.meta, tags = v.tags, title = v.title, priority = v.priority,
	state = CASE WHEN j.granted = 0 AND v.run_at > @now
		AND (j.state = 'throttled' OR j.state = 'enqueued' AND j.limit_key IS NULL) THEN 'scheduled' ELSE j.state END,
	run_at = CASE WHEN j.granted = 1 OR j.state = 'enqueued' AND j.limit_key IS NOT NULL THEN j.run_at
		WHEN j.state IN ('awaiting', 'scheduled') OR v.run_at > @now THEN v.run_at ELSE j.run_at END
OUTPUT inserted.id, inserted.state
FROM OPENJSON(@jobs) WITH (id BIGINT '$.id', debounce BIT '$.d', args NVARCHAR(MAX) '$.args',
	meta NVARCHAR(MAX) '$.meta' AS JSON, tags NVARCHAR(MAX) '$.tags' AS JSON, title NVARCHAR(200) '$.ti',
	priority SMALLINT '$.p', run_at DATETIME2(6) '$.r') v
JOIN {p}jobs j WITH (FORCESEEK) ON j.id = v.id
WHERE v.debounce = 1 AND j.state = 'scheduled' AND j.granted = 0
	OR v.debounce IS NULL AND j.state IN ('awaiting', 'scheduled', 'throttled', 'enqueued')`

const sqlLockBatches = `SELECT b.id, CASE WHEN b.finished_at IS NULL AND (b.sealed = 0 OR EXISTS (
	SELECT 1 FROM {p}jobs j WHERE j.batch_id = b.id) OR ` + unfinishedNested + `) THEN 1 ELSE 0 END
FROM OPENJSON(@ids) WITH (id BIGINT '$') v
JOIN {p}batches b WITH (UPDLOCK, ROWLOCK, FORCESEEK) ON b.id = v.id
ORDER BY b.id`

const sqlAttach = `UPDATE b SET total = b.total + v.n
FROM OPENJSON(@counts) WITH (id BIGINT '$.id', n BIGINT '$.n') v
JOIN {p}batches b ON b.id = v.id`

const sqlNow = `SELECT CAST(SYSUTCDATETIME() AS DATETIME2(6))`

type holder struct {
	id      int64
	state   driver.State
	granted bool
}

type inserter struct {
	s       *Store
	own     bool
	jobs    []driver.InsertParams
	res     []driver.Inserted
	first   []int
	live    []int
	pos     []int
	base    int64
	won     []bool
	keys    [][]byte
	holders map[string]holder
	swaps   []int
	swapped []int
	limits  []string
	rules   map[string]rule
	late    []string
	queues  []string
	linked  bool
	batched bool
	now     time.Time
	link    *linker
}

// Insert inserts jobs in one transaction, as [driver.Writer.Insert] describes, and admits the
// throttled jobs of the limit keys they use. Jobs with no unique key, dependency, batch or limit
// are inserted by a single statement. After the commit Insert publishes the queues that received
// jobs to run, when the store has a bus.
func (s *Store) Insert(ctx context.Context, jobs []driver.InsertParams) ([]driver.Inserted, error) {
	res, wk, err := s.insert(ctx, nil, jobs)
	if err != nil {
		return nil, err
	}
	s.admitLate(ctx, &wk)
	s.nt.ready(wk.queues)
	return res, nil
}

func (s *Store) insert(ctx context.Context, tx *sql.Tx, jobs []driver.InsertParams) ([]driver.Inserted, wake, error) {
	if len(jobs) == 0 {
		return nil, wake{}, nil
	}
	if err := driver.CheckInsert(jobs); err != nil {
		return nil, wake{}, err
	}
	in := &inserter{s: s, own: tx == nil, jobs: jobs}
	in.plan()
	if in.own && !in.linked && !in.batched && len(in.keys) == 0 && len(in.limits) == 0 {
		if err := in.writePlain(ctx); err != nil {
			return nil, wake{}, err
		}
		return in.res, wake{queues: in.queues}, nil
	}
	var q querier = s.db
	if tx != nil {
		q = tx
	}
	if err := in.allocate(ctx, q); err != nil {
		return nil, wake{}, err
	}
	if len(in.limits) > 0 {
		if err := s.declare(ctx, in.limits, in.rules, tx != nil); err != nil {
			return nil, wake{}, wrap("insert", err)
		}
	}
	if tx != nil {
		if err := in.write(ctx, tx); err != nil {
			return nil, wake{}, err
		}
		return in.res, wake{queues: in.queues, late: in.rules}, nil
	}
	if err := s.txn(ctx, func(tx *sql.Tx) error { return in.write(ctx, tx) }); err != nil {
		return nil, wake{}, err
	}
	wk := wake{queues: in.queues}
	if len(in.late) > 0 {
		wk.late = make(map[string]rule, len(in.late))
		for _, key := range in.late {
			wk.late[key] = in.rules[key]
		}
	}
	return in.res, wk, nil
}

func (in *inserter) plan() {
	in.first = make([]int, len(in.jobs))
	in.pos = make([]int, len(in.jobs))
	var seen map[string]int
	for i := range in.jobs {
		p := &in.jobs[i]
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
			if p.UniqueReplace || p.UniqueDebounce > 0 {
				in.swaps = append(in.swaps, i)
			}
		}
		if p.LimitKey != "" {
			if in.rules == nil {
				in.rules = make(map[string]rule)
			}
			if _, ok := in.rules[p.LimitKey]; !ok {
				in.limits = append(in.limits, p.LimitKey)
			}
			in.rules[p.LimitKey] = ruleOf(p)
		}
		in.pos[i] = len(in.live)
		in.live = append(in.live, i)
	}
	slices.Sort(in.limits)
}

func (in *inserter) allocate(ctx context.Context, q querier) error {
	var now moment
	if err := q.QueryRowContext(ctx, in.s.q.allocate, sql.Named("n", len(in.live))).Scan(&in.base, &now); err != nil {
		return wrap("insert", err)
	}
	in.now = now.Time
	return nil
}

func (in *inserter) reset() {
	in.res = make([]driver.Inserted, len(in.jobs))
	in.won = make([]bool, len(in.live))
	for r, i := range in.live {
		in.won[r] = len(in.jobs[i].UniqueKey) == 0
	}
	in.holders = nil
	in.swapped = in.swapped[:0]
	in.link = nil
	in.queues = in.queues[:0]
}

func (in *inserter) writePlain(ctx context.Context) error {
	in.reset()
	var t table
	for r, i := range in.live {
		in.row(&t, r, i, 0, nil)
	}
	rows, err := in.s.db.QueryContext(ctx, in.s.q.insertPlain, sql.Named("n", len(in.live)), sql.Named("jobs", t.String()))
	if err != nil {
		return wrap("insert", err)
	}
	got, err := scanStates(rows)
	if err != nil {
		return wrap("insert", err)
	}
	if len(got) != len(in.live) {
		return fmt.Errorf("kiln: insert: %d rows inserted for %d jobs", len(got), len(in.live))
	}
	in.base = slices.Min(slices.Collect(maps.Keys(got)))
	in.record(got)
	return nil
}

func scanStates(rows *sql.Rows) (map[int64]driver.State, error) {
	defer rows.Close()
	got := make(map[int64]driver.State)
	for rows.Next() {
		var (
			id    int64
			state string
		)
		if err := rows.Scan(&id, &state); err != nil {
			return nil, err
		}
		got[id] = driver.State(state)
	}
	return got, rows.Err()
}

func (in *inserter) record(got map[int64]driver.State) {
	for r, i := range in.live {
		id := in.base + int64(r)
		st, ok := got[id]
		if !ok {
			continue
		}
		in.res[i] = driver.Inserted{ID: id, State: st}
		if st == driver.Enqueued {
			in.queues = merge(in.queues, in.jobs[i].Queue)
		}
	}
}

func (in *inserter) write(ctx context.Context, q querier) error {
	in.reset()
	if len(in.keys) > 0 {
		if err := in.claimUniques(ctx, q); err != nil {
			return err
		}
	}
	if len(in.swaps) > 0 {
		if err := in.replace(ctx, q); err != nil {
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
	}
	var t, deps table
	if in.linked {
		in.link.rows(&t, &deps)
	} else {
		for r, i := range in.live {
			if in.won[r] {
				in.row(&t, r, i, 0, nil)
			}
		}
	}
	if t.n > 0 {
		stmt := in.s.q.insertJobs
		if in.link != nil && in.link.doomed > 0 {
			stmt = in.s.q.insertDoomed + stmt
		}
		if deps.n > 0 {
			stmt = in.s.q.insertDeps + stmt
		}
		rows, err := q.QueryContext(ctx, stmt, sql.Named("base", in.base), sql.Named("now", stamp(in.now)),
			sql.Named("jobs", t.String()), sql.Named("deps", deps.String()))
		if err != nil {
			return wrap("insert", err)
		}
		got, err := scanStates(rows)
		if err != nil {
			return wrap("insert", err)
		}
		in.record(got)
	}
	if in.own && len(in.limits) > 0 {
		if err := in.admit(ctx, q); err != nil {
			return err
		}
	}
	in.settle()
	return nil
}

func (in *inserter) row(t *table, r, i, pending int, parents []byte) {
	p := &in.jobs[i]
	t.row()
	t.int("i", int64(r))
	t.str("q", p.Queue)
	t.str("k", p.Kind)
	t.int("p", int64(p.Priority))
	t.int("m", int64(p.MaxAttempts))
	t.int("t", millis(p.Timeout))
	if pending > 0 {
		t.int("d", int64(pending))
	}
	switch {
	case micros(p.UniqueDebounce) > 0:
		t.int("y", micros(p.UniqueDebounce))
	case !p.RunAt.IsZero():
		t.time("r", p.RunAt)
	case micros(p.Delay) > 0:
		t.int("y", micros(p.Delay))
	}
	if p.BatchID != 0 {
		t.int("b", p.BatchID)
	}
	if p.AfterBatch != 0 {
		t.int("a", p.AfterBatch)
	}
	t.json("pa", parents)
	t.opt("rc", p.RecurringID)
	if len(p.UniqueKey) > 0 && micros(p.UniqueFor) <= 0 {
		t.bin("u", p.UniqueKey)
	}
	t.opt("l", p.LimitKey)
	t.text("args", p.Args)
	t.json("meta", encodeMeta(p.Meta))
	t.json("tags", encodeStrings(p.Tags))
	t.opt("ti", clean(p.Title, maxTitle))
}

func (in *inserter) claimUniques(ctx context.Context, q querier) error {
	type claim struct {
		key []byte
		id  int64
		us  int64
	}
	var claims []claim
	for r, i := range in.live {
		if p := &in.jobs[i]; len(p.UniqueKey) > 0 {
			claims = append(claims, claim{key: p.UniqueKey, id: in.base + int64(r), us: micros(p.UniqueFor)})
		}
	}
	slices.SortFunc(claims, func(a, b claim) int { return bytes.Compare(a.key, b.key) })
	var t table
	for _, c := range claims {
		t.row()
		t.bin("k", c.key)
		t.int("id", c.id)
		t.int("us", c.us)
	}
	rows, err := q.QueryContext(ctx, in.s.q.claimUniques, sql.Named("claims", t.String()))
	if err != nil {
		return wrap("insert", err)
	}
	defer rows.Close()
	in.holders = make(map[string]holder, len(in.keys))
	for rows.Next() {
		var (
			key   []byte
			h     holder
			state string
		)
		if err := rows.Scan(&key, &h.id, &state, &h.granted); err != nil {
			return wrap("insert", err)
		}
		h.state = driver.State(state)
		in.holders[string(key)] = h
	}
	if err := rows.Err(); err != nil {
		return wrap("insert", err)
	}
	for r, i := range in.live {
		if key := in.jobs[i].UniqueKey; len(key) > 0 {
			in.won[r] = in.holders[string(key)].id == in.base+int64(r)
		}
	}
	return nil
}

func (in *inserter) replace(ctx context.Context, q querier) error {
	var t table
	for _, i := range in.swaps {
		p := &in.jobs[i]
		h := in.holders[string(p.UniqueKey)]
		if in.won[in.pos[i]] || !replaces(h, p) {
			continue
		}
		t.row()
		t.int("id", h.id)
		t.flag("d", p.UniqueDebounce > 0)
		t.text("args", p.Args)
		t.json("meta", encodeMeta(p.Meta))
		t.json("tags", encodeStrings(p.Tags))
		t.opt("ti", clean(p.Title, maxTitle))
		t.int("p", int64(p.Priority))
		t.time("r", in.runAt(p))
	}
	if t.n == 0 {
		return nil
	}
	rows, err := q.QueryContext(ctx, in.s.q.replace, sql.Named("jobs", t.String()), sql.Named("now", stamp(in.now)))
	if err != nil {
		return wrap("insert", err)
	}
	got, err := scanStates(rows)
	if err != nil {
		return wrap("insert", err)
	}
	for _, i := range in.swaps {
		key := string(in.jobs[i].UniqueKey)
		h := in.holders[key]
		if st, ok := got[h.id]; ok && !in.won[in.pos[i]] {
			h.state = st
			in.holders[key] = h
			in.swapped = append(in.swapped, i)
		}
	}
	return nil
}

func replaces(h holder, p *driver.InsertParams) bool {
	if p.UniqueDebounce > 0 {
		return h.state == driver.Scheduled && !h.granted
	}
	return p.UniqueReplace && (h.state == driver.Awaiting || h.state == driver.Scheduled ||
		h.state == driver.Throttled || h.state == driver.Enqueued)
}

func (in *inserter) runAt(p *driver.InsertParams) time.Time {
	switch {
	case micros(p.UniqueDebounce) > 0:
		return in.now.Add(p.UniqueDebounce)
	case !p.RunAt.IsZero():
		return p.RunAt
	}
	return in.now.Add(max(p.Delay, 0))
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
	ids := make([]int64, 0, len(counts))
	for id := range counts {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	rows, err := q.QueryContext(ctx, in.s.q.lockBatches, sql.Named("ids", idList(ids)))
	if err != nil {
		return wrap("insert", err)
	}
	open := make(map[int64]bool, len(ids))
	for rows.Next() {
		var (
			id int64
			ok bool
		)
		if err := rows.Scan(&id, &ok); err != nil {
			rows.Close()
			return wrap("insert", err)
		}
		open[id] = ok
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return wrap("insert", err)
	}
	var t table
	for _, id := range ids {
		ok, found := open[id]
		switch {
		case !found:
			return fmt.Errorf("%w: batch %d", driver.ErrNotFound, id)
		case !ok:
			return fmt.Errorf("%w: batch %d", driver.ErrClosed, id)
		}
		t.row()
		t.int("id", id)
		t.int("n", counts[id])
	}
	if _, err := q.ExecContext(ctx, in.s.q.attach, sql.Named("counts", t.String())); err != nil {
		return wrap("insert", err)
	}
	return nil
}

func (in *inserter) admit(ctx context.Context, q querier) error {
	slots, err := in.s.lockLimits(ctx, q, in.limits, true)
	if err != nil {
		return wrap("insert", err)
	}
	if err := in.s.restore(ctx, q, in.limits, in.rules, slots); err != nil {
		return wrap("insert", err)
	}
	in.late = in.late[:0]
	for _, key := range in.limits {
		if slots[key] == nil {
			in.late = append(in.late, key)
		}
	}
	a, err := in.s.fill(ctx, q, slots)
	if err != nil {
		return wrap("insert", err)
	}
	for _, id := range a.ids {
		in.mark(id, driver.Enqueued)
	}
	for _, m := range a.reserved {
		in.mark(m.id, driver.Scheduled)
	}
	in.queues = merge(in.queues, a.queues...)
	return nil
}

func (in *inserter) mark(id int64, st driver.State) {
	r := id - in.base
	if r < 0 || r >= int64(len(in.live)) {
		return
	}
	if i := in.live[r]; in.res[i].State == driver.Throttled {
		in.res[i].State = st
	}
}

func (in *inserter) settle() {
	for i, f := range in.first {
		if f == i && in.res[i].ID == 0 {
			h := in.holders[string(in.jobs[i].UniqueKey)]
			in.res[i] = driver.Inserted{ID: h.id, State: h.state, Duplicate: true,
				Replaced: slices.Contains(in.swapped, i)}
		}
	}
	for i, f := range in.first {
		if f >= 0 && f != i {
			r := in.res[f]
			r.Duplicate, r.Replaced = true, false
			in.res[i] = r
		}
	}
}

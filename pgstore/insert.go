package pgstore

import (
	"context"
	"fmt"
	"slices"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rafaelaugustos/kiln/driver"
)

const claimUniques = `, c AS (
	INSERT INTO {s}.uniques AS u (key, job_id, expires_at)
	SELECT w.ukey, w.id, w.expires_at FROM w ORDER BY w.ukey
	ON CONFLICT (key) DO NOTHING
	RETURNING u.key
), o AS MATERIALIZED (
	SELECT u.key FROM {s}.uniques u
	WHERE u.key = ANY(ARRAY(SELECT ukey FROM w)) AND (u.expires_at <= now() OR u.expires_at IS NULL AND (
		EXISTS (SELECT 1 FROM {s}.archive a WHERE a.id = u.job_id)
		OR EXISTS (SELECT 1 FROM {s}.jobs h WHERE h.id = u.job_id AND h.state = 'failed')))
	ORDER BY u.key
	FOR UPDATE
), x AS (
	UPDATE {s}.uniques u SET job_id = w.id, expires_at = w.expires_at
	FROM w WHERE u.key = w.ukey AND u.key IN (SELECT key FROM o)
	RETURNING u.key
), won AS (
	SELECT key FROM c UNION ALL SELECT key FROM x
)`

const insertRows = `
	FROM unnest($1::bigint[], $2::text[], $3::text[], $4::smallint[], $5::int[], $6::bigint[], $7::int[],
		$8::timestamptz[], $9::bigint[], $10::bigint[], $11::bigint[], $12::text[], $13::text[], $14::bytea[],
		$15::bigint[], $16::text[], $17::text[], $18::text[], $19::text[])
	WITH ORDINALITY AS t(id, queue, kind, priority, max_attempts, timeout_ms, pending, at, delay, batch_id,
		after_batch, parents, recurring_id, ukey, ufor, limit_key, args, meta, tags, ord)`

const sqlInsert = `WITH v AS MATERIALIZED (
	SELECT coalesce(t.id, nextval('{s}.job_ids')) AS id, t.ord, t.queue, t.kind, coalesce(t.priority, 0) AS priority,
		t.max_attempts, coalesce(t.timeout_ms, 0) AS timeout_ms, coalesce(t.pending, 0) AS pending,
		coalesce(t.at, statement_timestamp() + coalesce(t.delay, 0) * interval '1 microsecond') AS run_at,
		nullif(t.batch_id, 0) AS batch_id, nullif(t.after_batch, 0) AS after_batch,
		nullif(t.parents, '')::bigint[] AS parents, nullif(t.recurring_id, '') AS recurring_id,
		t.ukey, coalesce(t.ufor, 0) AS ufor, nullif(t.limit_key, '') AS limit_key,
		t.args::json AS args, nullif(t.meta, '')::jsonb AS meta, nullif(t.tags, '')::text[] AS tags` + insertRows + `
), w AS (
	SELECT v.id, v.ukey, CASE WHEN v.ufor > 0 THEN now() + v.ufor * interval '1 microsecond' END AS expires_at
	FROM v WHERE $20 AND v.ukey IS NOT NULL
)` + claimUniques + `, i AS (
	INSERT INTO {s}.jobs (id, state, queue, kind, priority, max_attempts, timeout_ms, deps_pending, run_at,
		batch_id, after_batch, parents, recurring_id, unique_key, limit_key, args, meta, tags)
	SELECT v.id, CASE
			WHEN v.pending > 0 THEN 'awaiting'
			WHEN v.run_at > statement_timestamp() THEN 'scheduled'
			WHEN v.limit_key IS NOT NULL THEN 'throttled'
			ELSE 'enqueued'
		END::{s}.state,
		v.queue, v.kind, v.priority, v.max_attempts, v.timeout_ms, v.pending, v.run_at,
		v.batch_id, v.after_batch, v.parents, v.recurring_id, CASE WHEN v.ufor = 0 THEN v.ukey END,
		v.limit_key, v.args, v.meta, v.tags
	FROM v WHERE v.ukey IS NULL OR NOT $20 OR v.ukey IN (SELECT key FROM won)
	RETURNING id, state
)
SELECT v.ord, i.id, i.state::text FROM i JOIN v ON v.id = i.id`

const sqlInsertDoomed = `INSERT INTO {s}.archive (id, state, queue, kind, priority, attempt, max_attempts, claim,
	timeout_ms, run_at, created_at, finalized_at, batch_id, after_batch, parents, recurring_id, limit_key,
	args, meta, tags, history)
SELECT t.id, 'deleted', t.queue, t.kind, coalesce(t.priority, 0), 0, t.max_attempts, 0, coalesce(t.timeout_ms, 0),
	coalesce(t.at, statement_timestamp() + coalesce(t.delay, 0) * interval '1 microsecond'), now(), now(),
	nullif(t.batch_id, 0), nullif(t.after_batch, 0), nullif(t.parents, '')::bigint[], nullif(t.recurring_id, ''),
	nullif(t.limit_key, ''), t.args::json, nullif(t.meta, '')::jsonb, nullif(t.tags, '')::text[],
	jsonb_build_array({s}.entry('deleted', 0, r.reason, '', '', NULL))` + insertRows + `
JOIN unnest($20::text[]) WITH ORDINALITY AS r(reason, ord) ON r.ord = t.ord`

const sqlAllocate = `WITH v AS MATERIALIZED (
	SELECT nextval('{s}.job_ids') AS id, t.ord, t.ukey, coalesce(t.ufor, 0) AS ufor
	FROM unnest($1::bytea[], $2::bigint[]) WITH ORDINALITY AS t(ukey, ufor, ord)
), w AS (
	SELECT v.id, v.ukey, CASE WHEN v.ufor > 0 THEN now() + v.ufor * interval '1 microsecond' END AS expires_at
	FROM v WHERE v.ukey IS NOT NULL
)` + claimUniques + `
SELECT v.id, v.ukey IS NULL OR v.ukey IN (SELECT key FROM won) FROM v ORDER BY v.ord`

const sqlHolders = `SELECT u.key, u.job_id, coalesce(j.state::text, a.state::text, '')
FROM {s}.uniques u
LEFT JOIN {s}.jobs j ON j.id = u.job_id
LEFT JOIN {s}.archive a ON a.id = u.job_id
WHERE u.key = ANY($1)`

const sqlLockParents = `SELECT id, state::text FROM {s}.jobs WHERE id = ANY($1) ORDER BY id FOR SHARE`

const sqlArchivedParents = `SELECT id, state::text FROM {s}.archive WHERE id = ANY($1)`

const sqlAfterBatches = `SELECT id, finished_at IS NOT NULL FROM {s}.batches WHERE id = ANY($1)`

const sqlMissingLinks = `SELECT {s}.raise('KL002', m.what) FROM (
	SELECT 'parent job ' || p.id AS what FROM unnest($1::bigint[]) AS p(id)
	WHERE NOT EXISTS (SELECT 1 FROM {s}.jobs j WHERE j.id = p.id)
		AND NOT EXISTS (SELECT 1 FROM {s}.archive a WHERE a.id = p.id)
	UNION ALL
	SELECT 'batch ' || b.id FROM unnest($2::bigint[]) AS b(id)
	WHERE NOT EXISTS (SELECT 1 FROM {s}.batches x WHERE x.id = b.id)
	LIMIT 1
) m`

const sqlInsertDeps = `INSERT INTO {s}.deps (batch, parent_id, job_id, mask, resolved)
SELECT b, p, j, m, r FROM unnest($1::bool[], $2::bigint[], $3::bigint[], $4::smallint[], $5::bool[]) AS t(b, p, j, m, r)
ON CONFLICT DO NOTHING`

const sqlLimits = `WITH l AS MATERIALIZED (
	SELECT key FROM {s}.limits WHERE key = ANY($1) ORDER BY key FOR KEY SHARE
)
INSERT INTO {s}.limits (key, max)
SELECT t.k, t.m FROM unnest($1::text[], $2::int[]) AS t(k, m) WHERE t.k NOT IN (SELECT key FROM l) ORDER BY t.k
ON CONFLICT (key) DO NOTHING`

const sqlAttach = `WITH n AS (
	SELECT id, n FROM unnest($1::bigint[], $2::bigint[]) AS t(id, n)
), b AS MATERIALIZED (
	SELECT b.id, b.finished_at IS NULL AND (NOT b.sealed OR (
		SELECT count(*) FROM (SELECT 1 FROM {s}.jobs j WHERE j.batch_id = b.id LIMIT n.n + 1) x) > n.n) AS open
	FROM {s}.batches b JOIN n ON n.id = b.id
	ORDER BY b.id
	FOR NO KEY UPDATE OF b
), u AS (
	UPDATE {s}.batches x SET total = x.total + n.n FROM n, b WHERE x.id = n.id AND b.id = n.id AND b.open
)
SELECT CASE
	WHEN (SELECT count(*) FROM b) < (SELECT count(*) FROM n) THEN {s}.raise('KL002', 'batch not found')
	WHEN NOT (SELECT bool_and(open) FROM b) THEN {s}.raise('KL001', 'batch is closed')
	ELSE true
END`

type wake struct {
	queues []string
	keys   []string
	maxes  []int32
}

func (w *wake) queue(q string) {
	if !slices.Contains(w.queues, q) {
		w.queues = append(w.queues, q)
	}
}

func (w *wake) merge(o wake) {
	for _, q := range o.queues {
		w.queue(q)
	}
	for i, k := range o.keys {
		w.limit(k, o.maxes[i])
	}
}

func (w *wake) limit(key string, n int32) {
	if i := slices.Index(w.keys, key); i >= 0 {
		w.maxes[i] = n
		return
	}
	w.keys = append(w.keys, key)
	w.maxes = append(w.maxes, n)
}

type holder struct {
	id    int64
	state driver.State
}

type inserter struct {
	s       *Store
	tx      pgx.Tx
	conn    *pgxpool.Conn
	jobs    []driver.InsertParams
	res     []driver.Inserted
	first   []int
	live    []int
	keys    [][]byte
	holders map[string]holder
	linked  bool
	batched bool
	w       wake
}

func (s *Store) Insert(ctx context.Context, jobs []driver.InsertParams) ([]driver.Inserted, error) {
	res, w, err := s.insert(ctx, nil, jobs)
	if err != nil {
		return nil, err
	}
	s.nt.jobs(w.queues...)
	return res, nil
}

func (s *Store) insert(ctx context.Context, tx pgx.Tx, jobs []driver.InsertParams) ([]driver.Inserted, wake, error) {
	if len(jobs) == 0 {
		return nil, wake{}, nil
	}
	if err := validate(jobs); err != nil {
		return nil, wake{}, err
	}
	in := &inserter{s: s, tx: tx, jobs: jobs, res: make([]driver.Inserted, len(jobs))}
	in.plan()
	var err error
	if in.linked {
		err = in.runLinked(ctx)
	} else {
		err = in.runPlain(ctx)
	}
	if in.conn != nil {
		if err != nil {
			in.conn.Exec(context.WithoutCancel(ctx), "rollback")
		}
		in.conn.Release()
	}
	if err != nil {
		return nil, wake{}, err
	}
	return in.res, in.w, nil
}

func validate(jobs []driver.InsertParams) error {
	for i := range jobs {
		p := &jobs[i]
		switch {
		case p.Kind == "":
			return fmt.Errorf("%w: job %d has no kind", driver.ErrInvalid, i)
		case p.Queue == "":
			return fmt.Errorf("%w: job %d has no queue", driver.ErrInvalid, i)
		case p.MaxAttempts < 1:
			return fmt.Errorf("%w: job %d max attempts %d", driver.ErrInvalid, i, p.MaxAttempts)
		case len(p.Args) == 0:
			return fmt.Errorf("%w: job %d has no args", driver.ErrInvalid, i)
		case p.LimitKey != "" && p.LimitMax < 1:
			return fmt.Errorf("%w: job %d limit max %d", driver.ErrInvalid, i, p.LimitMax)
		case p.BatchID < 0 || p.AfterBatch < 0:
			return fmt.Errorf("%w: job %d batch", driver.ErrInvalid, i)
		}
		for _, par := range p.Parents {
			switch {
			case par.On&driver.OnFinished == 0 || par.ID < 0:
				return fmt.Errorf("%w: job %d parent %+v", driver.ErrInvalid, i, par)
			case par.ID == 0 && (par.Index < 0 || par.Index >= len(jobs) || par.Index == i):
				return fmt.Errorf("%w: job %d parent index %d", driver.ErrInvalid, i, par.Index)
			}
		}
	}
	return acyclic(jobs)
}

func acyclic(jobs []driver.InsertParams) error {
	const (
		unseen = iota
		open
		closed
	)
	color := make([]uint8, len(jobs))
	var visit func(i int) bool
	visit = func(i int) bool {
		color[i] = open
		for _, par := range jobs[i].Parents {
			if par.ID != 0 {
				continue
			}
			switch color[par.Index] {
			case open:
				return false
			case unseen:
				if !visit(par.Index) {
					return false
				}
			}
		}
		color[i] = closed
		return true
	}
	for i := range jobs {
		if color[i] == unseen && !visit(i) {
			return fmt.Errorf("%w: dependency cycle through job %d", driver.ErrInvalid, i)
		}
	}
	return nil
}

func (in *inserter) plan() {
	in.first = make([]int, len(in.jobs))
	var seen map[string]int
	for i := range in.jobs {
		p := &in.jobs[i]
		in.first[i] = -1
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
		in.live = append(in.live, i)
	}
}

func (in *inserter) begin(ctx context.Context, b *pgx.Batch) error {
	if in.tx != nil {
		return nil
	}
	c, err := in.s.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("kiln: insert: %w", err)
	}
	in.conn = c
	b.Queue("begin")
	return nil
}

func (in *inserter) send(ctx context.Context, b *pgx.Batch) error {
	var br pgx.BatchResults
	switch {
	case in.tx != nil:
		br = in.tx.SendBatch(ctx, b)
	case in.conn != nil:
		br = in.conn.SendBatch(ctx, b)
	default:
		br = in.s.pool.SendBatch(ctx, b)
	}
	if err := br.Close(); err != nil {
		return wrap("insert", err)
	}
	return nil
}

func (in *inserter) runPlain(ctx context.Context) error {
	split := in.batched && len(in.keys) > 0
	b := &pgx.Batch{}
	if split {
		if err := in.begin(ctx, b); err != nil {
			return err
		}
	}
	cols := in.columns(in.live, nil, nil)
	b.Queue(in.s.q.insert, cols.params(true)...).Query(in.scanInserted(in.live))
	if len(in.keys) > 0 {
		b.Queue(in.s.q.holders, in.keys).Query(in.scanHolders)
	}
	in.queueLimits(b, in.live)
	if !split {
		in.queueAttach(b, in.live)
	}
	if err := in.send(ctx, b); err != nil {
		return err
	}
	in.settle()
	if !split {
		return nil
	}
	b = &pgx.Batch{}
	in.queueAttach(b, in.inserted())
	if in.conn != nil {
		b.Queue("commit")
	}
	return in.send(ctx, b)
}

func (in *inserter) scanInserted(items []int) func(pgx.Rows) error {
	return func(rows pgx.Rows) error {
		var (
			ord   int
			id    int64
			state string
		)
		for rows.Next() {
			if err := rows.Scan(&ord, &id, &state); err != nil {
				return err
			}
			i := items[ord-1]
			in.res[i] = driver.Inserted{ID: id, State: driver.State(state)}
			if state == string(driver.Enqueued) {
				in.w.queue(in.jobs[i].Queue)
			}
		}
		return rows.Err()
	}
}

func (in *inserter) scanHolders(rows pgx.Rows) error {
	in.holders = make(map[string]holder, len(in.keys))
	var (
		key   []byte
		h     holder
		state string
	)
	for rows.Next() {
		if err := rows.Scan(&key, &h.id, &state); err != nil {
			return err
		}
		h.state = driver.State(state)
		in.holders[string(key)] = h
	}
	return rows.Err()
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

func (in *inserter) inserted() []int {
	var items []int
	for _, i := range in.live {
		if !in.res[i].Duplicate {
			items = append(items, i)
		}
	}
	return items
}

func (in *inserter) queueLimits(b *pgx.Batch, items []int) {
	var w wake
	for _, i := range items {
		if p := &in.jobs[i]; p.LimitKey != "" {
			w.limit(p.LimitKey, int32(p.LimitMax))
		}
	}
	if len(w.keys) == 0 {
		return
	}
	b.Queue(in.s.q.limits, w.keys, w.maxes)
	if in.tx == nil {
		b.Queue(in.s.q.admit, w.keys, w.maxes).Query(in.w.scanQueues)
		return
	}
	in.w.keys, in.w.maxes = w.keys, w.maxes
}

func (in *inserter) queueAttach(b *pgx.Batch, items []int) {
	var ids, counts []int64
	for _, i := range items {
		id := in.jobs[i].BatchID
		if id == 0 {
			continue
		}
		if j := slices.Index(ids, id); j >= 0 {
			counts[j]++
			continue
		}
		ids = append(ids, id)
		counts = append(counts, 1)
	}
	if len(ids) > 0 {
		b.Queue(in.s.q.attach, ids, counts)
	}
}

type columns struct {
	id        []int64
	queue     []string
	kind      []string
	priority  []int16
	attempts  []int32
	timeout   []int64
	pending   []int32
	at        []pgtype.Timestamptz
	delay     []int64
	batch     []int64
	after     []int64
	parents   []string
	recurring []string
	ukey      [][]byte
	ufor      []int64
	limit     []string
	args      []string
	meta      []string
	tags      []string
}

func put[T any](col *[]T, n, i int, v T) {
	if *col == nil {
		*col = make([]T, n)
	}
	(*col)[i] = v
}

func (in *inserter) columns(items []int, ids []int64, pending []int32) *columns {
	n := len(items)
	c := &columns{
		id:       ids,
		pending:  pending,
		queue:    make([]string, n),
		kind:     make([]string, n),
		attempts: make([]int32, n),
		args:     make([]string, n),
	}
	for r, i := range items {
		p := &in.jobs[i]
		c.queue[r] = p.Queue
		c.kind[r] = p.Kind
		c.attempts[r] = int32(p.MaxAttempts)
		c.args[r] = string(p.Args)
		if p.Priority != 0 {
			put(&c.priority, n, r, p.Priority)
		}
		if p.Timeout != 0 {
			put(&c.timeout, n, r, millis(p.Timeout))
		}
		if !p.RunAt.IsZero() {
			put(&c.at, n, r, pgtype.Timestamptz{Time: p.RunAt, Valid: true})
		} else if p.Delay > 0 {
			put(&c.delay, n, r, micros(p.Delay))
		}
		if p.BatchID != 0 {
			put(&c.batch, n, r, p.BatchID)
		}
		if p.AfterBatch != 0 {
			put(&c.after, n, r, p.AfterBatch)
		}
		if p.RecurringID != "" {
			put(&c.recurring, n, r, p.RecurringID)
		}
		if len(p.UniqueKey) > 0 {
			put(&c.ukey, n, r, p.UniqueKey)
			if p.UniqueFor > 0 {
				put(&c.ufor, n, r, max(micros(p.UniqueFor), 1))
			}
		}
		if p.LimitKey != "" {
			put(&c.limit, n, r, p.LimitKey)
		}
		if len(p.Meta) > 0 {
			put(&c.meta, n, r, encodeMeta(p.Meta))
		}
		if p.Tags != nil {
			put(&c.tags, n, r, textArray(p.Tags))
		}
	}
	return c
}

func (c *columns) params(extra ...any) []any {
	return append([]any{c.id, c.queue, c.kind, c.priority, c.attempts, c.timeout, c.pending, c.at, c.delay,
		c.batch, c.after, c.parents, c.recurring, c.ukey, c.ufor, c.limit, c.args, c.meta, c.tags}, extra...)
}

package mysqlstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

const maxStatement = 8 << 20

const sqlAllocate = `UPDATE {p}sequences SET last_id = LAST_INSERT_ID(last_id + ?) WHERE name = 'jobs'`

const sqlInsertJobs = `INSERT INTO {p}jobs (id, state, queue, kind, priority, max_attempts, timeout_ms, deps_pending,
	run_at, created_at, batch_id, after_batch, parents, recurring_id, unique_key, limit_key, args, meta, tags) VALUES `

const sqlInsertDoomed = `INSERT INTO {p}archive (id, state, queue, kind, priority, attempt, max_attempts, claim,
	timeout_ms, run_at, created_at, finalized_at, batch_id, after_batch, parents, recurring_id, limit_key, args, meta,
	tags, history) VALUES `

const sqlInsertDeps = `INSERT INTO {p}deps (batch, parent_id, job_id, mask, resolved) VALUES `

const sqlKeyState = `SELECT u.unique_key, u.job_id, j.id IS NULL FROM {p}uniques u
LEFT JOIN {p}jobs j ON j.id = u.job_id AND j.state <> 'failed'
WHERE u.unique_key IN (?)`

const sqlClaimUniques = `INSERT INTO {p}uniques (unique_key, job_id, expires_at)
SELECT v.k, v.id, v.exp FROM (VALUES `

const sqlClaimUniquesTail = `) AS v (k, id, exp, seen, gone)
ORDER BY v.k
ON DUPLICATE KEY UPDATE
	job_id = IF({p}uniques.expires_at <= UTC_TIMESTAMP(6)
		OR {p}uniques.expires_at IS NULL AND v.gone AND {p}uniques.job_id <=> v.seen, v.id, {p}uniques.job_id),
	expires_at = IF({p}uniques.job_id = v.id, v.exp, {p}uniques.expires_at)`

const sqlHolders = `SELECT u.unique_key, u.job_id, COALESCE(j.state, a.state, ''), UTC_TIMESTAMP(6)
FROM {p}uniques u
LEFT JOIN {p}jobs j ON j.id = u.job_id
LEFT JOIN {p}archive a ON a.id = u.job_id
WHERE u.unique_key IN (?)
FOR SHARE OF u`

const sqlLockBatches = `SELECT b.id, b.finished_at IS NULL AND (NOT b.sealed OR EXISTS (
	SELECT 1 FROM {p}jobs j WHERE j.batch_id = b.id))
FROM {p}batches b FORCE INDEX (PRIMARY) WHERE b.id IN (?) ORDER BY b.id FOR UPDATE OF b`

const sqlNow = `SELECT UTC_TIMESTAMP(6)`

type holder struct {
	id    int64
	state driver.State
}

type inserter struct {
	s       *Store
	own     bool
	jobs    []driver.InsertParams
	res     []driver.Inserted
	first   []int
	live    []int
	pos     []int
	ids     []int64
	won     []bool
	keys    [][]byte
	holders map[string]holder
	limits  []string
	maxes   map[string]int
	late    []string
	linked  bool
	batched bool
	timed   bool
	now     time.Time
	link    *linker
}

func (s *Store) Insert(ctx context.Context, jobs []driver.InsertParams) ([]driver.Inserted, error) {
	res, _, err := s.insert(ctx, nil, jobs)
	return res, err
}

func (s *Store) insert(ctx context.Context, tx *sql.Tx, jobs []driver.InsertParams) ([]driver.Inserted, map[string]int, error) {
	if len(jobs) == 0 {
		return nil, nil, nil
	}
	if err := validate(jobs); err != nil {
		return nil, nil, err
	}
	in := &inserter{s: s, own: tx == nil, jobs: jobs}
	in.plan()
	if err := in.allocate(ctx); err != nil {
		return nil, nil, err
	}
	if len(in.limits) > 0 {
		if err := s.declare(ctx, in.limits, in.maxes, tx != nil); err != nil {
			return nil, nil, wrap("insert", err)
		}
	}
	if tx != nil {
		if err := in.write(ctx, tx); err != nil {
			return nil, nil, err
		}
		return in.res, in.maxes, nil
	}
	var err error
	if in.linked || in.batched || len(in.keys) > 0 || len(in.limits) > 0 {
		err = s.txn(ctx, func(tx *sql.Tx) error { return in.write(ctx, tx) })
	} else {
		err = in.writePlain(ctx)
	}
	if err != nil {
		return nil, nil, err
	}
	if len(in.late) > 0 {
		late := make(map[string]int, len(in.late))
		for _, key := range in.late {
			late[key] = in.maxes[key]
		}
		s.admitKeys(ctx, late)
	}
	return in.res, nil, nil
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
		case !json.Valid(p.Args):
			return fmt.Errorf("%w: job %d args are not valid JSON", driver.ErrInvalid, i)
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
		if !p.RunAt.IsZero() {
			in.timed = true
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
		if p.LimitKey != "" {
			if in.maxes == nil {
				in.maxes = make(map[string]int)
			}
			if _, ok := in.maxes[p.LimitKey]; !ok {
				in.limits = append(in.limits, p.LimitKey)
			}
			in.maxes[p.LimitKey] = p.LimitMax
		}
		in.pos[i] = len(in.live)
		in.live = append(in.live, i)
	}
	slices.Sort(in.limits)
}

func (in *inserter) allocate(ctx context.Context) error {
	first, err := in.s.seq.take(ctx, int64(len(in.live)))
	if err != nil {
		return wrap("insert", err)
	}
	in.ids = make([]int64, len(in.live))
	for i := range in.ids {
		in.ids[i] = first + int64(i)
	}
	return nil
}

func (in *inserter) reset() {
	in.res = make([]driver.Inserted, len(in.jobs))
	in.won = make([]bool, len(in.live))
	for r, i := range in.live {
		in.won[r] = len(in.jobs[i].UniqueKey) == 0
	}
	in.holders = nil
	in.link = nil
}

func (in *inserter) writePlain(ctx context.Context) error {
	in.reset()
	if in.timed {
		if err := in.fetchNow(ctx, in.s.db); err != nil {
			return err
		}
	}
	stmts := in.jobRows(in.live, nil, nil)
	if len(stmts) == 1 {
		if _, err := in.s.db.ExecContext(ctx, stmts[0]); err != nil {
			return wrap("insert", err)
		}
		return nil
	}
	return in.s.txn(ctx, func(tx *sql.Tx) error { return execAll(ctx, tx, "insert", stmts) })
}

func (in *inserter) write(ctx context.Context, q querier) error {
	in.reset()
	if len(in.keys) > 0 {
		if err := in.claimUniques(ctx, q); err != nil {
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
	} else if in.timed && in.now.IsZero() {
		if err := in.fetchNow(ctx, q); err != nil {
			return err
		}
	}
	var stmts []string
	if in.link != nil {
		stmts = in.link.rows()
	} else {
		var items []int
		for r, i := range in.live {
			if in.won[r] {
				items = append(items, i)
			}
		}
		stmts = in.jobRows(items, nil, nil)
	}
	if err := execAll(ctx, q, "insert", stmts); err != nil {
		return err
	}
	if in.own && len(in.limits) > 0 {
		if err := in.admit(ctx, q); err != nil {
			return err
		}
	}
	in.settle()
	return nil
}

func execAll(ctx context.Context, q querier, op string, stmts []string) error {
	for _, stmt := range stmts {
		if _, err := q.ExecContext(ctx, stmt); err != nil {
			return wrap(op, err)
		}
	}
	return nil
}

func (in *inserter) fetchNow(ctx context.Context, q querier) error {
	var now stamp
	if err := q.QueryRowContext(ctx, sqlNow).Scan(&now); err != nil {
		return wrap("insert", err)
	}
	in.now = now.Time
	return nil
}

type seenKey struct {
	holder int64
	gone   bool
}

func (in *inserter) keyState(ctx context.Context, q querier) (map[string]seenKey, error) {
	rows, err := q.QueryContext(ctx, render(in.s.q.keyState, in.keys))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	seen := make(map[string]seenKey, len(in.keys))
	for rows.Next() {
		var (
			key []byte
			k   seenKey
		)
		if err := rows.Scan(&key, &k.holder, &k.gone); err != nil {
			return nil, err
		}
		seen[string(key)] = k
	}
	return seen, rows.Err()
}

func (in *inserter) claimUniques(ctx context.Context, q querier) error {
	seen, err := in.keyState(ctx, q)
	if err != nil {
		return wrap("insert", err)
	}
	b := make([]byte, 0, 96*len(in.keys)+512)
	b = append(b, in.s.q.claimUniques...)
	n := 0
	for r, i := range in.live {
		p := &in.jobs[i]
		if len(p.UniqueKey) == 0 {
			continue
		}
		if n > 0 {
			b = append(b, ',')
		}
		n++
		b = append(b, "ROW("...)
		b = appendBytes(b, p.UniqueKey)
		b = append(b, ',')
		b = strconv.AppendInt(b, in.ids[r], 10)
		b = append(b, ',')
		if us := micros(p.UniqueFor); us > 0 {
			b = append(b, "UTC_TIMESTAMP(6) + INTERVAL "...)
			b = strconv.AppendInt(b, us, 10)
			b = append(b, " MICROSECOND"...)
		} else {
			b = append(b, "NULL"...)
		}
		if k, ok := seen[string(p.UniqueKey)]; ok {
			b = appendSQL(b, ",?,?)", k.holder, k.gone)
		} else {
			b = append(b, ",NULL,FALSE)"...)
		}
	}
	b = append(b, in.s.q.claimUniquesTail...)
	if _, err := q.ExecContext(ctx, string(b)); err != nil {
		return wrap("insert", err)
	}
	rows, err := q.QueryContext(ctx, render(in.s.q.holders, in.keys))
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
			now   stamp
		)
		if err := rows.Scan(&key, &h.id, &state, &now); err != nil {
			return wrap("insert", err)
		}
		h.state = driver.State(state)
		in.holders[string(key)] = h
		in.now = now.Time
	}
	if err := rows.Err(); err != nil {
		return wrap("insert", err)
	}
	for r, i := range in.live {
		if key := in.jobs[i].UniqueKey; len(key) > 0 {
			in.won[r] = in.holders[string(key)].id == in.ids[r]
		}
	}
	return nil
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
	rows, err := q.QueryContext(ctx, render(in.s.q.lockBatches, ids))
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
	for _, id := range ids {
		ok, found := open[id]
		switch {
		case !found:
			return fmt.Errorf("%w: batch %d", driver.ErrNotFound, id)
		case !ok:
			return fmt.Errorf("%w: batch %d", driver.ErrClosed, id)
		}
	}
	b := make([]byte, 0, 64+24*len(ids))
	b = append(b, "UPDATE "...)
	b = append(b, in.s.prefix...)
	b = append(b, "batches SET total = total + CASE id"...)
	for _, id := range ids {
		b = appendSQL(b, " WHEN ? THEN ?", id, counts[id])
	}
	b = appendSQL(b, " END WHERE id IN (?)", ids)
	if _, err := q.ExecContext(ctx, string(b)); err != nil {
		return wrap("insert", err)
	}
	return nil
}

func (in *inserter) admit(ctx context.Context, q querier) error {
	slots, err := in.s.lockLimits(ctx, q, in.limits, true)
	if err != nil {
		return wrap("insert", err)
	}
	if err := in.s.restore(ctx, q, in.limits, in.maxes, slots); err != nil {
		return wrap("insert", err)
	}
	in.late = in.late[:0]
	for _, key := range in.limits {
		if slots[key] == nil {
			in.late = append(in.late, key)
		}
	}
	admitted, err := in.s.fill(ctx, q, slots)
	if err != nil {
		return wrap("insert", err)
	}
	for _, id := range admitted.ids {
		r, ok := slices.BinarySearch(in.ids, id)
		if !ok {
			continue
		}
		if i := in.live[r]; in.res[i].State == driver.Throttled {
			in.res[i].State = driver.Enqueued
		}
	}
	return nil
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

func (in *inserter) state(p *driver.InsertParams, pending int) driver.State {
	switch {
	case pending > 0:
		return driver.Awaiting
	case !p.RunAt.IsZero() && p.RunAt.Truncate(time.Microsecond).After(in.now):
		return driver.Scheduled
	case p.RunAt.IsZero() && micros(p.Delay) > 0:
		return driver.Scheduled
	case p.LimitKey != "":
		return driver.Throttled
	}
	return driver.Enqueued
}

func appendRunAt(b []byte, p *driver.InsertParams) []byte {
	switch {
	case !p.RunAt.IsZero():
		return appendTime(b, p.RunAt)
	case micros(p.Delay) > 0:
		b = append(b, "UTC_TIMESTAMP(6) + INTERVAL "...)
		b = strconv.AppendInt(b, micros(p.Delay), 10)
		return append(b, " MICROSECOND"...)
	}
	return append(b, "UTC_TIMESTAMP(6)"...)
}

func appendOptInt(b []byte, n int64) []byte {
	if n == 0 {
		return append(b, "NULL"...)
	}
	return strconv.AppendInt(b, n, 10)
}

func appendOptString(b []byte, s string) []byte {
	if s == "" {
		return append(b, "NULL"...)
	}
	return appendString(b, s)
}

func appendJSON(b []byte, j jsonText) []byte {
	if j == nil {
		return append(b, "NULL"...)
	}
	return appendString(b, string(j))
}

type chunks struct {
	head string
	tail string
	max  int
	b    []byte
	last int
	rows int
	out  []string
}

func (c *chunks) row() {
	c.fit()
	if c.rows == 0 {
		c.b = append(c.b[:0], c.head...)
	} else {
		c.b = append(c.b, ',')
	}
	c.last = len(c.b)
	c.rows++
}

func (c *chunks) fit() {
	if c.rows == 0 || len(c.b)+len(c.tail) <= c.max {
		return
	}
	if c.rows > 1 {
		c.out = append(c.out, string(c.b[:c.last-1])+c.tail)
		c.b = c.b[:len(c.head)+copy(c.b[len(c.head):], c.b[c.last:])]
		c.last = len(c.head)
		c.rows = 1
	}
	if len(c.b)+len(c.tail) > c.max {
		c.flush()
	}
}

func (c *chunks) flush() {
	if c.rows > 0 {
		c.out = append(c.out, string(append(c.b, c.tail...)))
		c.rows = 0
	}
}

func (c *chunks) done() []string {
	c.fit()
	c.flush()
	return c.out
}

func (in *inserter) jobRows(items []int, pending []int, parents []jsonText) []string {
	if len(items) == 0 {
		return nil
	}
	c := &chunks{head: in.s.q.insertJobs, max: in.s.budget, b: make([]byte, 0, min(in.s.budget, 256*len(items)))}
	for k, i := range items {
		p := &in.jobs[i]
		n := 0
		if pending != nil {
			n = pending[k]
		}
		var list jsonText
		if parents != nil {
			list = parents[k]
		}
		st := in.state(p, n)
		r := in.pos[i]
		in.res[i] = driver.Inserted{ID: in.ids[r], State: st}
		c.row()
		b := append(c.b, '(')
		b = strconv.AppendInt(b, in.ids[r], 10)
		b = append(b, ",'"...)
		b = append(b, st...)
		b = append(b, "',"...)
		b = appendString(b, p.Queue)
		b = append(b, ',')
		b = appendString(b, p.Kind)
		b = append(b, ',')
		b = strconv.AppendInt(b, int64(p.Priority), 10)
		b = append(b, ',')
		b = strconv.AppendInt(b, int64(p.MaxAttempts), 10)
		b = append(b, ',')
		b = strconv.AppendInt(b, millis(p.Timeout), 10)
		b = append(b, ',')
		b = strconv.AppendInt(b, int64(n), 10)
		b = append(b, ',')
		b = appendRunAt(b, p)
		b = append(b, ",UTC_TIMESTAMP(6),"...)
		b = appendOptInt(b, p.BatchID)
		b = append(b, ',')
		b = appendOptInt(b, p.AfterBatch)
		b = append(b, ',')
		b = appendJSON(b, list)
		b = append(b, ',')
		b = appendOptString(b, p.RecurringID)
		b = append(b, ',')
		if len(p.UniqueKey) > 0 && micros(p.UniqueFor) <= 0 {
			b = appendBytes(b, p.UniqueKey)
		} else {
			b = append(b, "NULL"...)
		}
		b = append(b, ',')
		b = appendOptString(b, p.LimitKey)
		b = append(b, ',')
		b = appendBytes(b, p.Args)
		b = append(b, ',')
		b = appendJSON(b, encodeMeta(p.Meta))
		b = append(b, ',')
		b = appendJSON(b, encodeStrings(p.Tags))
		c.b = append(b, ')')
	}
	return c.done()
}

package mysqlstore

import (
	"context"
	"fmt"
	"slices"
	"strconv"

	"github.com/rafaelaugustos/kiln/driver"
)

const sqlLockParents = `(SELECT 'j', id, state, NULL FROM {p}jobs FORCE INDEX (PRIMARY) WHERE id IN (?) ORDER BY id FOR SHARE)
UNION ALL (SELECT 'a', id, state, NULL FROM {p}archive WHERE id IN (?))
UNION ALL `

const sqlArchivedParents = `SELECT id, state FROM {p}archive WHERE id IN (?) FOR SHARE`

const sqlAfterBatches = `(SELECT 'b', id, IF(finished_at IS NULL, '', 'finished'), NULL FROM {p}batches WHERE id IN (?))
UNION ALL `

const sqlLinkedNow = `(SELECT 'n', 0, '', UTC_TIMESTAMP(6))`

type edge struct {
	id    int64
	mask  driver.Mask
	state driver.State
}

type dep struct {
	batch    bool
	parent   int64
	job      int64
	mask     driver.Mask
	resolved bool
}

type linker struct {
	in       *inserter
	parents  []int64
	batches  []int64
	states   map[int64]driver.State
	finished map[int64]bool
	seen     []bool
	reason   []string
	pending  []int
	lists    []jsonText
	deps     []dep
}

func newLinker(in *inserter) *linker {
	n := len(in.live)
	l := &linker{
		in:       in,
		states:   make(map[int64]driver.State),
		finished: make(map[int64]bool),
		seen:     make([]bool, n),
		reason:   make([]string, n),
		pending:  make([]int, n),
		lists:    make([]jsonText, n),
	}
	jobs, batches := make(map[int64]bool), make(map[int64]bool)
	for _, i := range in.live {
		p := &in.jobs[i]
		for _, par := range p.Parents {
			if par.ID > 0 && !jobs[par.ID] {
				jobs[par.ID] = true
				l.parents = append(l.parents, par.ID)
			}
		}
		if p.AfterBatch != 0 && !batches[p.AfterBatch] {
			batches[p.AfterBatch] = true
			l.batches = append(l.batches, p.AfterBatch)
		}
	}
	slices.Sort(l.parents)
	slices.Sort(l.batches)
	return l
}

func (l *linker) fetch(ctx context.Context, q querier) error {
	b := make([]byte, 0, 512)
	if len(l.parents) > 0 {
		b = appendSQL(b, l.in.s.q.lockParents, l.parents, l.parents)
	}
	if len(l.batches) > 0 {
		b = appendSQL(b, l.in.s.q.afterBatches, l.batches)
	}
	b = append(b, l.in.s.q.linkedNow...)
	rows, err := q.QueryContext(ctx, string(b))
	if err != nil {
		return wrap("insert", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			tag, state string
			id         int64
			now        stamp
		)
		if err := rows.Scan(&tag, &id, &state, &now); err != nil {
			return wrap("insert", err)
		}
		switch tag {
		case "j", "a":
			if _, ok := l.states[id]; !ok {
				l.states[id] = driver.State(state)
			}
		case "b":
			l.finished[id] = state != ""
		case "n":
			l.in.now = now.Time
		}
	}
	if err := rows.Err(); err != nil {
		return wrap("insert", err)
	}
	var missing []int64
	for _, id := range l.parents {
		if _, ok := l.states[id]; !ok {
			missing = append(missing, id)
		}
	}
	if missing != nil {
		if err := l.archived(ctx, q, missing); err != nil {
			return err
		}
	}
	for _, id := range l.batches {
		if _, ok := l.finished[id]; !ok {
			return fmt.Errorf("%w: batch %d", driver.ErrNotFound, id)
		}
	}
	return nil
}

func (l *linker) archived(ctx context.Context, q querier, ids []int64) error {
	rows, err := q.QueryContext(ctx, render(l.in.s.q.archivedParents, ids))
	if err != nil {
		return wrap("insert", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			id    int64
			state string
		)
		if err := rows.Scan(&id, &state); err != nil {
			return wrap("insert", err)
		}
		l.states[id] = driver.State(state)
	}
	if err := rows.Err(); err != nil {
		return wrap("insert", err)
	}
	for _, id := range ids {
		if _, ok := l.states[id]; !ok {
			return fmt.Errorf("%w: parent job %d", driver.ErrNotFound, id)
		}
	}
	return nil
}

func (l *linker) edge(par driver.Parent) edge {
	if par.ID > 0 {
		return edge{id: par.ID, mask: par.On, state: l.states[par.ID]}
	}
	in := l.in
	f := par.Index
	if in.first[f] >= 0 {
		f = in.first[f]
	}
	r := in.pos[f]
	if !in.won[r] {
		h := in.holders[string(in.jobs[f].UniqueKey)]
		return edge{id: h.id, mask: par.On, state: h.state}
	}
	l.eval(r)
	e := edge{id: in.ids[r], mask: par.On, state: driver.Awaiting}
	if l.reason[r] != "" {
		e.state = driver.Deleted
	}
	return e
}

func (l *linker) eval(r int) {
	if l.seen[r] {
		return
	}
	l.seen[r] = true
	in := l.in
	p := &in.jobs[in.live[r]]
	var edges []edge
	for _, par := range p.Parents {
		e := l.edge(par)
		if k := slices.IndexFunc(edges, func(x edge) bool { return x.id == e.id }); k >= 0 {
			edges[k].mask |= e.mask
			continue
		}
		edges = append(edges, e)
	}
	ids := make([]int64, len(edges))
	for k, e := range edges {
		ids[k] = e.id
		doom := !e.mask.Has(e.state) && (e.state == driver.Succeeded || e.state == driver.Deleted)
		if doom && l.reason[r] == "" {
			l.reason[r] = "parent " + strconv.FormatInt(e.id, 10) + " " + string(e.state)
		}
		resolved := doom || e.mask.Has(e.state)
		if !resolved {
			l.pending[r]++
		}
		l.deps = append(l.deps, dep{parent: e.id, job: in.ids[r], mask: e.mask, resolved: resolved})
	}
	if p.Parents != nil {
		l.lists[r] = encodeIDs(ids)
	}
	if p.AfterBatch != 0 {
		done := l.finished[p.AfterBatch]
		if !done {
			l.pending[r]++
		}
		l.deps = append(l.deps, dep{batch: true, parent: p.AfterBatch, job: in.ids[r], resolved: done})
	}
}

func (l *linker) rows() []string {
	in := l.in
	for r := range in.live {
		if in.won[r] {
			l.eval(r)
		}
	}
	var (
		alive   []int
		pending []int
		lists   []jsonText
	)
	doomed := &chunks{head: in.s.q.insertDoomed, max: in.s.budget}
	for r, i := range in.live {
		switch {
		case !in.won[r]:
		case l.reason[r] != "":
			in.res[i] = driver.Inserted{ID: in.ids[r], State: driver.Deleted}
			doomed.row()
			doomed.b = l.appendDoomed(doomed.b, r)
		default:
			alive = append(alive, i)
			pending = append(pending, l.pending[r])
			lists = append(lists, l.lists[r])
		}
	}
	stmts := in.jobRows(alive, pending, lists)
	stmts = append(stmts, doomed.done()...)
	if len(l.deps) > 0 {
		c := &chunks{head: in.s.q.insertDeps, max: in.s.budget}
		for _, d := range l.deps {
			c.row()
			c.b = appendSQL(c.b, "(?, ?, ?, ?, ?)", d.batch, d.parent, d.job, int16(d.mask), d.resolved)
		}
		stmts = append(stmts, c.done()...)
	}
	return stmts
}

func (l *linker) appendDoomed(b []byte, r int) []byte {
	in := l.in
	p := &in.jobs[in.live[r]]
	e := entry{state: driver.Deleted, reason: l.reason[r]}
	history := append(append([]byte{'['}, e.encode(in.now)...), ']')
	b = append(b, '(')
	b = strconv.AppendInt(b, in.ids[r], 10)
	b = append(b, ",'deleted',"...)
	b = appendString(b, p.Queue)
	b = append(b, ',')
	b = appendString(b, p.Kind)
	b = appendSQL(b, ", ?, 0, ?, 0, ?, ", p.Priority, p.MaxAttempts, millis(p.Timeout))
	b = appendRunAt(b, p)
	b = appendSQL(b, ", ?, ?, ", in.now, in.now)
	b = appendOptInt(b, p.BatchID)
	b = append(b, ',')
	b = appendOptInt(b, p.AfterBatch)
	b = append(b, ',')
	b = appendJSON(b, l.lists[r])
	b = append(b, ',')
	b = appendOptString(b, p.RecurringID)
	b = append(b, ',')
	b = appendOptString(b, p.LimitKey)
	b = append(b, ',')
	b = appendBytes(b, p.Args)
	b = append(b, ',')
	b = appendJSON(b, encodeMeta(p.Meta))
	b = append(b, ',')
	b = appendJSON(b, encodeStrings(p.Tags))
	b = append(b, ',')
	b = appendJSON(b, history)
	return append(b, ')')
}

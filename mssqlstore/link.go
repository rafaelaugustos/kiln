package mssqlstore

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strconv"

	"github.com/rafaelaugustos/kiln/driver"
)

const sqlLockParents = `SELECT 'j', j.id, j.state FROM OPENJSON(@parents) WITH (id BIGINT '$') v
JOIN {p}jobs j WITH (REPEATABLEREAD, ROWLOCK, FORCESEEK) ON j.id = v.id
UNION ALL
SELECT 'a', a.id, a.state FROM {p}archive a WHERE a.id IN (SELECT id FROM OPENJSON(@parents) WITH (id BIGINT '$'))
UNION ALL
SELECT 'b', b.id, CASE WHEN b.finished_at IS NULL THEN '' ELSE 'finished' END FROM {p}batches b
WHERE b.id IN (SELECT id FROM OPENJSON(@batches) WITH (id BIGINT '$'))`

const sqlArchivedParents = `SELECT a.id, a.state FROM OPENJSON(@ids) WITH (id BIGINT '$') v
JOIN {p}archive a WITH (REPEATABLEREAD, ROWLOCK, FORCESEEK) ON a.id = v.id`

type edge struct {
	id    int64
	mask  driver.Mask
	state driver.State
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
	lists    [][]byte
	deps     table
	doomed   int
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
		lists:    make([][]byte, n),
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
	if len(l.parents)+len(l.batches) == 0 {
		return nil
	}
	rows, err := q.QueryContext(ctx, l.in.s.q.lockParents, sql.Named("parents", idList(l.parents)),
		sql.Named("batches", idList(l.batches)))
	if err != nil {
		return wrap("insert", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			tag, state string
			id         int64
		)
		if err := rows.Scan(&tag, &id, &state); err != nil {
			return wrap("insert", err)
		}
		switch tag {
		case "j", "a":
			if _, ok := l.states[id]; !ok {
				l.states[id] = driver.State(state)
			}
		case "b":
			l.finished[id] = state != ""
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
	rows, err := q.QueryContext(ctx, l.in.s.q.archivedParents, sql.Named("ids", idList(ids)))
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
	e := edge{id: in.base + int64(r), mask: par.On, state: driver.Awaiting}
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
	id := in.base + int64(r)
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
		l.dep(false, e.id, id, e.mask, resolved)
	}
	if p.Parents != nil {
		l.lists[r] = encodeIDs(ids)
	}
	if p.AfterBatch != 0 {
		done := l.finished[p.AfterBatch]
		if !done {
			l.pending[r]++
		}
		l.dep(true, p.AfterBatch, id, 0, done)
	}
}

func (l *linker) dep(batch bool, parent, job int64, mask driver.Mask, resolved bool) {
	l.deps.row()
	l.deps.flag("b", batch)
	l.deps.int("p", parent)
	l.deps.int("j", job)
	l.deps.int("m", int64(mask))
	l.deps.flag("r", resolved)
}

func (l *linker) rows(jobs, deps *table) {
	in := l.in
	for r := range in.live {
		if in.won[r] {
			l.eval(r)
		}
	}
	for r, i := range in.live {
		switch {
		case !in.won[r]:
		case l.reason[r] != "":
			in.res[i] = driver.Inserted{ID: in.base + int64(r), State: driver.Deleted}
			in.row(jobs, r, i, 0, l.lists[r])
			e := entry{state: driver.Deleted, reason: l.reason[r]}
			jobs.int("x", 1)
			jobs.json("h", append(append([]byte{'['}, e.encode(in.now)...), ']'))
			l.doomed++
		default:
			in.row(jobs, r, i, l.pending[r], l.lists[r])
		}
	}
	*deps = l.deps
}

package pgstore

import (
	"context"
	"fmt"
	"slices"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/rafaelaugustos/kiln/driver"
)

type edge struct {
	id    int64
	mask  driver.Mask
	state driver.State
}

type deps struct {
	batch    []bool
	parent   []int64
	job      []int64
	mask     []int16
	resolved []bool
}

func (d *deps) add(batch bool, parent, job int64, mask driver.Mask, resolved bool) {
	d.batch = append(d.batch, batch)
	d.parent = append(d.parent, parent)
	d.job = append(d.job, job)
	d.mask = append(d.mask, int16(mask))
	d.resolved = append(d.resolved, resolved)
}

type linker struct {
	in       *inserter
	parents  []int64
	batches  []int64
	states   map[int64]driver.State
	finished map[int64]bool
	ids      []int64
	won      []bool
	pos      []int
	seen     []bool
	reason   []string
	pending  []int32
	lists    []string
	deps     deps
}

func (in *inserter) runLinked(ctx context.Context) error {
	n := len(in.live)
	l := &linker{
		in:       in,
		states:   make(map[int64]driver.State),
		finished: make(map[int64]bool),
		ids:      make([]int64, n),
		won:      make([]bool, n),
		pos:      make([]int, len(in.jobs)),
		seen:     make([]bool, n),
		reason:   make([]string, n),
		pending:  make([]int32, n),
		lists:    make([]string, n),
	}
	seenJob, seenBatch := make(map[int64]bool), make(map[int64]bool)
	for r, i := range in.live {
		l.pos[i] = r
		p := &in.jobs[i]
		for _, par := range p.Parents {
			if par.ID > 0 && !seenJob[par.ID] {
				seenJob[par.ID] = true
				l.parents = append(l.parents, par.ID)
			}
		}
		if p.AfterBatch != 0 && !seenBatch[p.AfterBatch] {
			seenBatch[p.AfterBatch] = true
			l.batches = append(l.batches, p.AfterBatch)
		}
	}
	if err := l.fetch(ctx); err != nil {
		return err
	}
	for r := range in.live {
		if l.won[r] {
			l.eval(r)
		}
	}
	if err := l.write(ctx); err != nil {
		return err
	}
	in.settle()
	return nil
}

func (l *linker) fetch(ctx context.Context) error {
	in := l.in
	b := &pgx.Batch{}
	if err := in.begin(ctx, b); err != nil {
		return err
	}
	if len(l.parents) > 0 {
		b.Queue(in.s.q.lockParents, l.parents).Query(l.scanStates)
		b.Queue(in.s.q.archivedParents, l.parents).Query(l.scanStates)
	}
	if len(l.batches) > 0 {
		b.Queue(in.s.q.afterBatches, l.batches).Query(func(rows pgx.Rows) error {
			var (
				id   int64
				done bool
			)
			for rows.Next() {
				if err := rows.Scan(&id, &done); err != nil {
					return err
				}
				l.finished[id] = done
			}
			return rows.Err()
		})
	}
	b.Queue(in.s.q.missingLinks, l.parents, l.batches)
	ukeys := make([][]byte, len(in.live))
	var ufors []int64
	for r, i := range in.live {
		p := &in.jobs[i]
		ukeys[r] = p.UniqueKey
		if p.UniqueFor > 0 && len(p.UniqueKey) > 0 {
			put(&ufors, len(in.live), r, max(micros(p.UniqueFor), 1))
		}
	}
	b.Queue(in.s.q.allocate, ukeys, ufors).Query(func(rows pgx.Rows) error {
		for r := 0; rows.Next(); r++ {
			if err := rows.Scan(&l.ids[r], &l.won[r]); err != nil {
				return err
			}
		}
		return rows.Err()
	})
	if len(in.keys) > 0 {
		b.Queue(in.s.q.holders, in.keys).Query(in.scanHolders)
	}
	if err := in.send(ctx, b); err != nil {
		return err
	}
	for _, id := range l.parents {
		if _, ok := l.states[id]; !ok {
			return fmt.Errorf("%w: parent job %d", driver.ErrNotFound, id)
		}
	}
	for _, id := range l.batches {
		if _, ok := l.finished[id]; !ok {
			return fmt.Errorf("%w: batch %d", driver.ErrNotFound, id)
		}
	}
	return nil
}

func (l *linker) scanStates(rows pgx.Rows) error {
	var (
		id    int64
		state string
	)
	for rows.Next() {
		if err := rows.Scan(&id, &state); err != nil {
			return err
		}
		if _, ok := l.states[id]; !ok {
			l.states[id] = driver.State(state)
		}
	}
	return rows.Err()
}

func (l *linker) edge(par driver.Parent) edge {
	if par.ID > 0 {
		return edge{id: par.ID, mask: par.On, state: l.states[par.ID]}
	}
	f := par.Index
	if l.in.first[f] >= 0 {
		f = l.in.first[f]
	}
	q := l.pos[f]
	if !l.won[q] {
		h := l.in.holders[string(l.in.jobs[f].UniqueKey)]
		return edge{id: h.id, mask: par.On, state: h.state}
	}
	l.eval(q)
	e := edge{id: l.ids[q], mask: par.On, state: driver.Awaiting}
	if l.reason[q] != "" {
		e.state = driver.Deleted
	}
	return e
}

func (l *linker) eval(r int) {
	if l.seen[r] {
		return
	}
	l.seen[r] = true
	p := &l.in.jobs[l.in.live[r]]
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
		l.deps.add(false, e.id, l.ids[r], e.mask, resolved)
	}
	if p.Parents != nil {
		l.lists[r] = intArray(ids)
	}
	if p.AfterBatch != 0 {
		done := l.finished[p.AfterBatch]
		if !done {
			l.pending[r]++
		}
		l.deps.add(true, p.AfterBatch, l.ids[r], 0, done)
	}
}

func (l *linker) write(ctx context.Context) error {
	in := l.in
	var (
		alive, doomed         []int
		aliveIDs, doomedIDs   []int64
		pending               []int32
		aliveLists, doomLists []string
		reasons               []string
	)
	for r, i := range in.live {
		switch {
		case !l.won[r]:
		case l.reason[r] != "":
			doomed = append(doomed, i)
			doomedIDs = append(doomedIDs, l.ids[r])
			doomLists = append(doomLists, l.lists[r])
			reasons = append(reasons, l.reason[r])
			in.res[i] = driver.Inserted{ID: l.ids[r], State: driver.Deleted}
		default:
			alive = append(alive, i)
			aliveIDs = append(aliveIDs, l.ids[r])
			pending = append(pending, l.pending[r])
			aliveLists = append(aliveLists, l.lists[r])
		}
	}
	b := &pgx.Batch{}
	if len(alive) > 0 {
		c := in.columns(alive, aliveIDs, pending)
		c.parents = aliveLists
		b.Queue(in.s.q.insert, c.params(false)...).Query(in.scanInserted(alive))
	}
	if len(doomed) > 0 {
		c := in.columns(doomed, doomedIDs, nil)
		c.parents = doomLists
		b.Queue(in.s.q.insertDoomed, c.params(reasons)...)
	}
	if d := &l.deps; len(d.job) > 0 {
		b.Queue(in.s.q.insertDeps, d.batch, d.parent, d.job, d.mask, d.resolved)
	}
	in.queueLimits(b, alive)
	in.queueAttach(b, append(alive, doomed...))
	if in.conn != nil {
		b.Queue("commit")
	}
	if len(b.QueuedQueries) == 0 {
		return nil
	}
	return in.send(ctx, b)
}

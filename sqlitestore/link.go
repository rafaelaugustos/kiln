package sqlitestore

import (
	"context"
	"fmt"
	"slices"
	"strconv"

	"github.com/rafaelaugustos/kiln/driver"
)

const sqlParents = `SELECT id, state FROM {p}jobs WHERE id IN (SELECT value FROM json_each(?))
UNION ALL
SELECT id, state FROM {p}archive WHERE id IN (SELECT value FROM json_each(?))`

const sqlAfterBatches = `SELECT id, finished_at IS NOT NULL FROM {p}batches WHERE id IN (SELECT value FROM json_each(?))`

const (
	unseen = iota
	visiting
	visited
)

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
	mark     []uint8
	reason   []string
	pending  []int
	lists    [][]byte
	deps     []dep
}

func newLinker(in *inserter) *linker {
	n := len(in.live)
	l := &linker{
		in:       in,
		states:   make(map[int64]driver.State),
		finished: make(map[int64]bool),
		mark:     make([]uint8, n),
		reason:   make([]string, n),
		pending:  make([]int, n),
		lists:    make([][]byte, n),
	}
	for r, i := range in.live {
		if !in.won[r] {
			continue
		}
		p := &in.jobs[i]
		for _, par := range p.Parents {
			if par.ID > 0 && !slices.Contains(l.parents, par.ID) {
				l.parents = append(l.parents, par.ID)
			}
		}
		if p.AfterBatch != 0 && !slices.Contains(l.batches, p.AfterBatch) {
			l.batches = append(l.batches, p.AfterBatch)
		}
	}
	slices.Sort(l.parents)
	slices.Sort(l.batches)
	return l
}

func (l *linker) fetch(ctx context.Context, q querier) error {
	if len(l.parents) > 0 {
		list := idList(l.parents)
		rows, err := q.QueryContext(ctx, l.in.s.q.parents, list, list)
		err = each(rows, err, func() error {
			var (
				id    int64
				state string
			)
			if err := rows.Scan(&id, &state); err != nil {
				return err
			}
			l.states[id] = driver.State(state)
			return nil
		})
		if err != nil {
			return err
		}
		for _, id := range l.parents {
			if _, ok := l.states[id]; !ok {
				return fmt.Errorf("%w: parent job %d", driver.ErrNotFound, id)
			}
		}
	}
	if len(l.batches) > 0 {
		rows, err := q.QueryContext(ctx, l.in.s.q.afterBatches, idList(l.batches))
		err = each(rows, err, func() error {
			var (
				id   int64
				done bool
			)
			if err := rows.Scan(&id, &done); err != nil {
				return err
			}
			l.finished[id] = done
			return nil
		})
		if err != nil {
			return err
		}
		for _, id := range l.batches {
			if _, ok := l.finished[id]; !ok {
				return fmt.Errorf("%w: batch %d", driver.ErrNotFound, id)
			}
		}
	}
	return nil
}

func (l *linker) edge(par driver.Parent) (edge, error) {
	if par.ID > 0 {
		return edge{id: par.ID, mask: par.On, state: l.states[par.ID]}, nil
	}
	in := l.in
	f := par.Index
	if in.first[f] >= 0 {
		f = in.first[f]
	}
	r := in.pos[f]
	if !in.won[r] {
		h := in.holders[string(in.jobs[f].UniqueKey)]
		return edge{id: h.id, mask: par.On, state: h.state}, nil
	}
	if err := l.eval(r); err != nil {
		return edge{}, err
	}
	e := edge{id: in.ids[r], mask: par.On, state: driver.Awaiting}
	if l.reason[r] != "" {
		e.state = driver.Deleted
	}
	return e, nil
}

func (l *linker) eval(r int) error {
	switch l.mark[r] {
	case visiting:
		return fmt.Errorf("%w: dependency cycle through job %d", driver.ErrInvalid, l.in.live[r])
	case visited:
		return nil
	}
	l.mark[r] = visiting
	in := l.in
	p := &in.jobs[in.live[r]]
	var edges []edge
	for _, par := range p.Parents {
		e, err := l.edge(par)
		if err != nil {
			return err
		}
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
	l.mark[r] = visited
	return nil
}

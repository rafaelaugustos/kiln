package memstore

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

type item struct {
	p       *driver.InsertParams
	alias   int
	deps    []dep
	pending int
	doom    string
}

type plan struct {
	items []item
	res   []driver.Inserted
	order []int
}

func (s *Store) Insert(_ context.Context, jobs []driver.InsertParams) ([]driver.Inserted, error) {
	s.begin()
	defer s.end()
	return s.insert(jobs)
}

func (s *Store) insert(jobs []driver.InsertParams) ([]driver.Inserted, error) {
	p, err := s.prepare(jobs, nil)
	if err != nil {
		return nil, err
	}
	if err := s.resolve(p, nil); err != nil {
		return nil, err
	}
	s.apply(p, true)
	s.settle()
	for i := range p.res {
		r := &p.res[i]
		switch a := p.items[i].alias; {
		case a >= 0:
			r.State = p.res[a].State
		case !r.Duplicate:
			r.State = s.jobs[r.ID].state
		}
	}
	return p.res, nil
}

func (s *Store) prepare(jobs []driver.InsertParams, ov *overlay) (*plan, error) {
	p := &plan{items: make([]item, len(jobs)), res: make([]driver.Inserted, len(jobs))}
	if err := driver.CheckInsert(jobs); err != nil {
		return nil, err
	}
	var seen map[string]int
	for i := range jobs {
		p.items[i] = item{p: &jobs[i], alias: -1}
		if len(jobs[i].UniqueKey) == 0 {
			continue
		}
		key := string(jobs[i].UniqueKey)
		if first, ok := seen[key]; ok {
			p.items[i].alias = first
			p.res[i].Duplicate = true
			continue
		}
		if seen == nil {
			seen = make(map[string]int)
		}
		seen[key] = i
		if u := s.holder(key); u != nil {
			st, _ := s.state(u.job, ov)
			p.res[i] = driver.Inserted{ID: u.job, State: st, Duplicate: true}
		}
	}
	if err := p.sort(); err != nil {
		return nil, err
	}
	for i := range p.res {
		switch a := p.items[i].alias; {
		case a >= 0:
			p.res[i].ID = p.res[a].ID
		case !p.res[i].Duplicate:
			s.jobSeq++
			p.res[i].ID = s.jobSeq
		}
	}
	return p, nil
}

func (p *plan) sort() error {
	p.order = make([]int, 0, len(p.items))
	marks := make([]uint8, len(p.items))
	var visit func(i int) error
	visit = func(i int) error {
		switch marks[i] {
		case 1:
			return fmt.Errorf("%w: dependency cycle at index %d", driver.ErrInvalid, i)
		case 2:
			return nil
		}
		marks[i] = 1
		for _, pr := range p.items[i].p.Parents {
			if pr.ID != 0 {
				continue
			}
			if k := p.target(pr.Index); k >= 0 {
				if err := visit(k); err != nil {
					return err
				}
			}
		}
		marks[i] = 2
		p.order = append(p.order, i)
		return nil
	}
	for i := range p.items {
		if p.res[i].Duplicate {
			continue
		}
		if err := visit(i); err != nil {
			return err
		}
	}
	return nil
}

func (p *plan) target(k int) int {
	if a := p.items[k].alias; a >= 0 {
		k = a
	}
	if p.res[k].Duplicate {
		return -1
	}
	return k
}

func (s *Store) resolve(p *plan, ov *overlay) error {
	for _, i := range p.order {
		it := &p.items[i]
		it.deps, it.pending, it.doom = it.deps[:0], 0, ""
		for _, pr := range it.p.Parents {
			id, st, err := s.parent(p, pr, ov)
			if err != nil {
				return err
			}
			if slices.ContainsFunc(it.deps, func(d dep) bool { return d.parent == id }) {
				continue
			}
			d := dep{parent: id, on: pr.On, resolved: pr.On.Has(st)}
			if !d.resolved {
				it.pending++
				if it.doom == "" && st.Archived() {
					it.doom = fmt.Sprintf("parent %d %s", id, st)
				}
			}
			it.deps = append(it.deps, d)
		}
		if id := it.p.AfterBatch; id != 0 {
			v, ok := s.view(id, ov)
			if !ok {
				return fmt.Errorf("%w: batch %d", driver.ErrNotFound, id)
			}
			if !v.finished {
				it.pending++
			}
			it.deps = append(it.deps, dep{parent: id, batch: true, resolved: v.finished})
		}
		if id := it.p.BatchID; id != 0 {
			v, ok := s.view(id, ov)
			if !ok {
				return fmt.Errorf("%w: batch %d", driver.ErrNotFound, id)
			}
			if v.finished || v.sealed && v.live == 0 {
				return fmt.Errorf("%w: batch %d", driver.ErrClosed, id)
			}
		}
		p.res[i].State = s.initial(it)
	}
	return nil
}

func (s *Store) parent(p *plan, pr driver.Parent, ov *overlay) (int64, driver.State, error) {
	id := pr.ID
	if id == 0 {
		k := pr.Index
		if a := p.items[k].alias; a >= 0 {
			k = a
		}
		if r := p.res[k]; !r.Duplicate {
			return r.ID, r.State, nil
		}
		id = p.res[k].ID
	}
	st, ok := s.state(id, ov)
	if !ok {
		return 0, "", fmt.Errorf("%w: parent %d", driver.ErrNotFound, id)
	}
	return id, st, nil
}

func (s *Store) state(id int64, ov *overlay) (driver.State, bool) {
	if ov != nil {
		if st, ok := ov.jobs[id]; ok {
			return st, true
		}
	}
	if j := s.jobs[id]; j != nil {
		return j.state, true
	}
	return "", false
}

func (s *Store) initial(it *item) driver.State {
	switch {
	case it.doom != "":
		return driver.Deleted
	case it.pending > 0:
		return driver.Awaiting
	case runAt(it.p, s.now).After(s.now):
		return driver.Scheduled
	case it.p.LimitKey != "":
		return driver.Throttled
	}
	return driver.Enqueued
}

func runAt(p *driver.InsertParams, now time.Time) time.Time {
	if !p.RunAt.IsZero() {
		return p.RunAt
	}
	return now.Add(max(p.Delay, 0))
}

func (s *Store) apply(p *plan, admit bool) {
	for _, i := range p.order {
		it := &p.items[i]
		ps := it.p
		j := &job{
			id:          p.res[i].ID,
			state:       p.res[i].State,
			queue:       ps.Queue,
			kind:        ps.Kind,
			args:        slices.Clone(ps.Args),
			meta:        maps.Clone(ps.Meta),
			tags:        slices.Clone(ps.Tags),
			priority:    ps.Priority,
			maxAttempts: ps.MaxAttempts,
			timeout:     ps.Timeout,
			runAt:       runAt(ps, s.now),
			createdAt:   s.now,
			batch:       ps.BatchID,
			afterBatch:  ps.AfterBatch,
			recurring:   ps.RecurringID,
			unique:      string(ps.UniqueKey),
			uniqueFor:   ps.UniqueFor,
			limit:       ps.LimitKey,
			deps:        it.deps,
			pending:     it.pending,
		}
		s.jobs[j.id] = j
		for _, d := range j.deps {
			switch {
			case !d.batch:
				pj := s.jobs[d.parent]
				pj.children = append(pj.children, j.id)
			case !d.resolved:
				b := s.batches[d.parent]
				b.dependents = append(b.dependents, j.id)
			}
		}
		if j.unique != "" {
			s.hold(j)
		}
		if j.limit != "" {
			l := s.limitFor(j.limit)
			l.refs++
			if r := ruleOf(ps); l.rule != r {
				l.rule = r
				if admit {
					s.admitting(j.limit)
				}
			}
		}
		if j.batch != 0 {
			s.batches[j.batch].total++
		}
		if j.state == driver.Deleted {
			j.finalizedAt = s.now
			s.log(j, driver.Entry{State: driver.Deleted, Reason: it.doom})
		}
		s.index(j)
		if j.state == driver.Throttled && admit {
			s.admitting(j.limit)
		}
	}
}

func cloneParams(p driver.InsertParams) driver.InsertParams {
	p.Args = slices.Clone(p.Args)
	p.Meta = maps.Clone(p.Meta)
	p.Tags = slices.Clone(p.Tags)
	p.UniqueKey = slices.Clone(p.UniqueKey)
	p.Parents = slices.Clone(p.Parents)
	return p
}

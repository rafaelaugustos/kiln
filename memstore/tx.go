package memstore

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/rafaelaugustos/kiln/driver"
)

var errDone = errors.New("kiln: transaction already committed or rolled back")

type Tx struct {
	s    *Store
	ov   overlay
	ops  []txop
	keys []string
	done bool
}

type txop struct {
	open *batch
	seal int64
	plan *plan
}

type overlay struct {
	jobs    map[int64]driver.State
	batches map[int64]*bview
}

type bview struct {
	sealed   bool
	finished bool
	live     int
}

func newOverlay() overlay {
	return overlay{jobs: make(map[int64]driver.State), batches: make(map[int64]*bview)}
}

func (s *Store) Begin() *Tx {
	return &Tx{s: s, ov: newOverlay()}
}

func (s *Store) InTx(_ context.Context, fn func(driver.Writer) error) error {
	tx := s.Begin()
	defer tx.Rollback()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func (t *Tx) Insert(_ context.Context, jobs []driver.InsertParams) ([]driver.Inserted, error) {
	s := t.s
	s.begin()
	defer s.end()
	if t.done {
		return nil, errDone
	}
	jobs = slices.Clone(jobs)
	for i := range jobs {
		jobs[i] = cloneParams(jobs[i])
	}
	p, err := s.prepare(jobs, &t.ov)
	if err != nil {
		return nil, err
	}
	if err := s.resolve(p, &t.ov); err != nil {
		return nil, err
	}
	t.ov.record(s, p)
	t.reserve(p)
	t.ops = append(t.ops, txop{plan: p})
	res := slices.Clone(p.res)
	for i := range res {
		if a := p.items[i].alias; a >= 0 {
			res[i].State = res[a].State
		}
	}
	return res, nil
}

func (t *Tx) OpenBatch(_ context.Context, nb driver.NewBatch) (int64, error) {
	s := t.s
	s.begin()
	defer s.end()
	if t.done {
		return 0, errDone
	}
	s.batchSeq++
	b := &batch{id: s.batchSeq, desc: nb.Description, meta: maps.Clone(nb.Meta), created: s.now}
	t.ov.batches[b.id] = &bview{}
	t.ops = append(t.ops, txop{open: b})
	return b.id, nil
}

func (t *Tx) SealBatch(_ context.Context, id int64) error {
	s := t.s
	s.begin()
	defer s.end()
	if t.done {
		return errDone
	}
	v := t.ov.batch(s, id)
	if v == nil {
		return fmt.Errorf("%w: batch %d", driver.ErrNotFound, id)
	}
	v.seal()
	t.ops = append(t.ops, txop{seal: id})
	return nil
}

func (t *Tx) Commit() error {
	s := t.s
	s.begin()
	defer s.end()
	if t.done {
		return errDone
	}
	t.done = true
	ov := newOverlay()
	for _, op := range t.ops {
		if err := op.check(s, &ov); err != nil {
			t.release()
			return err
		}
	}
	for _, op := range t.ops {
		switch {
		case op.open != nil:
			s.batches[op.open.id] = op.open
		case op.seal != 0:
			s.seal(s.batches[op.seal])
		default:
			s.apply(op.plan, false)
		}
	}
	s.settle()
	return nil
}

func (t *Tx) Rollback() error {
	s := t.s
	s.begin()
	defer s.end()
	if t.done {
		return errDone
	}
	t.done = true
	t.release()
	return nil
}

func (t *Tx) reserve(p *plan) {
	for _, i := range p.order {
		ps := p.items[i].p
		if len(ps.UniqueKey) == 0 {
			continue
		}
		u := &uniq{job: p.res[i].ID, tx: t}
		if ps.UniqueFor > 0 {
			u.expires = t.s.now.Add(ps.UniqueFor)
		}
		k := string(ps.UniqueKey)
		t.s.uniques[k] = u
		t.keys = append(t.keys, k)
	}
}

func (t *Tx) release() {
	for _, k := range t.keys {
		if u := t.s.uniques[k]; u != nil && u.tx == t {
			delete(t.s.uniques, k)
		}
	}
}

func (op txop) check(s *Store, ov *overlay) error {
	switch {
	case op.open != nil:
		ov.batches[op.open.id] = &bview{}
	case op.seal != 0:
		v := ov.batch(s, op.seal)
		if v == nil {
			return fmt.Errorf("%w: batch %d", driver.ErrNotFound, op.seal)
		}
		v.seal()
	default:
		if err := s.resolve(op.plan, ov); err != nil {
			return err
		}
		ov.record(s, op.plan)
	}
	return nil
}

func (ov *overlay) record(s *Store, p *plan) {
	for _, i := range p.order {
		r := p.res[i]
		ov.jobs[r.ID] = r.State
		if b := p.items[i].p.BatchID; b != 0 && !r.State.Archived() {
			ov.batch(s, b).live++
		}
	}
}

func (ov *overlay) batch(s *Store, id int64) *bview {
	if v, ok := ov.batches[id]; ok {
		return v
	}
	b := s.batches[id]
	if b == nil {
		return nil
	}
	v := b.view()
	ov.batches[id] = &v
	return &v
}

func (v *bview) seal() {
	if !v.sealed {
		v.sealed = true
		v.finished = v.live == 0
	}
}

func (s *Store) view(id int64, ov *overlay) (bview, bool) {
	if ov != nil {
		if v := ov.batch(s, id); v != nil {
			return *v, true
		}
		return bview{}, false
	}
	if b := s.batches[id]; b != nil {
		return b.view(), true
	}
	return bview{}, false
}

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

// Tx is a transaction on a [Store], and a [driver.TxWriter]. Each write is checked when it is
// made, against the store and the earlier writes of the Tx, and checked again by Commit, which
// applies them all or none. The unique keys of the jobs it inserts are taken at once: while the Tx
// is open, an insert elsewhere with one of them is a duplicate of the Tx's job. Commit or Rollback
// finishes the Tx; any write, Commit or Rollback after that fails.
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

// Begin starts a transaction. The jobs and batches written through the [Tx] stay out of sight
// until [Tx.Commit] applies them together, and [Tx.Rollback] discards them.
func (s *Store) Begin() *Tx {
	return &Tx{s: s, ov: newOverlay()}
}

// InTx calls fn with a new [Tx] and commits it if fn returns nil. Otherwise it rolls the Tx back
// and returns fn's error.
func (s *Store) InTx(_ context.Context, fn func(driver.Writer) error) error {
	tx := s.Begin()
	defer tx.Rollback()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

// Insert checks jobs and returns the ids they will have, but the jobs exist only once
// [Tx.Commit] has applied them.
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

// OpenBatch returns the id of a new batch, which exists once the transaction commits. Jobs
// inserted through the Tx can join it before then.
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

// SealBatch seals the batch id, whether the Tx opened it or not, when the transaction commits. It
// fails with [driver.ErrNotFound] for a batch that does not exist.
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

// Commit checks the writes of the transaction again and applies them all, or none if a parent
// job or a batch they rely on was pruned, or a batch closed, in the meantime. Like the writes of
// the store, it then admits throttled jobs and wakes subscribers. The Tx is finished even when
// Commit fails.
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
			s.apply(op.plan, true)
		}
	}
	s.settle()
	return nil
}

// Notify does nothing and returns nil: [Tx.Commit] has already admitted throttled jobs and woken
// subscribers. It makes Tx a [driver.TxWriter].
func (t *Tx) Notify(context.Context) error {
	return nil
}

// Rollback discards the writes of the transaction and releases the unique keys its inserts took.
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

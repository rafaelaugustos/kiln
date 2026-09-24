package memstore

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

type batch struct {
	id         int64
	desc       string
	meta       map[string]string
	total      int64
	sealed     bool
	live       int
	counts     [nstates]int64
	created    time.Time
	finished   time.Time
	dependents []int64
}

func (s *Store) OpenBatch(_ context.Context, nb driver.NewBatch) (int64, error) {
	s.begin()
	defer s.end()
	s.batchSeq++
	s.batches[s.batchSeq] = &batch{id: s.batchSeq, desc: nb.Description, meta: maps.Clone(nb.Meta), created: s.now}
	return s.batchSeq, nil
}

func (s *Store) SealBatch(_ context.Context, id int64) error {
	s.begin()
	defer s.end()
	b := s.batches[id]
	if b == nil {
		return fmt.Errorf("%w: batch %d", driver.ErrNotFound, id)
	}
	s.seal(b)
	s.settle()
	return nil
}

func (s *Store) seal(b *batch) {
	b.sealed = true
	s.complete(b)
}

func (s *Store) complete(b *batch) {
	if b.sealed && b.live == 0 && b.finished.IsZero() {
		b.finished = s.now
		s.done = append(s.done, b)
	}
}

func (b *batch) members() int64 {
	var n int64
	for _, c := range b.counts {
		n += c
	}
	return n
}

func (b *batch) view() bview {
	return bview{sealed: b.sealed, finished: !b.finished.IsZero(), live: b.live}
}

func (b *batch) info() driver.Batch {
	counts := make(map[driver.State]int64)
	for i, n := range b.counts {
		if n > 0 {
			counts[driver.States[i]] = n
		}
	}
	return driver.Batch{
		ID:          b.id,
		Description: b.desc,
		Meta:        maps.Clone(b.meta),
		Total:       b.total,
		Sealed:      b.sealed,
		Counts:      counts,
		CreatedAt:   b.created,
		FinishedAt:  b.finished,
	}
}

func (s *Store) Batch(_ context.Context, id int64) (driver.Batch, error) {
	s.begin()
	defer s.end()
	b := s.batches[id]
	if b == nil {
		return driver.Batch{}, fmt.Errorf("%w: batch %d", driver.ErrNotFound, id)
	}
	return b.info(), nil
}

func (s *Store) Batches(_ context.Context, q driver.BatchQuery) (driver.BatchPage, error) {
	limit := min(limitOr(q.Limit, 20), 500)
	var after int64
	if q.Cursor != "" {
		n, err := strconv.ParseInt(q.Cursor, 10, 64)
		if err != nil {
			return driver.BatchPage{}, fmt.Errorf("%w: cursor %q", driver.ErrInvalid, q.Cursor)
		}
		after = n
	}
	s.begin()
	defer s.end()
	var bs []*batch
	for id, b := range s.batches {
		if after == 0 || id < after {
			bs = append(bs, b)
		}
	}
	slices.SortFunc(bs, func(a, b *batch) int { return cmp.Compare(b.id, a.id) })
	var p driver.BatchPage
	if len(bs) > limit {
		bs = bs[:limit]
		p.Next = strconv.FormatInt(bs[limit-1].id, 10)
	}
	for _, b := range bs {
		p.Batches = append(p.Batches, b.info())
	}
	return p, nil
}

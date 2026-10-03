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
	parent     int64
	desc       string
	meta       map[string]string
	total      int64
	sealed     bool
	live       int
	nested     int64
	unfinished int
	counts     [nstates]int64
	created    time.Time
	finished   time.Time
	dependents []int64
}

// OpenBatch creates an unsealed batch and returns its id. With nb.Parent set it nests the batch
// in that one, or fails with [driver.ErrNotFound] or [driver.ErrClosed] as [driver.Batch]
// describes.
func (s *Store) OpenBatch(_ context.Context, nb driver.NewBatch) (int64, error) {
	s.begin()
	defer s.end()
	if nb.Parent > 0 {
		if err := s.joinable(nb.Parent, nil); err != nil {
			return 0, err
		}
	}
	b := s.newBatch(nb)
	s.add(b)
	return b.id, nil
}

func (s *Store) newBatch(nb driver.NewBatch) *batch {
	s.batchSeq++
	return &batch{id: s.batchSeq, parent: max(nb.Parent, 0), desc: nb.Description, meta: maps.Clone(nb.Meta), created: s.now}
}

func (s *Store) add(b *batch) {
	s.batches[b.id] = b
	if p := s.batches[b.parent]; p != nil {
		p.nested++
		p.unfinished++
	}
}

func (s *Store) joinable(id int64, ov *overlay) error {
	v, ok := s.view(id, ov)
	switch {
	case !ok:
		return fmt.Errorf("%w: batch %d", driver.ErrNotFound, id)
	case v.closed():
		return fmt.Errorf("%w: batch %d", driver.ErrClosed, id)
	}
	return nil
}

// SealBatch seals the batch id, which finishes at once if none of its members is live and its
// nested batches have finished. It fails with [driver.ErrNotFound] for an unknown id.
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
	if !b.idle() {
		return
	}
	b.finished = s.now
	s.done = append(s.done, b)
	if p := s.batches[b.parent]; p != nil {
		p.unfinished--
		s.complete(p)
	}
}

func (b *batch) idle() bool {
	return b.sealed && b.live == 0 && b.unfinished == 0 && b.finished.IsZero()
}

func (b *batch) members() int64 {
	var n int64
	for _, c := range b.counts {
		n += c
	}
	return n
}

func (b *batch) view() bview {
	return bview{sealed: b.sealed, finished: !b.finished.IsZero(), live: b.live, unfinished: b.unfinished, parent: b.parent}
}

func (b *batch) info() driver.Batch {
	counts := make(map[driver.State]int64)
	for i, n := range b.counts {
		if n > 0 {
			counts[driver.States[i]] = n
		}
	}
	return driver.Batch{
		ID:             b.id,
		Description:    b.desc,
		Meta:           maps.Clone(b.meta),
		Total:          b.total,
		Sealed:         b.sealed,
		Counts:         counts,
		CreatedAt:      b.created,
		FinishedAt:     b.finished,
		Parent:         b.parent,
		Nested:         b.nested,
		NestedFinished: b.nested - int64(b.unfinished),
	}
}

// Batch returns the batch id with its members counted by state, or an error wrapping
// [driver.ErrNotFound].
func (s *Store) Batch(_ context.Context, id int64) (driver.Batch, error) {
	s.begin()
	defer s.end()
	b := s.batches[id]
	if b == nil {
		return driver.Batch{}, fmt.Errorf("%w: batch %d", driver.ErrNotFound, id)
	}
	return b.info(), nil
}

// Batches returns a page of batches, newest first, or of the batches nested directly in q.Parent
// when it is set.
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
		if (after == 0 || id < after) && (q.Parent <= 0 || b.parent == q.Parent) {
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

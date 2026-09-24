package memstore

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

func (s *Store) Due(_ context.Context, limit int) ([]driver.Recurring, time.Time, error) {
	s.begin()
	defer s.end()
	var due []*driver.Recurring
	for _, r := range s.recurring {
		if !r.Paused && !r.NextRunAt.IsZero() && !r.NextRunAt.After(s.now) {
			due = append(due, r)
		}
	}
	slices.SortFunc(due, func(a, b *driver.Recurring) int {
		return cmp.Or(a.NextRunAt.Compare(b.NextRunAt), strings.Compare(a.ID, b.ID))
	})
	due = due[:min(len(due), limitOr(limit, 100))]
	out := make([]driver.Recurring, len(due))
	for i, r := range due {
		out[i] = cloneRecurring(r)
	}
	return out, s.now, nil
}

func (s *Store) Fire(_ context.Context, f driver.Fire) ([]driver.Inserted, error) {
	s.begin()
	defer s.end()
	r := s.recurring[f.ID]
	if r == nil {
		return nil, fmt.Errorf("%w: recurring %q", driver.ErrNotFound, f.ID)
	}
	if r.Version != f.Version {
		return nil, fmt.Errorf("%w: recurring %q is at version %d", driver.ErrConflict, f.ID, r.Version)
	}
	res, err := s.insert(f.Jobs)
	if err != nil {
		return nil, err
	}
	r.NextRunAt, r.LastRunAt = f.NextRunAt, f.LastRunAt
	if len(res) > 0 {
		r.LastJobID = res[len(res)-1].ID
	}
	r.Version++
	r.UpdatedAt = s.now
	return res, nil
}

func (s *Store) Recurring(_ context.Context, id string) (driver.Recurring, error) {
	s.begin()
	defer s.end()
	r := s.recurring[id]
	if r == nil {
		return driver.Recurring{}, fmt.Errorf("%w: recurring %q", driver.ErrNotFound, id)
	}
	return cloneRecurring(r), nil
}

func (s *Store) PutRecurring(_ context.Context, r driver.Recurring) error {
	if r.ID == "" {
		return fmt.Errorf("%w: empty recurring id", driver.ErrInvalid)
	}
	s.begin()
	defer s.end()
	cur := s.recurring[r.ID]
	switch {
	case cur == nil && r.Version == 0:
		r.CreatedAt = s.now
	case cur != nil && cur.Version == r.Version:
		r.CreatedAt = cur.CreatedAt
	default:
		return fmt.Errorf("%w: recurring %q version %d", driver.ErrConflict, r.ID, r.Version)
	}
	r.Version++
	r.UpdatedAt = s.now
	r.Template = cloneParams(r.Template)
	s.recurring[r.ID] = &r
	return nil
}

func (s *Store) RemoveRecurring(_ context.Context, id string) error {
	s.begin()
	defer s.end()
	if s.recurring[id] == nil {
		return fmt.Errorf("%w: recurring %q", driver.ErrNotFound, id)
	}
	delete(s.recurring, id)
	return nil
}

func (s *Store) Recurrings(context.Context) ([]driver.Recurring, error) {
	s.begin()
	defer s.end()
	out := make([]driver.Recurring, 0, len(s.recurring))
	for _, r := range s.recurring {
		out = append(out, cloneRecurring(r))
	}
	slices.SortFunc(out, func(a, b driver.Recurring) int { return strings.Compare(a.ID, b.ID) })
	return out, nil
}

func cloneRecurring(r *driver.Recurring) driver.Recurring {
	c := *r
	c.Template = cloneParams(c.Template)
	return c
}

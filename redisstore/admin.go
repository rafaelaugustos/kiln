package redisstore

import (
	"context"
	"fmt"
	"slices"

	"github.com/rafaelaugustos/kiln/driver"
)

const filterChunk = 1000

type wireFilter struct {
	IDs []int64 `json:"ids,omitempty"`
	St  string  `json:"st,omitempty"`
	Q   string  `json:"q,omitempty"`
	K   string  `json:"k,omitempty"`
	B   int64   `json:"b,omitempty"`
	Rec string  `json:"rec,omitempty"`
	Off int     `json:"off"`
	N   int     `json:"n"`
}

func (s *Store) each(ctx context.Context, op string, f driver.Filter) (int, error) {
	if err := driver.CheckFilter(f); err != nil {
		return 0, err
	}
	w := wireFilter{St: string(f.State), Q: f.Queue, K: f.Kind, B: f.BatchID, Rec: f.RecurringID, N: filterChunk}
	var res struct {
		N    int  `json:"n"`
		Off  int  `json:"off"`
		More bool `json:"more"`
	}
	total := 0
	if len(f.IDs) > 0 {
		ids := slices.Clone(f.IDs)
		slices.Sort(ids)
		for chunk := range slices.Chunk(slices.Compact(ids), filterChunk) {
			w.IDs = chunk
			if err := s.call(ctx, op, w, &res); err != nil {
				return total, err
			}
			total += res.N
		}
		return total, nil
	}
	for {
		if err := s.call(ctx, op, w, &res); err != nil {
			return total, err
		}
		total += res.N
		if !res.More {
			return total, nil
		}
		w.Off = res.Off
	}
}

// Delete deletes the jobs that match f as [driver.Admin.Delete] describes. Jobs named by id are
// deleted in one script per 1000 ids, and a filter by state is applied in scripts of up to 1000
// jobs.
func (s *Store) Delete(ctx context.Context, f driver.Filter) (int, error) {
	return s.each(ctx, "delete", f)
}

// Requeue moves the jobs that match f back to their queues as [driver.Admin.Requeue] describes,
// in scripts of up to 1000 jobs.
func (s *Store) Requeue(ctx context.Context, f driver.Filter) (int, error) {
	return s.each(ctx, "requeue", f)
}

// PauseQueue pauses or resumes the queue called name and publishes [driver.QueueChanged]. An
// empty name is [driver.ErrInvalid].
func (s *Store) PauseQueue(ctx context.Context, name string, paused bool) error {
	if name == "" {
		return fmt.Errorf("%w: empty queue", driver.ErrInvalid)
	}
	args := struct {
		Name   string `json:"name"`
		Paused bool   `json:"paused"`
	}{name, paused}
	return s.call(ctx, "pause", args, nil)
}

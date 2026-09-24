package memstore

import (
	"context"
	"fmt"
	"slices"

	"github.com/rafaelaugustos/kiln/driver"
)

func (s *Store) Delete(_ context.Context, f driver.Filter) (int, error) {
	if err := driver.CheckFilter(f); err != nil {
		return 0, err
	}
	s.begin()
	defer s.end()
	n := 0
	for _, j := range s.match(&f) {
		switch {
		case j.state == driver.Processing && j.cancel:
		case j.state == driver.Processing:
			j.cancel = true
			s.events = append(s.events, driver.Event{Kind: driver.CancelRequested, Queue: j.queue, ID: j.id})
			n++
		case !j.state.Archived():
			s.log(j, driver.Entry{State: driver.Deleted, Reason: "deleted"})
			s.move(j, driver.Deleted)
			n++
		}
	}
	s.settle()
	return n, nil
}

func (s *Store) Requeue(_ context.Context, f driver.Filter) (int, error) {
	if err := driver.CheckFilter(f); err != nil {
		return 0, err
	}
	s.begin()
	defer s.end()
	n := 0
	for _, j := range s.match(&f) {
		switch j.state {
		case driver.Failed, driver.Scheduled, driver.Succeeded, driver.Deleted:
		default:
			continue
		}
		if j.unique != "" && j.uniqueFor == 0 {
			if u := s.holder(j.unique); u != nil && u.job != j.id {
				continue
			}
			s.uniques[j.unique] = &uniq{job: j.id}
		}
		to := driver.Enqueued
		if j.limit != "" {
			to = driver.Throttled
		}
		j.maxAttempts = max(j.maxAttempts, j.attempt+1)
		j.cancel = false
		j.runAt = s.now
		s.log(j, driver.Entry{State: to, Reason: "requeued"})
		s.enqueue(j)
		n++
	}
	s.settle()
	return n, nil
}

func (s *Store) PauseQueue(_ context.Context, name string, paused bool) error {
	if name == "" {
		return fmt.Errorf("%w: empty queue", driver.ErrInvalid)
	}
	s.begin()
	defer s.end()
	q := s.queue(name)
	q.paused, q.row = paused, true
	s.events = append(s.events, driver.Event{Kind: driver.QueueChanged, Queue: name})
	return nil
}

func (s *Store) match(f *driver.Filter) []*job {
	var js []*job
	if len(f.IDs) == 0 {
		for _, j := range byID(s.states[ord(f.State)]) {
			if matches(f, j) {
				js = append(js, j)
			}
		}
		return js
	}
	ids := slices.Clone(f.IDs)
	slices.Sort(ids)
	for _, id := range slices.Compact(ids) {
		if j := s.jobs[id]; j != nil && matches(f, j) {
			js = append(js, j)
		}
	}
	return js
}

func matches(f *driver.Filter, j *job) bool {
	return (f.State == "" || j.state == f.State) &&
		(f.Queue == "" || j.queue == f.Queue) &&
		(f.Kind == "" || j.kind == f.Kind) &&
		(f.BatchID == 0 || j.batch == f.BatchID) &&
		(f.RecurringID == "" || j.recurring == f.RecurringID)
}

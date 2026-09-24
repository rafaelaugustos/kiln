package memstore

import (
	"context"
	"encoding/json"
	"slices"

	"github.com/rafaelaugustos/kiln/driver"
)

func (s *Store) Finish(_ context.Context, server string, outs []driver.Outcome) ([]driver.Result, error) {
	res := make([]driver.Result, len(outs))
	s.begin()
	defer s.end()
	s.server = server
	for i := range outs {
		res[i] = s.finish(&outs[i])
	}
	s.settle()
	return res, nil
}

func (s *Store) finish(o *driver.Outcome) driver.Result {
	j := s.jobs[o.ID]
	if j == nil || j.state != driver.Processing || j.claim != o.Claim {
		return driver.Stale
	}
	switch o.State {
	case driver.Succeeded, driver.Failed, driver.Deleted, driver.Scheduled, driver.Enqueued:
	default:
		return driver.Rejected
	}
	if len(o.Output) > 0 && !json.Valid(o.Output) {
		return driver.Rejected
	}
	to := o.State
	if to == driver.Scheduled && o.Delay <= 0 {
		to = driver.Enqueued
	}
	if to == driver.Enqueued && j.limit != "" {
		to = driver.Throttled
	}
	if j.cancel && o.State != driver.Succeeded {
		to = driver.Deleted
	}
	if o.Reason != "" {
		s.log(j, driver.Entry{State: to, Reason: o.Reason, Error: o.Error, Trace: o.Trace, Server: j.server})
	}
	if o.Refund {
		j.attempt = max(j.attempt-1, 0)
	}
	if o.Output != nil {
		j.output = slices.Clone(o.Output)
	}
	switch to {
	case driver.Scheduled:
		j.runAt = s.now.Add(o.Delay)
		s.move(j, to)
	case driver.Enqueued, driver.Throttled:
		j.runAt = s.now
		s.enqueue(j)
	default:
		s.move(j, to)
	}
	if o.State == driver.Scheduled && to != driver.Deleted && !o.Refund {
		s.bump(driver.Scheduled)
	}
	return driver.Applied
}

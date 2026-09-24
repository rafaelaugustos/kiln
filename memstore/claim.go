package memstore

import (
	"context"

	"github.com/rafaelaugustos/kiln/driver"
)

func (s *Store) Claim(_ context.Context, q driver.ClaimQuery) ([]driver.Job, error) {
	if q.Limit <= 0 {
		return nil, nil
	}
	s.begin()
	defer s.end()
	var out []driver.Job
	for _, name := range q.Queues {
		qu := s.queues[name]
		if qu == nil || qu.paused {
			continue
		}
		for len(out) < q.Limit {
			j := qu.next(q.Kinds)
			if j == nil {
				break
			}
			j.attempt++
			j.claim++
			j.server = q.Server
			j.attemptedAt = s.now
			j.cancel = false
			s.move(j, driver.Processing)
			out = append(out, j.view())
		}
	}
	return out, nil
}

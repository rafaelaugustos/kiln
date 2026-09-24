package memstore

import (
	"cmp"
	"context"
	"slices"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

func (s *Store) Promote(_ context.Context, limit int) (driver.Promoted, error) {
	limit = limitOr(limit, 1000)
	s.begin()
	defer s.end()
	var p driver.Promoted
	for p.Count < limit {
		j := s.due.peek()
		if j == nil || j.runAt.After(s.now) {
			break
		}
		s.enqueue(j)
		p.Count++
	}
	s.settle()
	p.Queues = slices.Sorted(slices.Values(s.ready))
	if j := s.due.peek(); j != nil {
		p.Next = max(j.runAt.Sub(s.now), 1)
	}
	return p, nil
}

func (s *Store) Sweep(_ context.Context, limit int) (int, error) {
	limit = limitOr(limit, 1000)
	s.begin()
	defer s.end()
	n := 0
	for _, j := range byID(s.states[ord(driver.Awaiting)]) {
		if n == limit {
			break
		}
		if j.state == driver.Awaiting && s.repair(j) {
			n++
		}
	}
	for _, b := range s.batches {
		if n == limit {
			break
		}
		if b.sealed && b.live == 0 && b.finished.IsZero() {
			s.complete(b)
			n++
		}
	}
	s.settle()

	active := make(map[string]int, len(s.limits))
	for _, st := range [...]driver.State{driver.Enqueued, driver.Processing} {
		for _, j := range s.states[ord(st)] {
			if j.limit != "" {
				active[j.limit]++
			}
		}
	}
	for key, l := range s.limits {
		if l.active != active[key] {
			l.active = active[key]
			n++
		}
		n += s.admitKey(key)
	}
	s.settle()
	return n, nil
}

func (s *Store) Prune(_ context.Context, p driver.PruneParams) (int, error) {
	limit := limitOr(p.Limit, 1000)
	servers := cmp.Or(p.Servers, time.Hour)
	stats := cmp.Or(p.Stats, 14*24*time.Hour)
	s.begin()
	defer s.end()

	n := s.pruneJobs(limit, p.Retention)
	s.settle()

	cutoff := s.now.Add(-servers)
	n += evict(s.servers, limit, func(_ string, v driver.ServerInfo) bool {
		return v.HeartbeatAt.Before(cutoff)
	})
	oldest := s.now.Add(-stats).Unix()
	n += evict(s.stats, limit, func(k bucket, _ *counters) bool {
		return k.at < oldest
	})
	n += evict(s.uniques, limit, func(_ string, u *uniq) bool {
		return u.tx == nil && !u.expires.IsZero() && !u.expires.After(s.now)
	})
	n += evict(s.limits, limit, func(_ string, l *throttle) bool {
		return l.refs == 0
	})
	n += evict(s.batches, limit, func(_ int64, b *batch) bool {
		return !b.finished.IsZero() && b.members() == 0
	})
	return n, nil
}

func (s *Store) pruneJobs(limit int, r driver.Retention) int {
	expired := func(js []*job, st driver.State, keep time.Duration) []*job {
		if keep < 0 {
			return js
		}
		cutoff := s.now.Add(-keep)
		for _, j := range s.states[ord(st)] {
			if j.finalizedAt.Before(cutoff) {
				js = append(js, j)
			}
		}
		return js
	}
	archived := expired(expired(nil, driver.Succeeded, r.Succeeded), driver.Deleted, r.Deleted)
	failed := expired(nil, driver.Failed, r.Failed)
	n := 0
	for _, js := range [...][]*job{archived, failed} {
		slices.SortFunc(js, func(a, b *job) int {
			return cmp.Or(a.finalizedAt.Compare(b.finalizedAt), cmp.Compare(a.id, b.id))
		})
		for _, j := range js[:min(len(js), limit)] {
			n += s.drop(j)
		}
	}
	return n
}

func (s *Store) drop(j *job) int {
	s.unindex(j)
	delete(s.jobs, j.id)
	if j.limit != "" {
		s.limits[j.limit].refs--
	}
	for _, id := range j.children {
		s.follow(s.jobs[id], j.id, driver.Deleted, "pruned")
	}
	for _, d := range j.deps {
		if d.batch {
			if b := s.batches[d.parent]; b != nil {
				b.dependents = slices.DeleteFunc(b.dependents, func(id int64) bool { return id == j.id })
			}
		} else if p := s.jobs[d.parent]; p != nil {
			p.children = slices.DeleteFunc(p.children, func(id int64) bool { return id == j.id })
		}
	}
	if j.batch != 0 {
		s.complete(s.batches[j.batch])
	}
	return 1 + len(j.deps)
}

func evict[K comparable, V any](m map[K]V, limit int, dead func(K, V) bool) int {
	n := 0
	for k, v := range m {
		if n == limit {
			break
		}
		if dead(k, v) {
			delete(m, k)
			n++
		}
	}
	return n
}

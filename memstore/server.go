package memstore

import (
	"cmp"
	"context"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

func (s *Store) Heartbeat(_ context.Context, info driver.ServerInfo) (driver.Directives, error) {
	s.begin()
	defer s.end()
	if info.StartedAt.IsZero() {
		info.StartedAt = s.now
		if old, ok := s.servers[info.ID]; ok {
			info.StartedAt = old.StartedAt
		}
	}
	info.HeartbeatAt = s.now
	info.Queues = slices.Clone(info.Queues)
	info.Kinds = slices.Clone(info.Kinds)
	s.servers[info.ID] = info

	var d driver.Directives
	for _, j := range s.running[info.ID] {
		d.Leases = append(d.Leases, driver.Lease{
			Ref:    driver.Ref{ID: j.id, Claim: j.claim},
			Cancel: j.cancel,
			Age:    s.now.Sub(j.attemptedAt),
		})
	}
	slices.SortFunc(d.Leases, func(a, b driver.Lease) int { return cmp.Compare(a.ID, b.ID) })
	for name, q := range s.queues {
		if q.paused {
			d.Paused = append(d.Paused, name)
		}
	}
	slices.Sort(d.Paused)
	return d, nil
}

func (s *Store) Unregister(_ context.Context, server string) error {
	s.begin()
	defer s.end()
	delete(s.servers, server)
	return nil
}

func (s *Store) SetMeta(_ context.Context, ref driver.Ref, meta map[string]string) error {
	s.begin()
	defer s.end()
	j := s.jobs[ref.ID]
	if j == nil || j.state != driver.Processing || j.claim != ref.Claim {
		return driver.ErrLost
	}
	if j.meta == nil {
		j.meta = make(map[string]string, len(meta))
	}
	maps.Copy(j.meta, meta)
	return nil
}

func (s *Store) Orphans(_ context.Context, deadAfter time.Duration, limit int) ([]driver.Orphan, error) {
	s.begin()
	defer s.end()
	cutoff := s.now.Add(-deadAfter)
	var out []driver.Orphan
	for server, m := range s.running {
		if info, ok := s.servers[server]; ok && !info.HeartbeatAt.Before(cutoff) {
			continue
		}
		for _, j := range m {
			out = append(out, driver.Orphan{
				Ref:         driver.Ref{ID: j.id, Claim: j.claim},
				Kind:        j.kind,
				Queue:       j.queue,
				Attempt:     j.attempt,
				MaxAttempts: j.maxAttempts,
				Server:      j.server,
				Cancel:      j.cancel,
			})
		}
	}
	slices.SortFunc(out, func(a, b driver.Orphan) int { return cmp.Compare(a.ID, b.ID) })
	return out[:min(len(out), limitOr(limit, 1000))], nil
}

func (s *Store) Servers(context.Context) ([]driver.ServerInfo, error) {
	s.begin()
	defer s.end()
	out := make([]driver.ServerInfo, 0, len(s.servers))
	for _, info := range s.servers {
		info.Queues = slices.Clone(info.Queues)
		info.Kinds = slices.Clone(info.Kinds)
		out = append(out, info)
	}
	slices.SortFunc(out, func(a, b driver.ServerInfo) int { return strings.Compare(a.ID, b.ID) })
	return out, nil
}

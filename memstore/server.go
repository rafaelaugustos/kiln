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

// Heartbeat records that the server info describes is alive and returns the leases of its
// processing jobs and the paused queues; see [driver.Worker.Heartbeat].
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
			ID: j.id, Claim: j.claim,
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

// Unregister forgets the server. Jobs it still has processing become orphans at once.
func (s *Store) Unregister(_ context.Context, server string) error {
	s.begin()
	defer s.end()
	delete(s.servers, server)
	return nil
}

// SetMeta merges meta into the metadata of the job ref points to while the job is processing
// under ref's claim, and fails with [driver.ErrLost] otherwise.
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

// Orphans returns up to limit processing jobs whose server is unknown or has not sent a heartbeat
// for deadAfter. It changes nothing.
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
				ID: j.id, Claim: j.claim,
				Kind:        j.kind,
				Queue:       j.queue,
				Attempt:     j.attempt,
				MaxAttempts: j.maxAttempts,
				Server:      j.server,
				Cancel:      j.cancel,
				LastReason:  j.lastReason(),
			})
		}
	}
	slices.SortFunc(out, func(a, b driver.Orphan) int { return cmp.Compare(a.ID, b.ID) })
	return out[:min(len(out), max(limit, 1))], nil
}

// Servers returns the registered servers, ordered by id.
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

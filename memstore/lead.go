package memstore

import (
	"context"
	"time"
)

type lease struct {
	holder   string
	expires  time.Time
	acquired time.Time
}

// Lead acquires or renews the lease called name for holder, as [driver.Coordinator.Lead]
// describes.
func (s *Store) Lead(_ context.Context, name, holder string, ttl time.Duration) (time.Duration, bool, error) {
	s.begin()
	defer s.end()
	l := s.leases[name]
	switch {
	case l == nil || !l.expires.After(s.now):
		s.leases[name] = &lease{holder: holder, expires: s.now.Add(ttl), acquired: s.now}
		return 0, true, nil
	case l.holder == holder:
		l.expires = s.now.Add(ttl)
		return s.now.Sub(l.acquired), true, nil
	}
	return 0, false, nil
}

// Resign releases the lease called name if holder has it.
func (s *Store) Resign(_ context.Context, name, holder string) error {
	s.begin()
	defer s.end()
	if l := s.leases[name]; l != nil && l.holder == holder {
		delete(s.leases, name)
	}
	return nil
}

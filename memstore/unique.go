package memstore

import (
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

type uniq struct {
	job     int64
	expires time.Time
	tx      *Tx
}

func (s *Store) holder(key string) *uniq {
	u := s.uniques[key]
	if u == nil || !u.expires.IsZero() && !u.expires.After(s.now) {
		return nil
	}
	return u
}

func (s *Store) hold(j *job) {
	switch {
	case j.uniqueFor > 0:
		s.uniques[j.unique] = &uniq{job: j.id, expires: j.createdAt.Add(j.uniqueFor)}
	case j.state == driver.Deleted:
		s.release(j)
	default:
		s.uniques[j.unique] = &uniq{job: j.id}
	}
}

func (s *Store) release(j *job) {
	if j.unique == "" || j.uniqueFor > 0 {
		return
	}
	if u := s.uniques[j.unique]; u != nil && u.job == j.id {
		delete(s.uniques, j.unique)
	}
}

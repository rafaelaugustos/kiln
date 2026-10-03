package memstore

import (
	"maps"
	"slices"
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
		u := &uniq{job: j.id, expires: j.createdAt.Add(j.uniqueFor)}
		if t := s.uniques[j.unique]; t != nil && t.job == j.id {
			u.expires = t.expires
		}
		s.uniques[j.unique] = u
	case j.state == driver.Deleted:
		s.release(j)
	default:
		s.uniques[j.unique] = &uniq{job: j.id}
	}
}

func replaces(j *job, p *driver.InsertParams) bool {
	if p.UniqueDebounce > 0 {
		return j.state == driver.Scheduled && !j.granted
	}
	return p.UniqueReplace && (j.state == driver.Awaiting || j.state == driver.Scheduled ||
		j.state == driver.Throttled || j.state == driver.Enqueued)
}

func (s *Store) replaced(j *job, at time.Time) (driver.State, time.Time) {
	switch {
	case j.granted || j.state == driver.Enqueued && j.limit != "":
		return j.state, j.runAt
	case j.state == driver.Awaiting || j.state == driver.Scheduled:
		return j.state, at
	case at.After(s.now):
		return driver.Scheduled, at
	}
	return j.state, j.runAt
}

func (s *Store) replace(j *job, p *driver.InsertParams) {
	st, at := s.replaced(j, runAt(p, s.now))
	s.unindex(j)
	j.args, j.meta, j.tags = slices.Clone(p.Args), maps.Clone(p.Meta), slices.Clone(p.Tags)
	j.title, j.priority, j.state, j.runAt = clip(p.Title, maxTitle), p.Priority, st, at
	s.index(j)
}

func (s *Store) release(j *job) {
	if j.unique == "" || j.uniqueFor > 0 {
		return
	}
	if u := s.uniques[j.unique]; u != nil && u.job == j.id {
		delete(s.uniques, j.unique)
	}
}

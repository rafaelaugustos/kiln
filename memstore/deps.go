package memstore

import (
	"fmt"

	"github.com/rafaelaugustos/kiln/driver"
)

func (s *Store) settle() {
	for {
		switch {
		case len(s.resolved) > 0:
			c := pop(&s.resolved)
			s.cascade(c.j, c.st)
		case len(s.done) > 0:
			s.unblock(pop(&s.done))
		case len(s.admit) > 0:
			s.admitKey(pop(&s.admit))
		default:
			return
		}
	}
}

func (s *Store) cascade(p *job, st driver.State) {
	for _, id := range p.children {
		s.follow(s.jobs[id], p.id, st, string(st))
	}
}

func (s *Store) follow(c *job, parent int64, st driver.State, how string) bool {
	if c.state != driver.Awaiting {
		return false
	}
	d := c.dep(parent, false)
	switch {
	case d == nil:
		return false
	case d.on.Has(st):
		s.satisfy(c, d)
	case st != driver.Failed:
		s.log(c, driver.Entry{State: driver.Deleted, Reason: fmt.Sprintf("parent %d %s", parent, how)})
		s.move(c, driver.Deleted)
	default:
		return false
	}
	return true
}

func (s *Store) satisfy(c *job, d *dep) {
	d.resolved = true
	c.pending--
	if c.pending == 0 {
		s.wake(c)
	}
}

func (s *Store) unblock(b *batch) {
	for _, id := range b.dependents {
		if c := s.jobs[id]; c.state == driver.Awaiting {
			if d := c.dep(b.id, true); d != nil {
				s.satisfy(c, d)
			}
		}
	}
	b.dependents = nil
}

func (s *Store) repair(c *job) bool {
	changed := false
	for i := range c.deps {
		d := &c.deps[i]
		switch {
		case c.state != driver.Awaiting:
			return true
		case d.resolved:
		case d.batch:
			if b := s.batches[d.parent]; b == nil || !b.finished.IsZero() {
				s.satisfy(c, d)
				changed = true
			}
		default:
			switch p := s.jobs[d.parent]; {
			case p == nil:
				changed = s.follow(c, d.parent, driver.Deleted, "pruned") || changed
			case final(p.state):
				changed = s.follow(c, p.id, p.state, string(p.state)) || changed
			}
		}
	}
	if c.state == driver.Awaiting && c.pending == 0 {
		s.wake(c)
		return true
	}
	return changed
}

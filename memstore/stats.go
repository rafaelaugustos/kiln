package memstore

import (
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

type bucket struct {
	at     int64
	server string
}

type counters struct {
	succeeded int64
	failed    int64
	deleted   int64
	retried   int64
}

func (c *counters) add(st driver.State) {
	switch st {
	case driver.Succeeded:
		c.succeeded++
	case driver.Failed:
		c.failed++
	case driver.Deleted:
		c.deleted++
	case driver.Scheduled:
		c.retried++
	}
}

func (c *counters) merge(o *counters) {
	c.succeeded += o.succeeded
	c.failed += o.failed
	c.deleted += o.deleted
	c.retried += o.retried
}

func (s *Store) bump(st driver.State) {
	k := bucket{at: s.now.Truncate(time.Minute).Unix(), server: s.server}
	c := s.stats[k]
	if c == nil {
		c = new(counters)
		s.stats[k] = c
	}
	c.add(st)
	s.total.add(st)
}

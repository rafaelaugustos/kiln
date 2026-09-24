package memstore

import (
	"context"

	"github.com/rafaelaugustos/kiln/driver"
)

type sub struct {
	events chan driver.Event
	resync chan struct{}
}

func (s *Store) Subscribe(ctx context.Context, fn func(driver.Event)) error {
	sb := &sub{events: make(chan driver.Event, 256), resync: make(chan struct{}, 1)}
	s.subMu.Lock()
	s.subs[sb] = struct{}{}
	s.listeners.Add(1)
	s.subMu.Unlock()
	defer func() {
		s.subMu.Lock()
		delete(s.subs, sb)
		s.listeners.Add(-1)
		s.subMu.Unlock()
	}()

	fn(driver.Event{Kind: driver.Resync})
	for {
		select {
		case <-ctx.Done():
			return nil
		case e := <-sb.events:
			fn(e)
		case <-sb.resync:
			fn(driver.Event{Kind: driver.Resync})
		}
	}
}

func (s *Store) publish(evs []driver.Event) {
	if len(evs) == 0 {
		return
	}
	s.subMu.Lock()
	defer s.subMu.Unlock()
	for sb := range s.subs {
		for _, e := range evs {
			select {
			case sb.events <- e:
			default:
				select {
				case sb.resync <- struct{}{}:
				default:
				}
			}
		}
	}
}

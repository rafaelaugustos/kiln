package redisbus

import (
	"encoding/binary"
	"fmt"
	"iter"

	"github.com/rafaelaugustos/kiln/driver"
)

func appendEvents(b []byte, events []driver.Event) ([]byte, error) {
	for _, e := range events {
		switch e.Kind {
		case driver.Resync:
			b = append(b, byte(e.Kind), 0)
		case driver.JobsReady, driver.QueueChanged:
			b = binary.AppendUvarint(append(b, byte(e.Kind)), uint64(len(e.Queue)))
			b = append(b, e.Queue...)
		case driver.CancelRequested:
			b = append(b, byte(e.Kind), 0)
			n := len(b)
			b = binary.AppendUvarint(b, uint64(e.ID))
			b[n-1] = byte(len(b) - n)
		default:
			return b, fmt.Errorf("%w: event kind %d", driver.ErrInvalid, e.Kind)
		}
	}
	return b, nil
}

func decode(p string) iter.Seq[driver.Event] {
	return func(yield func(driver.Event) bool) {
		for s := p; s != ""; {
			n, w := uvarint(s[1:])
			if w <= 0 || n > uint64(len(s)-1-w) {
				return
			}
			e := driver.Event{Kind: driver.EventKind(s[0])}
			body := s[1+w : 1+w+int(n)]
			s = s[1+w+int(n):]
			switch e.Kind {
			case driver.Resync:
			case driver.JobsReady, driver.QueueChanged:
				e.Queue = body
			case driver.CancelRequested:
				id, w := uvarint(body)
				if w <= 0 || w != len(body) {
					continue
				}
				e.ID = int64(id)
			default:
				continue
			}
			if !yield(e) {
				return
			}
		}
	}
}

func uvarint(s string) (uint64, int) {
	return binary.Uvarint([]byte(s[:min(len(s), binary.MaxVarintLen64)]))
}

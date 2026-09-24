package pgstore

import (
	"context"
	"strconv"
	"sync"
	"time"
)

type notice struct {
	channel string
	payload string
}

type notifier struct {
	s       *Store
	mu      sync.Mutex
	pending map[notice]struct{}
	wake    chan struct{}
	stop    chan struct{}
	done    chan struct{}
}

func newNotifier(s *Store) *notifier {
	n := &notifier{
		s:       s,
		pending: make(map[notice]struct{}),
		wake:    make(chan struct{}, 1),
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
	go n.run()
	return n
}

func (n *notifier) jobs(queues ...string) {
	if len(queues) == 0 {
		return
	}
	ch := n.s.channel("jobs")
	n.mu.Lock()
	for _, q := range queues {
		n.pending[notice{ch, q}] = struct{}{}
	}
	n.mu.Unlock()
	n.signal()
}

func (n *notifier) cancel(ids ...int64) {
	if len(ids) == 0 {
		return
	}
	ch := n.s.channel("cancel")
	n.mu.Lock()
	for _, id := range ids {
		n.pending[notice{ch, strconv.FormatInt(id, 10)}] = struct{}{}
	}
	n.mu.Unlock()
	n.signal()
}

func (n *notifier) queue(name string) {
	n.mu.Lock()
	n.pending[notice{n.s.channel("queue"), name}] = struct{}{}
	n.mu.Unlock()
	n.signal()
}

func (n *notifier) signal() {
	select {
	case n.wake <- struct{}{}:
	default:
	}
}

func (n *notifier) run() {
	defer close(n.done)
	for {
		select {
		case <-n.stop:
			n.flush()
			return
		case <-n.wake:
			n.flush()
		}
	}
}

func (n *notifier) flush() {
	n.mu.Lock()
	if len(n.pending) == 0 {
		n.mu.Unlock()
		return
	}
	chans := make([]string, 0, len(n.pending))
	payloads := make([]string, 0, len(n.pending))
	for k := range n.pending {
		chans = append(chans, k.channel)
		payloads = append(payloads, k.payload)
	}
	clear(n.pending)
	n.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	n.s.pool.Exec(ctx, n.s.q.notify, chans, payloads)
}

func (n *notifier) close() {
	close(n.stop)
	<-n.done
}

func (s *Store) notifyNow(ctx context.Context, channel string, payloads []string) error {
	chans := make([]string, len(payloads))
	for i := range chans {
		chans[i] = channel
	}
	_, err := s.pool.Exec(ctx, s.q.notify, chans, payloads)
	return err
}

const sqlNotify = `SELECT pg_notify(c, p) FROM unnest($1::text[], $2::text[]) AS n(c, p)`

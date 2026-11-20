package kiln

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln/memstore"
)

type countingOutbox struct {
	calls  atomic.Int64
	limit  atomic.Int64
	backed atomic.Int64
	err    atomic.Bool
}

func (o *countingOutbox) Drain(_ context.Context, limit int) (int, error) {
	o.calls.Add(1)
	o.limit.Store(int64(limit))
	if o.err.Load() {
		return 0, errors.New("database unreachable")
	}
	if o.backed.Add(-1) >= 0 {
		return limit, nil
	}
	return 0, nil
}

func TestServerDrainsOutbox(t *testing.T) {
	t.Parallel()
	ob := &countingOutbox{}
	ob.backed.Store(50)
	cfg := fastConfig()
	cfg.PollInterval = time.Hour
	cfg.Outbox = ob
	m := NewMux()
	Handle(m, func(context.Context, *Job[ping]) error { return nil })
	runServer(t, NewClient(memstore.New()), m, cfg)
	eventually(t, 5*time.Second, "the backlog to be drained without waiting for a poll", func() bool {
		return ob.calls.Load() >= 51
	})
	if ob.limit.Load() < 1 {
		t.Fatalf("drain limit %d", ob.limit.Load())
	}
}

func TestServerSurvivesOutboxErrors(t *testing.T) {
	t.Parallel()
	ob := &countingOutbox{}
	ob.err.Store(true)
	cfg := fastConfig()
	cfg.Outbox = ob
	c := NewClient(memstore.New())
	m := NewMux()
	Handle(m, func(context.Context, *Job[ping]) error { return nil })
	runServer(t, c, m, cfg)
	id := mustEnqueue(t, c, ping{N: 1})
	waitState(t, c, id, Succeeded)
	eventually(t, 5*time.Second, "drain to be retried after an error", func() bool { return ob.calls.Load() >= 3 })
}

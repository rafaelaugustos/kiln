package kiln

import (
	"context"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln/memstore"
)

func TestPromoteWithoutNotifier(t *testing.T) {
	st := memstore.New()
	m := NewMux()
	starts := make(chan time.Time, 1)
	Handle(m, func(context.Context, *Job[ping]) error {
		starts <- time.Now()
		return nil
	})
	cfg := fastConfig()
	cfg.PollInterval = 5 * time.Second
	runServer(t, NewClient(bareStore{st}), m, cfg)
	time.Sleep(50 * time.Millisecond)

	enqueued := time.Now()
	mustEnqueue(t, NewClient(st), ping{}, Delay(100*time.Millisecond))
	select {
	case at := <-starts:
		if d := at.Sub(enqueued); d > 600*time.Millisecond {
			t.Fatalf("delayed job started %v after enqueue, want within about %v", d, 100*time.Millisecond+blindPromote)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("delayed job did not start")
	}
}

package memstore_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

func TestSubscribe(t *testing.T) {
	t.Parallel()
	s, _ := open(t)
	sctx, cancel := context.WithCancel(ctx)
	events := make(chan driver.Event, 64)
	errc := make(chan error, 1)
	go func() { errc <- s.Subscribe(sctx, func(e driver.Event) { events <- e }) }()

	next := func() driver.Event {
		t.Helper()
		select {
		case e := <-events:
			return e
		case <-time.After(time.Second):
			t.Fatal("no event")
		}
		return driver.Event{}
	}
	if e := next(); e.Kind != driver.Resync {
		t.Fatalf("first event = %+v", e)
	}
	id := ids(t, s, params("a"), params("a"), params("a", queue("q")))[0]
	got := map[string]bool{}
	for range 2 {
		e := next()
		if e.Kind != driver.JobsReady {
			t.Fatalf("event = %+v", e)
		}
		got[e.Queue] = true
	}
	if !got["default"] || !got["q"] {
		t.Fatalf("ready queues = %v", got)
	}
	claimOne(t, s)
	if _, err := s.Delete(ctx, driver.Filter{IDs: []int64{id}}); err != nil {
		t.Fatal(err)
	}
	if e := next(); e.Kind != driver.CancelRequested || e.ID != id {
		t.Fatalf("cancel event = %+v", e)
	}
	must(t, s.PauseQueue(ctx, "q", true))
	if e := next(); e.Kind != driver.QueueChanged || e.Queue != "q" {
		t.Fatalf("queue event = %+v", e)
	}
	cancel()
	if err := <-errc; err != nil {
		t.Fatalf("subscribe returned %v", err)
	}
}

func TestSubscribeOverflow(t *testing.T) {
	t.Parallel()
	s, _ := open(t)
	sctx, cancel := context.WithCancel(ctx)
	defer cancel()
	release := make(chan struct{})
	resyncs := make(chan struct{}, 8)
	go s.Subscribe(sctx, func(e driver.Event) {
		if e.Kind == driver.Resync {
			resyncs <- struct{}{}
		}
		<-release
	})
	<-resyncs
	for i := range 600 {
		insert(t, s, params("a", queue(fmt.Sprintf("q%d", i))))
	}
	close(release)
	select {
	case <-resyncs:
	case <-time.After(time.Second):
		t.Fatal("no resync after overflow")
	}
}

package redisbus

import (
	"slices"
	"strings"
	"testing"

	"github.com/rafaelaugustos/kiln/driver"
)

func TestNew(t *testing.T) {
	t.Parallel()
	if b := New(nil); b.channel != "kiln" {
		t.Errorf("default channel %q", b.channel)
	}
	if b := New(nil, Channel("billing")); b.channel != "billing" {
		t.Errorf("channel %q, want billing", b.channel)
	}
}

func TestRoundTrip(t *testing.T) {
	t.Parallel()
	b := New(connect(t), Channel(channel()))
	r := listen(t, b)

	want := []driver.Event{{Kind: driver.Resync}}
	for _, e := range []driver.Event{
		{Kind: driver.JobsReady, Queue: "default"},
		{Kind: driver.CancelRequested, ID: 42},
		{Kind: driver.QueueChanged, Queue: "mail"},
		{Kind: driver.Resync},
	} {
		publish(t, b, e)
		want = append(want, e)
	}
	batch := []driver.Event{
		{Kind: driver.JobsReady, Queue: "critical"},
		{Kind: driver.JobsReady, Queue: strings.Repeat("q", 200)},
		{Kind: driver.CancelRequested, ID: 1 << 50},
		{Kind: driver.CancelRequested, ID: 7},
		{Kind: driver.QueueChanged, Queue: "fila-ação"},
		{Kind: driver.JobsReady},
	}
	publish(t, b, batch...)
	want = append(want, batch...)

	got := r.wait(t, "every event", func(evs []driver.Event) bool { return len(evs) >= len(want) })
	if !slices.Equal(got, want) {
		t.Fatalf("got %v\nwant %v", got, want)
	}
}

func TestSubscribers(t *testing.T) {
	t.Parallel()
	ch := channel()
	b := New(connect(t), Channel(ch))
	rs := []*recorder{listen(t, b), listen(t, b), listen(t, New(connect(t), Channel(ch)))}

	sent := []driver.Event{{Kind: driver.JobsReady, Queue: "a"}, {Kind: driver.CancelRequested, ID: 7}}
	publish(t, b, sent...)
	want := append([]driver.Event{{Kind: driver.Resync}}, sent...)
	for i, r := range rs {
		got := r.wait(t, "both events", func(evs []driver.Event) bool { return len(evs) >= len(want) })
		if !slices.Equal(got, want) {
			t.Errorf("subscriber %d got %v, want %v", i, got, want)
		}
	}
}

func TestChannelIsolation(t *testing.T) {
	t.Parallel()
	c := connect(t)
	a, b := New(c, Channel(channel())), New(c, Channel(channel()))
	ra, rb := listen(t, a), listen(t, b)

	toA := driver.Event{Kind: driver.JobsReady, Queue: "a"}
	toB := driver.Event{Kind: driver.JobsReady, Queue: "b"}
	marker := driver.Event{Kind: driver.JobsReady, Queue: "a-again"}
	publish(t, a, toA)
	publish(t, b, toB)
	publish(t, a, marker)

	if got := ra.wait(t, "the marker", has(marker)); slices.Contains(got, toB) {
		t.Errorf("channel a received %v from channel b", toB)
	}
	if got := rb.wait(t, "its event", has(toB)); slices.Contains(got, toA) {
		t.Errorf("channel b received %v from channel a", toA)
	}
}

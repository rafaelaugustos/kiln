package sqlitestore

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

type bus struct {
	mu      sync.Mutex
	subs    map[chan driver.Event]struct{}
	log     []driver.Event
	down    error
	refuse  error
	probe   func(driver.Event) bool
	early   []driver.Event
	backlog func() bool
}

func newBus() *bus {
	return &bus{subs: make(map[chan driver.Event]struct{})}
}

func (b *bus) Subscribe(ctx context.Context, fn func(driver.Event)) error {
	if b.refuse != nil {
		return b.refuse
	}
	ch := make(chan driver.Event, 1024)
	b.mu.Lock()
	b.subs[ch] = struct{}{}
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		delete(b.subs, ch)
		b.mu.Unlock()
	}()
	fn(driver.Event{Kind: driver.Resync})
	for {
		select {
		case <-ctx.Done():
			return nil
		case e := <-ch:
			fn(e)
		}
	}
}

func (b *bus) Publish(_ context.Context, evs []driver.Event) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.down != nil {
		return b.down
	}
	for _, e := range evs {
		if b.probe != nil && !b.probe(e) {
			b.early = append(b.early, e)
		}
		for ch := range b.subs {
			select {
			case ch <- e:
			default:
			}
		}
	}
	b.log = append(b.log, evs...)
	return nil
}

func (b *bus) subscribers() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.subs)
}

const quiet = 50 * time.Millisecond

func (b *bus) since(n int) []driver.Event {
	b.mu.Lock()
	defer b.mu.Unlock()
	return slices.Clone(b.log[n:])
}

func (b *bus) size() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.log)
}

func (b *bus) idle() {
	n := b.size()
	for {
		time.Sleep(quiet)
		m := b.size()
		if m == n && !b.backlog() {
			return
		}
		n = m
	}
}

func (b *bus) expect(t *testing.T, op func(), want ...driver.Event) {
	t.Helper()
	b.idle()
	b.mu.Lock()
	from := len(b.log)
	b.early = nil
	b.mu.Unlock()
	op()
	deadline := time.Now().Add(2 * time.Second)
	for !covers(b.since(from), want) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	b.idle()
	b.mu.Lock()
	got, early := slices.Clone(b.log[from:]), b.early
	b.mu.Unlock()
	if len(got) != len(want) || !covers(got, want) {
		t.Fatalf("published %+v, want %+v", got, want)
	}
	if len(early) > 0 {
		t.Fatalf("published %+v before other connections could see the change", early)
	}
}

func covers(got, want []driver.Event) bool {
	for _, e := range want {
		if !slices.Contains(got, e) {
			return false
		}
	}
	return true
}

func committed(s *Store) func(driver.Event) bool {
	return func(e driver.Event) bool {
		var (
			query string
			arg   any
		)
		switch e.Kind {
		case driver.JobsReady:
			query, arg = "SELECT COUNT(*) FROM kiln_jobs WHERE queue = ? AND state IN ('enqueued', 'scheduled')", e.Queue
		case driver.CancelRequested:
			query, arg = "SELECT COUNT(*) FROM kiln_jobs WHERE id = ? AND cancel_requested", e.ID
		case driver.QueueChanged:
			query, arg = "SELECT COUNT(*) FROM kiln_queues WHERE name = ?", e.Queue
		default:
			return true
		}
		var n int
		return s.db.QueryRow(query, arg).Scan(&n) == nil && n > 0
	}
}

func openBus(t *testing.T) (*Store, *bus) {
	t.Helper()
	b := newBus()
	s := open(t, Bus(b))
	b.mu.Lock()
	b.probe = committed(s)
	b.mu.Unlock()
	b.backlog = func() bool {
		s.hub.relay.mu.Lock()
		defer s.hub.relay.mu.Unlock()
		return len(s.hub.relay.pending) > 0 || len(s.hub.relay.kick) > 0
	}
	return s, b
}

func ready(queue string) driver.Event {
	return driver.Event{Kind: driver.JobsReady, Queue: queue}
}

func delay(d time.Duration) func(*driver.InsertParams) {
	return func(p *driver.InsertParams) { p.Delay = d }
}

func afterBatch(id int64) func(*driver.InsertParams) {
	return func(p *driver.InsertParams) { p.AfterBatch = id }
}

func start(t *testing.T, s *Store, p driver.InsertParams) driver.Job {
	t.Helper()
	id := insert(t, s, p)[0].ID
	js := claim(t, s, 1, p.Queue)
	if len(js) != 1 || js[0].ID != id {
		t.Fatalf("claimed %v from %s, want job %d", js, p.Queue, id)
	}
	return js[0]
}

func finishAll(t *testing.T, s *Store, outs ...driver.Outcome) {
	t.Helper()
	for i, r := range finish(t, s, outs...) {
		if r != driver.Applied {
			t.Fatalf("outcome %d for job %d: %v, want applied", i, outs[i].ID, r)
		}
	}
}

func exec(t *testing.T, s *Store, query string, args ...any) {
	t.Helper()
	if _, err := s.db.Exec(query, args...); err != nil {
		t.Fatal(err)
	}
}

func TestSubscribeBus(t *testing.T) {
	t.Parallel()
	b := newBus()
	path := filepath.Join(t.TempDir(), "kiln.db")
	var stores []*Store
	for range 2 {
		s, err := New(context.Background(), connect(t, path), Bus(b))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(s.Close)
		stores = append(stores, s)
	}
	here, there := stores[0], stores[1]
	e := listen(t, here)
	insert(t, there, job("a", queue("remote")))
	e.wait(t, ready("remote"))
	insert(t, here, job("a", queue("local")))
	e.wait(t, ready("local"))
}

func TestSubscribeBusFails(t *testing.T) {
	t.Parallel()
	b := newBus()
	b.refuse = errors.New("bus unreachable")
	s := open(t, Bus(b))
	if err := s.Subscribe(context.Background(), func(driver.Event) {}); !errors.Is(err, b.refuse) {
		t.Fatalf("subscribe: %v, want %v", err, b.refuse)
	}
}

func TestCloseEndsBusSubscription(t *testing.T) {
	t.Parallel()
	b := newBus()
	s := open(t, Bus(b))
	done := make(chan error, 1)
	go func() { done <- s.Subscribe(context.Background(), func(driver.Event) {}) }()
	deadline := time.Now().Add(2 * time.Second)
	for b.subscribers() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("never subscribed to the bus")
		}
		time.Sleep(time.Millisecond)
	}
	s.Close()
	select {
	case err := <-done:
		if !errors.Is(err, errClosed) {
			t.Fatalf("subscribe after close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("subscribe still running after close")
	}
	if n := b.subscribers(); n != 0 {
		t.Fatalf("%d bus subscriptions left after close", n)
	}
}

func TestBusInsert(t *testing.T) {
	t.Parallel()
	s, b := openBus(t)
	b.expect(t, func() { insert(t, s, job("a", queue("mail")), job("a", queue("mail")), job("a", queue("sms"))) },
		ready("mail"), ready("sms"))
	b.expect(t, func() { insert(t, s, job("a", queue("later"), delay(time.Hour))) })
	b.expect(t, func() { insert(t, s, job("a", queue("lim"), limited("k", 1)), job("a", queue("lim"), limited("k", 1))) },
		ready("lim"))
	paced := rated("r", 1, time.Minute, 1)
	b.expect(t, func() { insert(t, s, job("a", queue("now"), paced)) }, ready("now"))
	b.expect(t, func() {
		if st := insert(t, s, job("a", queue("slot"), paced))[0].State; st != driver.Scheduled {
			t.Fatalf("inserted as %s, want scheduled in a reserved slot", st)
		}
	}, ready("slot"))
	b.expect(t, func() { insert(t, s, job("a", queue("once"), unique("u"))) }, ready("once"))
	b.expect(t, func() { insert(t, s, job("a", queue("once"), unique("u"))) })
	parent := insert(t, s, job("p", queue("mail")))[0].ID
	b.expect(t, func() { insert(t, s, job("c", queue("child"), after(driver.OnSucceeded, parent))) })
	batch, err := s.OpenBatch(context.Background(), driver.NewBatch{})
	if err != nil {
		t.Fatal(err)
	}
	b.expect(t, func() { insert(t, s, job("m", queue("member"), inBatch(batch))) }, ready("member"))
}

func TestBusFinish(t *testing.T) {
	t.Parallel()
	s, b := openBus(t)
	parent := start(t, s, job("p", queue("parents")))
	insert(t, s, job("c", queue("children"), after(driver.OnSucceeded, parent.ID)))
	running := start(t, s, job("a", queue("first"), limited("k", 1)))
	insert(t, s, job("a", queue("waiting"), limited("k", 1)))
	retry := start(t, s, job("a", queue("retry")))
	later := start(t, s, job("a", queue("later")))
	done := start(t, s, job("a", queue("done")))
	b.expect(t, func() {
		finishAll(t, s,
			driver.Outcome{Ref: parent.Ref, State: driver.Succeeded},
			driver.Outcome{Ref: running.Ref, State: driver.Succeeded},
			driver.Outcome{Ref: retry.Ref, State: driver.Scheduled, Reason: "retry"},
			driver.Outcome{Ref: later.Ref, State: driver.Scheduled, Delay: time.Hour, Reason: "retry"},
			driver.Outcome{Ref: done.Ref, State: driver.Succeeded})
	}, ready("children"), ready("waiting"), ready("retry"))
}

func TestBusPromote(t *testing.T) {
	t.Parallel()
	s, b := openBus(t)
	insert(t, s, job("a", queue("due"), delay(20*time.Millisecond)))
	paced := rated("r", 20, time.Second, 1)
	if st := insert(t, s, job("a", queue("first"), paced), job("a", queue("second"), paced))[1].State; st != driver.Scheduled {
		t.Fatalf("second paced job inserted as %s, want scheduled in a reserved slot", st)
	}
	time.Sleep(60 * time.Millisecond)
	b.expect(t, func() {
		if p, err := s.Promote(context.Background(), 10); err != nil || p.Count != 2 {
			t.Fatalf("promote: %+v, %v", p, err)
		}
	}, ready("due"), ready("second"))
}

func TestBusSweep(t *testing.T) {
	t.Parallel()
	s, b := openBus(t)
	lim := insert(t, s, job("a", queue("swept"), limited("sk", 2)))[0].ID
	parent := insert(t, s, job("p", queue("parent")))[0].ID
	child := insert(t, s, job("c", queue("stuck"), after(driver.OnSucceeded, parent)))[0].ID
	exec(t, s, "UPDATE kiln_jobs SET state = 'throttled' WHERE id = ?", lim)
	exec(t, s, "UPDATE kiln_jobs SET deps_pending = 0 WHERE id = ?", child)
	b.expect(t, func() {
		if _, err := s.Sweep(context.Background(), 100); err != nil {
			t.Fatal(err)
		}
	}, ready("swept"), ready("stuck"))
}

func TestBusRequeue(t *testing.T) {
	t.Parallel()
	s, b := openBus(t)
	failed := start(t, s, job("a", queue("again")))
	lim := start(t, s, job("a", queue("limited"), limited("rk", 1)))
	succeeded := start(t, s, job("a", queue("archived")))
	finishAll(t, s,
		driver.Outcome{Ref: failed.Ref, State: driver.Failed},
		driver.Outcome{Ref: lim.Ref, State: driver.Failed},
		driver.Outcome{Ref: succeeded.Ref, State: driver.Succeeded})
	b.expect(t, func() {
		ids := []int64{failed.ID, lim.ID, succeeded.ID}
		if n, err := s.Requeue(context.Background(), driver.Filter{IDs: ids}); err != nil || n != 3 {
			t.Fatalf("requeue: %d, %v", n, err)
		}
	}, ready("again"), ready("limited"), ready("archived"))
}

func TestBusFire(t *testing.T) {
	t.Parallel()
	s, b := openBus(t)
	ctx := context.Background()
	now, err := s.Now(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r := driver.Recurring{ID: "tick", Spec: "@every 1m", Location: "UTC", Template: job("a", queue("cron")), NextRunAt: now}
	if err := s.PutRecurring(ctx, r); err != nil {
		t.Fatal(err)
	}
	if r, err = s.Recurring(ctx, "tick"); err != nil {
		t.Fatal(err)
	}
	b.expect(t, func() {
		f := driver.Fire{ID: r.ID, Version: r.Version, NextRunAt: now.Add(time.Minute), LastRunAt: now,
			Jobs: []driver.InsertParams{job("a", queue("cron")), job("a", queue("capped"), limited("fk", 1))}}
		if _, err := s.Fire(ctx, f); err != nil {
			t.Fatal(err)
		}
	}, ready("cron"), ready("capped"))
}

func TestBusSealBatch(t *testing.T) {
	t.Parallel()
	s, b := openBus(t)
	ctx := context.Background()
	own, err := s.OpenBatch(ctx, driver.NewBatch{})
	if err != nil {
		t.Fatal(err)
	}
	insert(t, s, job("t", queue("then"), afterBatch(own)), job("t", queue("capped"), afterBatch(own), limited("bk", 1)))
	b.expect(t, func() {
		if err := s.SealBatch(ctx, own); err != nil {
			t.Fatal(err)
		}
	}, ready("then"), ready("capped"))

	theirs, err := s.OpenBatch(ctx, driver.NewBatch{})
	if err != nil {
		t.Fatal(err)
	}
	insert(t, s, job("t", queue("later"), afterBatch(theirs)))
	tx := userTx(t, s)
	w := s.Tx(tx)
	b.expect(t, func() {
		if err := w.SealBatch(ctx, theirs); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	})
	b.expect(t, func() {
		if err := w.Notify(ctx); err != nil {
			t.Fatal(err)
		}
	}, ready("later"))
}

func TestBusTxWriter(t *testing.T) {
	t.Parallel()
	s, b := openBus(t)
	ctx := context.Background()
	tx := userTx(t, s)
	w := s.Tx(tx)
	b.expect(t, func() {
		if _, err := w.Insert(ctx, []driver.InsertParams{job("a", queue("tx")), job("a", queue("capped"), limited("tk", 1))}); err != nil {
			t.Fatal(err)
		}
	})
	b.expect(t, func() {
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	})
	notify := func() {
		if err := w.Notify(ctx); err != nil {
			t.Fatal(err)
		}
	}
	b.expect(t, notify, ready("tx"), ready("capped"))
	b.expect(t, notify)

	rolled := userTx(t, s)
	b.expect(t, func() {
		if _, err := s.Tx(rolled).Insert(ctx, []driver.InsertParams{job("a", queue("rolled"))}); err != nil {
			t.Fatal(err)
		}
		if err := rolled.Rollback(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestBusInTx(t *testing.T) {
	t.Parallel()
	s, b := openBus(t)
	ctx := context.Background()
	b.expect(t, func() {
		err := s.InTx(ctx, func(w driver.Writer) error {
			_, err := w.Insert(ctx, []driver.InsertParams{job("a", queue("intx")), job("a", queue("capped"), limited("ik", 1))})
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
	}, ready("intx"), ready("capped"))
	abort := errors.New("abort")
	b.expect(t, func() {
		err := s.InTx(ctx, func(w driver.Writer) error {
			if _, err := w.Insert(ctx, []driver.InsertParams{job("a", queue("gone"))}); err != nil {
				return err
			}
			return abort
		})
		if !errors.Is(err, abort) {
			t.Fatalf("InTx: %v, want %v", err, abort)
		}
	})
}

func TestBusDelete(t *testing.T) {
	t.Parallel()
	s, b := openBus(t)
	remove := func(id int64) func() {
		return func() {
			if _, err := s.Delete(context.Background(), driver.Filter{IDs: []int64{id}}); err != nil {
				t.Fatal(err)
			}
		}
	}
	running := start(t, s, job("a", queue("busy")))
	b.expect(t, remove(running.ID), driver.Event{Kind: driver.CancelRequested, ID: running.ID})
	b.expect(t, remove(running.ID))
	head := insert(t, s, job("a", queue("head"), limited("dk", 1)))[0].ID
	insert(t, s, job("a", queue("next"), limited("dk", 1)))
	b.expect(t, remove(head), ready("next"))
}

func TestBusPause(t *testing.T) {
	t.Parallel()
	s, b := openBus(t)
	for _, paused := range []bool{true, false} {
		b.expect(t, func() {
			if err := s.PauseQueue(context.Background(), "mail", paused); err != nil {
				t.Fatal(err)
			}
		}, driver.Event{Kind: driver.QueueChanged, Queue: "mail"})
	}
}

func TestBusPrune(t *testing.T) {
	t.Parallel()
	s, b := openBus(t)
	parent := start(t, s, job("p", queue("doomed")))
	insert(t, s, job("c", queue("orphan"), after(driver.OnDeleted, parent.ID)))
	finishAll(t, s, driver.Outcome{Ref: parent.Ref, State: driver.Failed})
	time.Sleep(2 * time.Millisecond)
	b.expect(t, func() {
		p := driver.PruneParams{Retention: driver.Retention{Succeeded: -1, Deleted: -1}, Limit: 100}
		if _, err := s.Prune(context.Background(), p); err != nil {
			t.Fatal(err)
		}
	}, ready("orphan"))
}

func TestBusDown(t *testing.T) {
	t.Parallel()
	b := newBus()
	b.down = errors.New("bus down")
	s := open(t, Bus(b))
	ctx := context.Background()
	retry := start(t, s, job("a", queue("retry")))
	finishAll(t, s, driver.Outcome{Ref: retry.Ref, State: driver.Scheduled, Reason: "retry"})
	running := start(t, s, job("a", queue("busy")))
	if n, err := s.Delete(ctx, driver.Filter{IDs: []int64{running.ID}}); err != nil || n != 1 {
		t.Fatalf("delete: %d, %v", n, err)
	}
	if err := s.PauseQueue(ctx, "busy", true); err != nil {
		t.Fatal(err)
	}
	tx := userTx(t, s)
	w := s.Tx(tx)
	if _, err := w.Insert(ctx, []driver.InsertParams{job("a")}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := w.Notify(ctx); !errors.Is(err, b.down) {
		t.Fatalf("notify: %v, want %v", err, b.down)
	}
}

package drivertest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

// OutboxHarness is an outbox under test, as [RunOutbox] takes it: the outbox, and a way to open
// transactions in its database with a writer bound to each.
type OutboxHarness struct {
	Outbox driver.Outbox

	// Begin starts a transaction and returns the writer bound to it, a function that commits the
	// transaction and one that rolls it back.
	Begin func(t *testing.T) (w driver.TxWriter, commit, rollback func())
}

// RunOutbox checks an implementation of [driver.Outbox] and of the writers it binds to
// transactions, as subtests of t. Each test calls open with its own t and with the store the
// outbox must stage for and apply to, a fake that records what it is given; open returns an
// outbox of its own, empty, which it can release with t.Cleanup. The tests wait on the real
// clock for Drain to take entries it is allowed to leave for a second.
func RunOutbox(t *testing.T, open func(t *testing.T, target driver.Stager) OutboxHarness) {
	tests := []struct {
		name string
		fn   func(t *testing.T, r *Recorder, h OutboxHarness)
	}{
		{"Notify", testOutboxNotify},
		{"Drain", testOutboxDrain},
		{"Rollback", testOutboxRollback},
		{"Uncommitted", testOutboxUncommitted},
		{"SeveralWrites", testOutboxSeveralWrites},
		{"Retry", testOutboxRetry},
		{"Rejected", testOutboxRejected},
		{"Order", testOutboxOrder},
		{"Concurrent", testOutboxConcurrent},
	}
	for _, c := range tests {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			r := NewRecorder()
			c.fn(t, r, open(t, r))
		})
	}
}

// Recorder is a [driver.Stager] that keeps no jobs: it hands out ids, records the writes applied
// to it and can be told to fail, for tests of outboxes and of code that drains them.
type Recorder struct {
	mu      sync.Mutex
	seq     int64
	names   int
	encoded map[string][]byte
	calls   map[string]int
	applied []string
	fail    func(id string, call int) error
}

// NewRecorder returns an empty Recorder.
func NewRecorder() *Recorder {
	return &Recorder{encoded: make(map[string][]byte), calls: make(map[string]int)}
}

// FailWith makes Apply return what fn returns for the id and the number of the call, from 1,
// whenever that is not nil.
func (r *Recorder) FailWith(fn func(id string, call int) error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.fail = fn
}

// Applied returns the ids applied so far, in the order they were first applied.
func (r *Recorder) Applied() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.applied)
}

// Calls returns how many times Apply was called with id.
func (r *Recorder) Calls(id string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls[id]
}

// Stage returns a writer that records its writes.
func (r *Recorder) Stage(context.Context) (driver.Staged, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.names++
	return &recording{r: r, id: "w" + strconv.Itoa(r.names)}, nil
}

// Apply records the writes of id, once, after checking that data is what the writer last encoded.
func (r *Recorder) Apply(_ context.Context, id string, data []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls[id]++
	if r.fail != nil {
		if err := r.fail(id, r.calls[id]); err != nil {
			return err
		}
	}
	if !bytes.Equal(data, r.encoded[id]) {
		return fmt.Errorf("%w: writes %q are not what was encoded", driver.ErrInvalid, id)
	}
	if !slices.Contains(r.applied, id) {
		r.applied = append(r.applied, id)
	}
	return nil
}

type recording struct {
	r   *Recorder
	id  string
	ops []string
}

func (w *recording) next() int64 {
	w.r.mu.Lock()
	defer w.r.mu.Unlock()
	w.r.seq++
	return w.r.seq
}

func (w *recording) Insert(_ context.Context, jobs []driver.InsertParams) ([]driver.Inserted, error) {
	if err := driver.CheckInsert(jobs); err != nil {
		return nil, err
	}
	out := make([]driver.Inserted, len(jobs))
	for i := range jobs {
		out[i] = driver.Inserted{ID: w.next(), State: driver.Enqueued}
		w.ops = append(w.ops, "insert "+strconv.FormatInt(out[i].ID, 10)+" "+jobs[i].Kind)
	}
	return out, nil
}

func (w *recording) OpenBatch(context.Context, driver.NewBatch) (int64, error) {
	id := w.next()
	w.ops = append(w.ops, "open "+strconv.FormatInt(id, 10))
	return id, nil
}

func (w *recording) SealBatch(_ context.Context, id int64) error {
	w.ops = append(w.ops, "seal "+strconv.FormatInt(id, 10))
	return nil
}

func (w *recording) Encode() (string, []byte, error) {
	data, err := json.Marshal(w.ops)
	if err != nil {
		return "", nil, err
	}
	w.r.mu.Lock()
	defer w.r.mu.Unlock()
	w.r.encoded[w.id] = data
	return w.id, data, nil
}

const outboxWait = 10 * time.Second

func commitOne(t *testing.T, h OutboxHarness) driver.TxWriter {
	t.Helper()
	w, commit, _ := h.Begin(t)
	insert(t, w, task("out"))
	commit()
	return w
}

func drain(t *testing.T, h OutboxHarness, limit int) int {
	t.Helper()
	n, err := h.Outbox.Drain(t.Context(), limit)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	return n
}

func settled(t *testing.T, h OutboxHarness) {
	t.Helper()
	time.Sleep(1200 * time.Millisecond)
	if n := drain(t, h, 100); n != 0 {
		t.Fatalf("drain applied %d entries, want none left", n)
	}
}

func testOutboxNotify(t *testing.T, r *Recorder, h OutboxHarness) {
	w := commitOne(t, h)
	if got := r.Applied(); len(got) != 0 {
		t.Fatalf("applied %v before notify", got)
	}
	if err := w.Notify(t.Context()); err != nil {
		t.Fatalf("notify: %v", err)
	}
	got := r.Applied()
	if len(got) != 1 {
		t.Fatalf("applied %v after notify, want one entry", got)
	}
	if err := w.Notify(t.Context()); err != nil {
		t.Fatalf("second notify: %v", err)
	}
	settled(t, h)
	if n := r.Calls(got[0]); n != 1 {
		t.Fatalf("apply called %d times, want 1", n)
	}
}

func testOutboxDrain(t *testing.T, r *Recorder, h OutboxHarness) {
	commitOne(t, h)
	eventually(t, outboxWait, func() bool {
		drain(t, h, 10)
		return len(r.Applied()) == 1
	})
	settled(t, h)
	if id := r.Applied()[0]; r.Calls(id) != 1 {
		t.Fatalf("apply called %d times, want 1", r.Calls(id))
	}
}

func testOutboxRollback(t *testing.T, r *Recorder, h OutboxHarness) {
	w, _, rollback := h.Begin(t)
	insert(t, w, task("out"))
	rollback()
	if err := w.Notify(t.Context()); err != nil {
		t.Fatalf("notify after rollback: %v", err)
	}
	settled(t, h)
	if got := r.Applied(); len(got) != 0 {
		t.Fatalf("applied %v from a transaction that rolled back", got)
	}
}

func testOutboxUncommitted(t *testing.T, r *Recorder, h OutboxHarness) {
	w, commit, _ := h.Begin(t)
	insert(t, w, task("out"))
	settled(t, h)
	if got := r.Applied(); len(got) != 0 {
		t.Fatalf("applied %v before the commit", got)
	}
	commit()
	eventually(t, outboxWait, func() bool {
		drain(t, h, 10)
		return len(r.Applied()) == 1
	})
}

func testOutboxSeveralWrites(t *testing.T, r *Recorder, h OutboxHarness) {
	w, commit, _ := h.Begin(t)
	insert(t, w, task("out"))
	bid := openBatch(t, w)
	insert(t, w, members("out", bid, 2)...)
	seal(t, w, bid)
	commit()
	if err := w.Notify(t.Context()); err != nil {
		t.Fatalf("notify: %v", err)
	}
	if got := r.Applied(); len(got) != 1 {
		t.Fatalf("applied %v, want the transaction's writes as one entry", got)
	}
}

func testOutboxRetry(t *testing.T, r *Recorder, h OutboxHarness) {
	down := errors.New("store unreachable")
	r.FailWith(func(_ string, call int) error {
		if call == 1 {
			return down
		}
		return nil
	})
	w := commitOne(t, h)
	wantErr(t, w.Notify(t.Context()), down)
	if got := r.Applied(); len(got) != 0 {
		t.Fatalf("applied %v by a call that failed", got)
	}
	eventually(t, outboxWait, func() bool {
		h.Outbox.Drain(t.Context(), 10)
		return len(r.Applied()) == 1
	})
	settled(t, h)
}

func testOutboxRejected(t *testing.T, r *Recorder, h OutboxHarness) {
	r.FailWith(func(string, int) error { return fmt.Errorf("%w: parent 7", driver.ErrNotFound) })
	w := commitOne(t, h)
	wantErr(t, w.Notify(t.Context()), driver.ErrNotFound)
	time.Sleep(1200 * time.Millisecond)
	for range 3 {
		if n, _ := h.Outbox.Drain(t.Context(), 10); n != 0 {
			t.Fatalf("drain applied %d rejected entries", n)
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, n := range r.calls {
		if n != 1 {
			t.Fatalf("apply called %d times for %s, want 1 for writes that can never be applied", n, id)
		}
	}
}

func testOutboxOrder(t *testing.T, r *Recorder, h OutboxHarness) {
	for range 5 {
		commitOne(t, h)
	}
	eventually(t, outboxWait, func() bool {
		if n := drain(t, h, 2); n > 2 {
			t.Fatalf("drain applied %d entries with a limit of 2", n)
		}
		return len(r.Applied()) == 5
	})
	got := r.Applied()
	if !slices.IsSortedFunc(got, func(a, b string) int { return len(a) - len(b) }) || !slices.IsSorted(got) {
		t.Fatalf("applied in order %v, want oldest first", got)
	}
}

func testOutboxConcurrent(t *testing.T, r *Recorder, h OutboxHarness) {
	const entries = 20
	for range entries {
		commitOne(t, h)
	}
	time.Sleep(1200 * time.Millisecond)
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 20 {
				h.Outbox.Drain(context.Background(), 3)
			}
		})
	}
	wg.Wait()
	got := r.Applied()
	if len(got) != entries {
		t.Fatalf("applied %d entries, want %d", len(got), entries)
	}
	for _, id := range got {
		if n := r.Calls(id); n != 1 {
			t.Fatalf("apply called %d times for %s, want 1", n, id)
		}
	}
}

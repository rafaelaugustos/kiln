package memstore_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
	"github.com/rafaelaugustos/kiln/memstore"
)

var (
	ctx   = context.Background()
	epoch = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
)

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) add(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func open(t *testing.T) (*memstore.Store, *clock) {
	t.Helper()
	c := &clock{t: epoch}
	return memstore.New(memstore.Clock(c.now)), c
}

type opt func(*driver.InsertParams)

func params(kind string, opts ...opt) driver.InsertParams {
	p := driver.InsertParams{Kind: kind, Queue: "default", Args: []byte(`{}`), MaxAttempts: 3}
	for _, o := range opts {
		o(&p)
	}
	return p
}

func queue(q string) opt                { return func(p *driver.InsertParams) { p.Queue = q } }
func priority(n int16) opt              { return func(p *driver.InsertParams) { p.Priority = n } }
func delay(d time.Duration) opt         { return func(p *driver.InsertParams) { p.Delay = d } }
func inBatch(id int64) opt              { return func(p *driver.InsertParams) { p.BatchID = id } }
func afterBatch(id int64) opt           { return func(p *driver.InsertParams) { p.AfterBatch = id } }
func needs(i int, m driver.Mask) opt    { return parent(driver.Parent{Index: i, On: m}) }
func after(id int64, m driver.Mask) opt { return parent(driver.Parent{ID: id, On: m}) }

func parent(pr driver.Parent) opt {
	return func(p *driver.InsertParams) { p.Parents = append(p.Parents, pr) }
}

func limited(key string, n int) opt {
	return func(p *driver.InsertParams) { p.LimitKey, p.LimitMax = key, n }
}

func unique(key string, d time.Duration) opt {
	return func(p *driver.InsertParams) { p.UniqueKey, p.UniqueFor = []byte(key), d }
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func beat(t *testing.T, s driver.Store, info driver.ServerInfo) driver.Directives {
	t.Helper()
	d, err := s.Heartbeat(ctx, info)
	must(t, err)
	return d
}

func insert(t *testing.T, w driver.Writer, ps ...driver.InsertParams) []driver.Inserted {
	t.Helper()
	res, err := w.Insert(ctx, ps)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	return res
}

func ids(t *testing.T, w driver.Writer, ps ...driver.InsertParams) []int64 {
	t.Helper()
	var out []int64
	for _, r := range insert(t, w, ps...) {
		out = append(out, r.ID)
	}
	return out
}

func claim(t *testing.T, s driver.Store, n int, queues ...string) []driver.Job {
	t.Helper()
	if len(queues) == 0 {
		queues = []string{"default"}
	}
	js, err := s.Claim(ctx, driver.ClaimQuery{Queues: queues, Limit: n, Server: "s1"})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	return js
}

func claimOne(t *testing.T, s driver.Store) driver.Job {
	t.Helper()
	js := claim(t, s, 1)
	if len(js) != 1 {
		t.Fatalf("claimed %d jobs, want 1", len(js))
	}
	return js[0]
}

func finish(t *testing.T, s driver.Store, outs ...driver.Outcome) []driver.Result {
	t.Helper()
	res, err := s.Finish(ctx, "s1", outs)
	if err != nil {
		t.Fatalf("finish: %v", err)
	}
	return res
}

func done(j driver.Job, st driver.State) driver.Outcome {
	return driver.Outcome{Ref: j.Ref, State: st}
}

func record(t *testing.T, s driver.Store, id int64) driver.Record {
	t.Helper()
	r, err := s.Job(ctx, id)
	if err != nil {
		t.Fatalf("job %d: %v", id, err)
	}
	return r
}

func expectState(t *testing.T, s driver.Store, id int64, want driver.State) {
	t.Helper()
	if got := record(t, s, id).State; got != want {
		t.Fatalf("job %d is %s, want %s", id, got, want)
	}
}

func TestInitialState(t *testing.T) {
	t.Parallel()
	s, c := open(t)
	parents := ids(t, s, params("p"), params("p"), params("p"), params("p"))
	finish(t, s, done(claimOne(t, s), driver.Succeeded))
	finish(t, s, done(claimOne(t, s), driver.Failed))
	if _, err := s.Delete(ctx, driver.Filter{IDs: parents[2:3]}); err != nil {
		t.Fatal(err)
	}
	ok, failed, gone, live := parents[0], parents[1], parents[2], parents[3]

	tests := []struct {
		name string
		p    driver.InsertParams
		want driver.State
	}{
		{"plain", params("a"), driver.Enqueued},
		{"delay", params("a", delay(time.Minute)), driver.Scheduled},
		{"past run at", params("a", func(p *driver.InsertParams) { p.RunAt = c.now().Add(-time.Hour) }), driver.Enqueued},
		{"future run at", params("a", func(p *driver.InsertParams) { p.RunAt = c.now().Add(time.Hour) }), driver.Scheduled},
		{"first limited", params("a", limited("k", 1)), driver.Enqueued},
		{"second limited", params("a", limited("k", 1)), driver.Throttled},
		{"live parent", params("a", after(live, driver.OnSucceeded)), driver.Awaiting},
		{"succeeded parent", params("a", after(ok, driver.OnSucceeded)), driver.Enqueued},
		{"deleted parent", params("a", after(gone, driver.OnSucceeded)), driver.Deleted},
		{"failed parent pending", params("a", after(failed, driver.OnSucceeded)), driver.Awaiting},
		{"failed parent finished", params("a", after(failed, driver.OnFinished)), driver.Enqueued},
		{"succeeded parent failed mask", params("a", after(ok, driver.OnFailed)), driver.Deleted},
		{"resolved parent delayed", params("a", after(ok, driver.OnSucceeded), delay(time.Minute)), driver.Scheduled},
	}
	for _, tt := range tests {
		res := insert(t, s, tt.p)
		if res[0].State != tt.want {
			t.Errorf("%s: inserted as %s, want %s", tt.name, res[0].State, tt.want)
		}
		expectState(t, s, res[0].ID, tt.want)
	}

	res := insert(t, s, params("a", after(gone, driver.OnSucceeded)))
	h := record(t, s, res[0].ID).History
	if len(h) != 1 || h[0].Reason != "parent 3 deleted" || h[0].State != driver.Deleted {
		t.Fatalf("doomed history = %+v", h)
	}
}

func TestInsertValidation(t *testing.T) {
	t.Parallel()
	s, _ := open(t)
	tests := []struct {
		name string
		ps   []driver.InsertParams
		err  error
	}{
		{"empty kind", []driver.InsertParams{params("")}, driver.ErrInvalid},
		{"empty queue", []driver.InsertParams{params("a", queue(""))}, driver.ErrInvalid},
		{"max attempts", []driver.InsertParams{params("a", func(p *driver.InsertParams) { p.MaxAttempts = 0 })}, driver.ErrInvalid},
		{"bad args", []driver.InsertParams{params("a", func(p *driver.InsertParams) { p.Args = []byte(`{`) })}, driver.ErrInvalid},
		{"index range", []driver.InsertParams{params("a"), params("b", needs(2, driver.OnSucceeded))}, driver.ErrInvalid},
		{"self", []driver.InsertParams{params("a", needs(0, driver.OnSucceeded))}, driver.ErrInvalid},
		{"cycle", []driver.InsertParams{params("a", needs(1, driver.OnSucceeded)), params("b", needs(0, driver.OnSucceeded))}, driver.ErrInvalid},
		{"missing parent", []driver.InsertParams{params("a"), params("b", after(999, driver.OnSucceeded))}, driver.ErrNotFound},
		{"missing batch", []driver.InsertParams{params("a"), params("b", inBatch(7))}, driver.ErrNotFound},
		{"missing after batch", []driver.InsertParams{params("a"), params("b", afterBatch(7))}, driver.ErrNotFound},
	}
	for _, tt := range tests {
		if _, err := s.Insert(ctx, tt.ps); !errors.Is(err, tt.err) {
			t.Errorf("%s: err = %v, want %v", tt.name, err, tt.err)
		}
	}
	c, err := s.Counts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if c != (driver.Counts{}) {
		t.Fatalf("failed inserts left rows behind: %+v", c)
	}
}

func TestInsertIDsIncrease(t *testing.T) {
	t.Parallel()
	s, _ := open(t)
	got := ids(t, s, params("a"), params("a"), params("a", needs(3, driver.OnSucceeded)), params("a"))
	for i, id := range got {
		if id <= 0 || i > 0 && id <= got[i-1] {
			t.Fatalf("ids = %v", got)
		}
	}
	expectState(t, s, got[2], driver.Awaiting)
}

func TestClaimOrder(t *testing.T) {
	t.Parallel()
	s, _ := open(t)
	got := ids(t, s,
		params("a", queue("a"), priority(0)),
		params("a", queue("a"), priority(5)),
		params("a", queue("a"), priority(5)),
		params("a", queue("a"), priority(-1)),
		params("a", queue("b"), priority(9)),
	)
	var order []int64
	for _, j := range claim(t, s, 3, "a", "b") {
		order = append(order, j.ID)
	}
	for _, j := range claim(t, s, 10, "a", "b") {
		order = append(order, j.ID)
	}
	want := []int64{got[1], got[2], got[0], got[3], got[4]}
	if !slices.Equal(order, want) {
		t.Fatalf("claim order = %v, want %v", order, want)
	}
}

func TestClaimKindsAndPause(t *testing.T) {
	t.Parallel()
	s, _ := open(t)
	insert(t, s, params("x", priority(9)), params("y"), params("y", queue("other")))
	js, err := s.Claim(ctx, driver.ClaimQuery{Queues: []string{"default"}, Kinds: []string{"y"}, Limit: 5})
	if err != nil {
		t.Fatal(err)
	}
	if len(js) != 1 || js[0].Kind != "y" {
		t.Fatalf("kinds filter claimed %+v", js)
	}
	if err := s.PauseQueue(ctx, "default", true); err != nil {
		t.Fatal(err)
	}
	if js := claim(t, s, 5, "default", "other"); len(js) != 1 || js[0].Queue != "other" {
		t.Fatalf("paused queue claimed %+v", js)
	}
	if err := s.PauseQueue(ctx, "default", false); err != nil {
		t.Fatal(err)
	}
	if js := claim(t, s, 5); len(js) != 1 || js[0].Kind != "x" {
		t.Fatalf("resumed queue claimed %+v", js)
	}
}

func TestFencing(t *testing.T) {
	t.Parallel()
	s, _ := open(t)
	insert(t, s, params("a", func(p *driver.InsertParams) { p.Meta = map[string]string{"a": "1"} }))
	j := claimOne(t, s)
	if j.Attempt != 1 || j.Claim != 1 {
		t.Fatalf("attempt %d claim %d", j.Attempt, j.Claim)
	}
	stale := j.Ref
	stale.Claim++
	if err := s.SetMeta(ctx, stale, map[string]string{"b": "2"}); !errors.Is(err, driver.ErrLost) {
		t.Fatalf("set meta on stale claim: %v", err)
	}
	if err := s.SetMeta(ctx, j.Ref, map[string]string{"b": "2"}); err != nil {
		t.Fatal(err)
	}
	if m := record(t, s, j.ID).Meta; m["a"] != "1" || m["b"] != "2" {
		t.Fatalf("meta = %v", m)
	}
	if r := finish(t, s, driver.Outcome{Ref: stale, State: driver.Succeeded}); r[0] != driver.Stale {
		t.Fatalf("stale finish = %v", r)
	}
	out := done(j, driver.Succeeded)
	if r := finish(t, s, out, out); r[0] != driver.Applied || r[1] != driver.Stale {
		t.Fatalf("finish twice = %v", r)
	}
	if r := finish(t, s, out); r[0] != driver.Stale {
		t.Fatalf("resend = %v", r)
	}
	if err := s.SetMeta(ctx, j.Ref, map[string]string{"c": "3"}); !errors.Is(err, driver.ErrLost) {
		t.Fatalf("set meta after finish: %v", err)
	}
}

func TestOutcomes(t *testing.T) {
	t.Parallel()
	s, c := open(t)
	id := ids(t, s, params("a"))[0]

	j := claimOne(t, s)
	finish(t, s, driver.Outcome{Ref: j.Ref, State: driver.Scheduled, Delay: time.Minute, Refund: true, Reason: "snoozed"})
	r := record(t, s, id)
	if r.State != driver.Scheduled || r.Attempt != 0 || !r.RunAt.Equal(c.now().Add(time.Minute)) {
		t.Fatalf("snooze: %s attempt %d run at %v", r.State, r.Attempt, r.RunAt)
	}
	if _, err := s.Requeue(ctx, driver.Filter{IDs: []int64{id}}); err != nil {
		t.Fatal(err)
	}

	j = claimOne(t, s)
	if j.Attempt != 1 || j.Claim != 2 {
		t.Fatalf("after refund attempt %d claim %d", j.Attempt, j.Claim)
	}
	finish(t, s, driver.Outcome{Ref: j.Ref, State: driver.Scheduled, Reason: "retry"})
	expectState(t, s, id, driver.Enqueued)

	j = claimOne(t, s)
	finish(t, s, driver.Outcome{Ref: j.Ref, State: driver.Enqueued, Refund: true, Reason: "shutdown"})
	if r := record(t, s, id); r.State != driver.Enqueued || r.Attempt != 1 {
		t.Fatalf("shutdown: %s attempt %d", r.State, r.Attempt)
	}

	j = claimOne(t, s)
	if res := finish(t, s, driver.Outcome{Ref: j.Ref, State: driver.Succeeded, Output: []byte(`{`)}); res[0] != driver.Rejected {
		t.Fatalf("bad output = %v", res)
	}
	if n, err := s.Delete(ctx, driver.Filter{IDs: []int64{id}}); err != nil || n != 1 {
		t.Fatalf("delete processing = %d, %v", n, err)
	}
	if r := record(t, s, id); r.State != driver.Processing || !r.CancelRequested {
		t.Fatalf("cancel: %s %v", r.State, r.CancelRequested)
	}
	finish(t, s, driver.Outcome{Ref: j.Ref, State: driver.Failed, Reason: "exhausted"})
	r = record(t, s, id)
	if r.State != driver.Deleted || r.FinalizedAt.IsZero() {
		t.Fatalf("cancel coercion: %s", r.State)
	}
	var reasons []string
	for _, e := range r.History {
		reasons = append(reasons, e.Reason)
	}
	if want := []string{"snoozed", "requeued", "retry", "shutdown", "exhausted"}; !slices.Equal(reasons, want) {
		t.Fatalf("history = %v, want %v", reasons, want)
	}
	if h := r.History[len(r.History)-1]; h.State != driver.Deleted || h.Server != "s1" {
		t.Fatalf("last entry = %+v", h)
	}

	id = ids(t, s, params("a"))[0]
	j = claimOne(t, s)
	if _, err := s.Delete(ctx, driver.Filter{IDs: []int64{id}}); err != nil {
		t.Fatal(err)
	}
	finish(t, s, driver.Outcome{Ref: j.Ref, State: driver.Succeeded, Output: []byte(`{"ok":true}`)})
	if r := record(t, s, id); r.State != driver.Succeeded || string(r.Output) != `{"ok":true}` || len(r.History) != 0 {
		t.Fatalf("succeeded with cancel requested: %s %s %v", r.State, r.Output, r.History)
	}
}

func TestHistoryCap(t *testing.T) {
	t.Parallel()
	s, _ := open(t)
	id := ids(t, s, params("a", func(p *driver.InsertParams) { p.MaxAttempts = 100 }))[0]
	long := strings.Repeat("é", 3000)
	for range 20 {
		j := claimOne(t, s)
		finish(t, s, driver.Outcome{Ref: j.Ref, State: driver.Scheduled, Reason: "retry", Error: long, Trace: long + long + long})
	}
	h := record(t, s, id).History
	if len(h) != 16 {
		t.Fatalf("history has %d entries", len(h))
	}
	if h[0].Attempt != 5 || h[15].Attempt != 20 {
		t.Fatalf("kept attempts %d..%d, want 5..20", h[0].Attempt, h[15].Attempt)
	}
	if e := h[15]; len(e.Error) > 2048 || len(e.Trace) > 8192 || !strings.HasPrefix(long, e.Error) || len(e.Error) < 2046 {
		t.Fatalf("error %d bytes, trace %d bytes", len(e.Error), len(e.Trace))
	}
}

func TestConcurrentClaim(t *testing.T) {
	t.Parallel()
	s, _ := open(t)
	batch := make([]driver.InsertParams, 2000)
	for i := range batch {
		batch[i] = params("a", priority(int16(i%7)))
	}
	insert(t, s, batch...)
	var (
		mu   sync.Mutex
		seen = make(map[int64]int)
		wg   sync.WaitGroup
	)
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				js, err := s.Claim(ctx, driver.ClaimQuery{Queues: []string{"default"}, Limit: 7, Server: "s"})
				if err != nil {
					t.Error(err)
					return
				}
				if len(js) == 0 {
					return
				}
				mu.Lock()
				for _, j := range js {
					seen[j.ID]++
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if len(seen) != len(batch) {
		t.Fatalf("claimed %d distinct jobs, want %d", len(seen), len(batch))
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("job %d claimed %d times", id, n)
		}
	}
}

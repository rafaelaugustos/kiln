package sqlitestore_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln"
	"github.com/rafaelaugustos/kiln/driver"
	"github.com/rafaelaugustos/kiln/sqlitestore"
)

type double struct{ N int }

func (double) Kind() string { return "double" }

type step struct {
	Name  string
	Sleep time.Duration
}

func (step) Kind() string { return "step" }

type hold struct{}

func (hold) Kind() string { return "hold" }

type cluster struct {
	tb testing.TB
	db *sql.DB
}

func newCluster(tb testing.TB) *cluster {
	tb.Helper()
	return &cluster{tb: tb, db: sqlitestore.Database(tb)}
}

func (c *cluster) store(opts ...sqlitestore.Option) *sqlitestore.Store {
	c.tb.Helper()
	s, err := sqlitestore.New(context.Background(), c.db, opts...)
	if err != nil {
		c.tb.Fatal(err)
	}
	c.tb.Cleanup(s.Close)
	return s
}

func fast() kiln.ServerConfig {
	return kiln.ServerConfig{
		Queues:            map[string]int{kiln.DefaultQueue: 8},
		PollInterval:      50 * time.Millisecond,
		FetchCooldown:     time.Millisecond,
		ShutdownTimeout:   time.Second,
		KillGrace:         100 * time.Millisecond,
		HeartbeatInterval: 100 * time.Millisecond,
		DeadAfter:         5400 * time.Millisecond,
		LeaderTTL:         time.Second,
		Backoff:           kiln.Constant(10 * time.Millisecond),
	}
}

func serve(tb testing.TB, st driver.Store, m *kiln.Mux, cfg kiln.ServerConfig) (*kiln.Server, func()) {
	tb.Helper()
	s, err := kiln.NewServer(kiln.NewClient(st), m, cfg)
	if err != nil {
		tb.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			if err := <-done; err != nil {
				tb.Errorf("run: %v", err)
			}
		})
	}
	tb.Cleanup(stop)
	waitFor(tb, 5*time.Second, "server start", func() bool { return s.Healthy() == nil })
	return s, stop
}

func waitFor(tb testing.TB, d time.Duration, what string, cond func() bool) {
	tb.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			tb.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func recv[T any](tb testing.TB, ch <-chan T, d time.Duration) T {
	tb.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(d):
		tb.Fatalf("nothing received in %s", d)
	}
	var zero T
	return zero
}

func send[T any](ch chan<- T, v T) {
	select {
	case ch <- v:
	default:
	}
}

func get(tb testing.TB, c *kiln.Client, id int64) kiln.Record {
	tb.Helper()
	r, err := c.Get(context.Background(), id)
	if err != nil {
		tb.Fatalf("job %d: %v", id, err)
	}
	return r
}

func waitState(tb testing.TB, c *kiln.Client, id int64, st kiln.State) kiln.Record {
	tb.Helper()
	var r kiln.Record
	waitFor(tb, 10*time.Second, fmt.Sprintf("job %d to be %s", id, st), func() bool {
		r = get(tb, c, id)
		return r.State == st
	})
	return r
}

func enqueue(tb testing.TB, c *kiln.Client, args kiln.Args, opts ...kiln.InsertOption) int64 {
	tb.Helper()
	id, err := c.Enqueue(context.Background(), args, opts...)
	if err != nil {
		tb.Fatal(err)
	}
	return id
}

func reasons(r kiln.Record) []string {
	var out []string
	for _, e := range r.History {
		out = append(out, e.Reason)
	}
	return out
}

type trace struct {
	mu     sync.Mutex
	events []string
}

func (tr *trace) add(e string) {
	tr.mu.Lock()
	tr.events = append(tr.events, e)
	tr.mu.Unlock()
}

func (tr *trace) has(e string) bool {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return slices.Contains(tr.events, e)
}

func (tr *trace) before(tb testing.TB, a, b string) {
	tb.Helper()
	tr.mu.Lock()
	defer tr.mu.Unlock()
	i, j := slices.Index(tr.events, a), slices.Index(tr.events, b)
	if i < 0 || j < 0 || i > j {
		tb.Fatalf("want %q before %q in %v", a, b, tr.events)
	}
}

func steps(tr *trace) *kiln.Mux {
	m := kiln.NewMux()
	kiln.Handle(m, func(ctx context.Context, j *kiln.Job[step]) error {
		tr.add("start " + j.Args.Name)
		time.Sleep(j.Args.Sleep)
		tr.add("end " + j.Args.Name)
		return nil
	})
	return m
}

var errCrashed = errors.New("process crashed")

type crashable struct {
	driver.Store
	down atomic.Bool
}

func (c *crashable) Heartbeat(ctx context.Context, info driver.ServerInfo) (driver.Directives, error) {
	if c.down.Load() {
		return driver.Directives{}, errCrashed
	}
	return c.Store.Heartbeat(ctx, info)
}

func (c *crashable) Claim(ctx context.Context, q driver.ClaimQuery) ([]driver.Job, error) {
	if c.down.Load() {
		return nil, errCrashed
	}
	return c.Store.Claim(ctx, q)
}

func (c *crashable) Finish(ctx context.Context, server string, outs []driver.Outcome) ([]driver.Result, error) {
	if c.down.Load() {
		return nil, errCrashed
	}
	return c.Store.Finish(ctx, server, outs)
}

func (c *crashable) Promote(ctx context.Context, limit int) (driver.Promoted, error) {
	if c.down.Load() {
		return driver.Promoted{}, errCrashed
	}
	return c.Store.Promote(ctx, limit)
}

func (c *crashable) SetMeta(ctx context.Context, ref driver.Ref, meta map[string]string) error {
	if c.down.Load() {
		return errCrashed
	}
	return c.Store.SetMeta(ctx, ref, meta)
}

func (c *crashable) Unregister(ctx context.Context, server string) error {
	if c.down.Load() {
		return errCrashed
	}
	return c.Store.Unregister(ctx, server)
}

func TestE2ESucceeds(t *testing.T) {
	t.Parallel()
	st := newCluster(t).store()
	m := kiln.NewMux()
	kiln.Handle(m, func(ctx context.Context, j *kiln.Job[double]) error {
		return j.SetOutput(map[string]int{"n": j.Args.N * 2})
	})
	srv, _ := serve(t, st, m, fast())
	waitFor(t, 5*time.Second, "listen", func() bool { return srv.Stats().Listening })
	cl := kiln.NewClient(st)
	id := enqueue(t, cl, double{N: 21})
	r := waitState(t, cl, id, kiln.Succeeded)
	var out struct{ N int }
	if err := json.Unmarshal(r.Output, &out); err != nil || out.N != 42 {
		t.Fatalf("output %s: %v", r.Output, err)
	}
	if r.Attempt != 1 || r.FinalizedAt.IsZero() {
		t.Fatalf("attempt %d finalized %v", r.Attempt, r.FinalizedAt)
	}
}

func TestE2ERetryThenFails(t *testing.T) {
	t.Parallel()
	st := newCluster(t).store()
	var calls atomic.Int32
	m := kiln.NewMux()
	kiln.Handle(m, func(context.Context, *kiln.Job[double]) error {
		calls.Add(1)
		return errors.New("boom")
	})
	serve(t, st, m, fast())
	cl := kiln.NewClient(st)
	id := enqueue(t, cl, double{}, kiln.MaxAttempts(3))
	r := waitState(t, cl, id, kiln.Failed)
	if want := []string{"retry", "retry", "exhausted"}; !slices.Equal(reasons(r), want) {
		t.Fatalf("reasons %v, want %v", reasons(r), want)
	}
	if r.Attempt != 3 || calls.Load() != 3 {
		t.Fatalf("attempt %d calls %d", r.Attempt, calls.Load())
	}
	for i, e := range r.History {
		if e.Error != "boom" || e.Attempt != i+1 || e.Server == "" {
			t.Fatalf("history[%d] = %+v", i, e)
		}
	}
	if s := r.History[2].State; s != kiln.Failed {
		t.Fatalf("final history state %s", s)
	}
}

func TestE2EFlow(t *testing.T) {
	t.Parallel()
	st := newCluster(t).store()
	tr := &trace{}
	serve(t, st, steps(tr), fast())
	cl := kiln.NewClient(st)

	parent := enqueue(t, cl, step{Name: "parent", Sleep: 100 * time.Millisecond})
	child := enqueue(t, cl, step{Name: "child"}, kiln.After{parent})

	var f kiln.Flow
	x := f.Add(step{Name: "x", Sleep: 50 * time.Millisecond})
	y := f.Add(step{Name: "y", Sleep: 150 * time.Millisecond})
	z := f.Add(step{Name: "z"}, kiln.Needs{x, y})
	res, err := cl.EnqueueMany(context.Background(), f...)
	if err != nil {
		t.Fatal(err)
	}
	if res[z].State != kiln.Awaiting {
		t.Fatalf("fan-in job inserted as %s", res[z].State)
	}

	if r := waitState(t, cl, child, kiln.Succeeded); !slices.Equal(r.Parents, []int64{parent}) {
		t.Fatalf("child parents %v", r.Parents)
	}
	r := waitState(t, cl, res[z].ID, kiln.Succeeded)
	if got := slices.Sorted(slices.Values(r.Parents)); !slices.Equal(got, []int64{res[x].ID, res[y].ID}) {
		t.Fatalf("fan-in parents %v", r.Parents)
	}
	tr.before(t, "end parent", "start child")
	tr.before(t, "end x", "start z")
	tr.before(t, "end y", "start z")
}

func TestE2EBatchThen(t *testing.T) {
	t.Parallel()
	st := newCluster(t).store()
	tr := &trace{}
	serve(t, st, steps(tr), fast())
	cl := kiln.NewClient(st)
	ctx := context.Background()

	b := &kiln.Batch{Description: "import"}
	for i := range 3 {
		b.Add(step{Name: fmt.Sprint("part", i), Sleep: time.Duration(i+1) * 40 * time.Millisecond})
	}
	b.Then(step{Name: "report"})
	id, err := cl.StartBatch(ctx, b)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 10*time.Second, "batch continuation", func() bool { return tr.has("end report") })
	for i := range 3 {
		tr.before(t, fmt.Sprint("end part", i), "start report")
	}
	bt, err := st.Batch(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if bt.Total != 3 || !bt.Sealed || bt.FinishedAt.IsZero() || bt.Counts[kiln.Succeeded] != 3 {
		t.Fatalf("batch %+v", bt)
	}
}

func TestE2EOrphanRescue(t *testing.T) {
	t.Parallel()
	c := newCluster(t)
	a := &crashable{Store: c.store()}
	started := make(chan int64, 1)
	held := kiln.NewMux()
	kiln.Handle(held, func(ctx context.Context, j *kiln.Job[hold]) error {
		send(started, j.ID)
		<-ctx.Done()
		return ctx.Err()
	})
	cfg := fast()
	cfg.DisableMaintenance = true
	srvA, _ := serve(t, a, held, cfg)
	cl := kiln.NewClient(c.store())
	id := enqueue(t, cl, hold{}, kiln.MaxAttempts(3))
	recv(t, started, 5*time.Second)
	a.down.Store(true)

	rescued := make(chan *kiln.Job[hold], 1)
	m := kiln.NewMux()
	kiln.Handle(m, func(_ context.Context, j *kiln.Job[hold]) error {
		send(rescued, j)
		return nil
	})
	srvB, _ := serve(t, c.store(), m, fast())

	j := recv(t, rescued, 15*time.Second)
	if j.ID != id || j.Attempt != 2 {
		t.Fatalf("rescued job %d attempt %d, want %d attempt 2", j.ID, j.Attempt, id)
	}
	r := waitState(t, cl, id, kiln.Succeeded)
	if r.Server != srvB.ID() {
		t.Fatalf("finished by %q, want %q", r.Server, srvB.ID())
	}
	i := slices.IndexFunc(r.History, func(e kiln.Entry) bool { return e.Reason == "orphaned" })
	if i < 0 || !strings.Contains(r.History[i].Error, srvA.ID()) {
		t.Fatalf("history %+v", r.History)
	}
}

func TestE2ELimitAcrossServers(t *testing.T) {
	t.Parallel()
	c := newCluster(t)
	var running, peak atomic.Int32
	for range 3 {
		m := kiln.NewMux()
		kiln.Handle(m, func(context.Context, *kiln.Job[step]) error {
			n := running.Add(1)
			for {
				p := peak.Load()
				if n <= p || peak.CompareAndSwap(p, n) {
					break
				}
			}
			time.Sleep(30 * time.Millisecond)
			running.Add(-1)
			return nil
		})
		serve(t, c.store(), m, fast())
	}
	cl := kiln.NewClient(c.store())
	specs := make([]kiln.Spec, 12)
	for i := range specs {
		specs[i] = kiln.Spec{Args: step{Name: fmt.Sprint(i)}, Options: []kiln.InsertOption{kiln.Limit{Key: "export", Max: 2}}}
	}
	res, err := cl.EnqueueMany(context.Background(), specs...)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range res {
		waitState(t, cl, r.ID, kiln.Succeeded)
	}
	if p := peak.Load(); p < 1 || p > 2 {
		t.Fatalf("peak concurrency %d, limit 2", p)
	}
}

func TestE2EUnique(t *testing.T) {
	t.Parallel()
	st := newCluster(t).store()
	var calls atomic.Int32
	m := kiln.NewMux()
	kiln.Handle(m, func(context.Context, *kiln.Job[double]) error {
		calls.Add(1)
		return nil
	})
	serve(t, st, m, fast())
	cl := kiln.NewClient(st)
	u := kiln.Unique{Key: "report:42", For: time.Minute}
	a := enqueue(t, cl, double{N: 1}, u)
	if b := enqueue(t, cl, double{N: 2}, u); b != a {
		t.Fatalf("duplicate got id %d, want %d", b, a)
	}
	waitState(t, cl, a, kiln.Succeeded)
	res, err := cl.EnqueueMany(context.Background(), kiln.Spec{Args: double{N: 3}, Options: []kiln.InsertOption{u}})
	if err != nil {
		t.Fatal(err)
	}
	if res[0].ID != a || !res[0].Duplicate {
		t.Fatalf("after success: %+v, want duplicate of %d", res[0], a)
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("handler ran %d times", n)
	}
}

func TestE2ERecurring(t *testing.T) {
	t.Parallel()
	st := newCluster(t).store()
	fired := make(chan *kiln.Job[double], 8)
	m := kiln.NewMux()
	kiln.Handle(m, func(_ context.Context, j *kiln.Job[double]) error {
		send(fired, j)
		return nil
	})
	serve(t, st, m, fast())
	cl := kiln.NewClient(st)
	if err := cl.SetRecurring(context.Background(), "tick", "@every 1s", double{N: 7}); err != nil {
		t.Fatal(err)
	}
	var ids []int64
	for range 2 {
		j := recv(t, fired, 5*time.Second)
		if j.RecurringID != "tick" || j.Args.N != 7 {
			t.Fatalf("fired %+v", j)
		}
		ids = append(ids, j.ID)
	}
	if ids[0] == ids[1] {
		t.Fatalf("same job fired twice: %v", ids)
	}
	r, err := st.Recurring(context.Background(), "tick")
	if err != nil {
		t.Fatal(err)
	}
	if r.LastJobID < ids[1] || r.NextRunAt.IsZero() {
		t.Fatalf("recurring %+v", r)
	}
}

func TestE2ETxEnqueue(t *testing.T) {
	t.Parallel()
	c := newCluster(t)
	st := c.store()
	ran := make(chan int64, 4)
	m := kiln.NewMux()
	kiln.Handle(m, func(_ context.Context, j *kiln.Job[double]) error {
		send(ran, j.ID)
		return nil
	})
	cfg := fast()
	cfg.PollInterval = time.Minute
	srv, _ := serve(t, st, m, cfg)
	waitFor(t, 5*time.Second, "listen", func() bool { return srv.Stats().Listening })
	cl := kiln.NewClient(st)
	ctx := context.Background()

	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	gone, err := cl.EnqueueTx(ctx, st.Tx(tx), double{N: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if _, err := cl.Get(ctx, gone); !errors.Is(err, kiln.ErrNotFound) {
		t.Fatalf("rolled back job: %v", err)
	}

	tx, err = c.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	w := st.Tx(tx)
	id, err := cl.EnqueueTx(ctx, w, double{N: 2})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cl.Get(ctx, id); !errors.Is(err, kiln.ErrNotFound) {
		t.Fatalf("uncommitted job visible: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := w.Notify(ctx); err != nil {
		t.Fatal(err)
	}
	if got := recv(t, ran, 3*time.Second); got != id {
		t.Fatalf("ran %d, want %d", got, id)
	}
	waitState(t, cl, id, kiln.Succeeded)
}

type ping struct{ N int }

func (ping) Kind() string { return "ping" }

func TestE2ERateLimit(t *testing.T) {
	for attempt := 1; ; attempt++ {
		bad, late := rateLimitRun(t)
		if bad == "" {
			return
		}
		if late < 50*time.Millisecond || attempt == 3 {
			t.Fatalf("%s (latest start %v after its slot)", bad, late)
		}
		t.Logf("attempt %d: %s after a %v stall, retrying", attempt, bad, late)
	}
}

func rateLimitRun(t *testing.T) (string, time.Duration) {
	st := newCluster(t).store()
	type start struct {
		id int64
		at time.Time
	}
	const n = 30
	var (
		mu     sync.Mutex
		starts []start
		all    = make(chan struct{})
	)
	m := kiln.NewMux()
	kiln.Handle(m, func(_ context.Context, j *kiln.Job[ping]) error {
		mu.Lock()
		defer mu.Unlock()
		if starts = append(starts, start{j.ID, time.Now()}); len(starts) == n {
			close(all)
		}
		return nil
	})
	cfg := fast()
	cfg.PollInterval = time.Second
	cfg.DisableMaintenance = true
	srv, _ := serve(t, st, m, cfg)
	waitFor(t, 5*time.Second, "listen", func() bool { return srv.Stats().Listening })
	cl := kiln.NewClient(st)
	limit := kiln.Limit{Key: "api", Rate: 20, Per: time.Second, Burst: 1}
	specs := make([]kiln.Spec, n)
	for i := range specs {
		specs[i] = kiln.Spec{Args: ping{N: i}, Options: []kiln.InsertOption{limit}}
	}
	res, err := cl.EnqueueMany(context.Background(), specs...)
	if err != nil {
		t.Fatal(err)
	}
	recv(t, all, 10*time.Second)
	slots := make(map[int64]time.Time, len(res))
	for _, r := range res {
		slots[r.ID] = waitState(t, cl, r.ID, kiln.Succeeded).RunAt
	}
	mu.Lock()
	defer mu.Unlock()
	if len(starts) != n {
		t.Fatalf("%d handler starts for %d jobs", len(starts), n)
	}
	var late time.Duration
	for _, s := range starts {
		slot := slots[s.id]
		if s.at.Before(slot.Add(-2 * time.Millisecond)) {
			t.Fatalf("job %d started at %v, before its slot at %v", s.id, s.at, slot)
		}
		late = max(late, s.at.Sub(slot))
	}
	slices.SortFunc(starts, func(a, b start) int { return a.at.Compare(b.at) })
	for i := 5; i < len(starts); i++ {
		if d := starts[i].at.Sub(starts[i-5].at); d < 200*time.Millisecond {
			return fmt.Sprintf("starts %d to %d within %v, want at most 5 in any 200ms window", i-5, i, d), late
		}
	}
	return "", late
}

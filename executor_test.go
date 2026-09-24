package kiln

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
	"github.com/rafaelaugustos/kiln/memstore"
)

func TestServerOutcomes(t *testing.T) {
	t.Parallel()
	c := NewClient(memstore.New())
	m := NewMux()
	var deadlineCalls, snoozes atomic.Int32
	m.HandleFunc("permanent", func(context.Context, *RawJob) error { return Permanent(errors.New("bad input")) })
	m.HandleFunc("cancel", func(context.Context, *RawJob) error { return Cancel(errors.New("not needed")) })
	m.HandleFunc("snooze", func(context.Context, *RawJob) error {
		if snoozes.Add(1) == 1 {
			return Snooze(20 * time.Millisecond)
		}
		return nil
	})
	m.HandleFunc("panic", func(context.Context, *RawJob) error { panic("kaboom") })
	m.HandleFunc("timeout", func(ctx context.Context, _ *RawJob) error {
		<-ctx.Done()
		if cause := context.Cause(ctx); cause != ErrTimeout {
			return Permanent(fmt.Errorf("cause %v", cause))
		}
		return ctx.Err()
	})
	m.HandleFunc("deadline", func(context.Context, *RawJob) error {
		deadlineCalls.Add(1)
		return nil
	})
	m.HandleFunc("output", func(_ context.Context, j *RawJob) error {
		return j.SetOutput(strings.Repeat("x", maxOutput))
	})
	runServer(t, c, m, fastConfig())

	cases := []struct {
		kind    string
		opts    []InsertOption
		state   State
		reasons []string
		errText string
	}{
		{"permanent", nil, Failed, []string{"permanent"}, "bad input"},
		{"cancel", nil, Deleted, []string{"canceled"}, "not needed"},
		{"snooze", nil, Succeeded, []string{"snoozed"}, "snoozed"},
		{"panic", []InsertOption{MaxAttempts(1)}, Failed, []string{"exhausted"}, "kiln: panic: kaboom"},
		{"timeout", []InsertOption{MaxAttempts(1), Timeout(20 * time.Millisecond)}, Failed, []string{"exhausted"}, "timed out"},
		{"deadline", []InsertOption{Delay(80 * time.Millisecond), Deadline(time.Now().Add(20 * time.Millisecond))}, Deleted, []string{"deadline"}, ""},
		{"output", nil, Failed, []string{"output rejected"}, "output rejected"},
	}
	for _, tc := range cases {
		t.Run(tc.kind, func(t *testing.T) {
			t.Parallel()
			id := mustEnqueue(t, c, testArgs{K: tc.kind}, tc.opts...)
			r := waitState(t, c, id, tc.state)
			if !slices.Equal(reasons(r), tc.reasons) {
				t.Fatalf("reasons = %v, want %v", reasons(r), tc.reasons)
			}
			if r.Attempt != 1 {
				t.Fatalf("attempt = %d, want 1", r.Attempt)
			}
			if e := r.History[0].Error; !strings.Contains(e, tc.errText) {
				t.Fatalf("error = %q, want %q", e, tc.errText)
			}
			if tc.kind == "panic" && !strings.Contains(r.History[0].Trace, "goroutine") {
				t.Fatalf("trace = %q", r.History[0].Trace)
			}
		})
	}
	t.Cleanup(func() {
		if n := deadlineCalls.Load(); n != 0 {
			t.Errorf("deadline handler ran %d times", n)
		}
	})
}

func TestDispatch(t *testing.T) {
	t.Parallel()
	m := NewMux()
	var calls atomic.Int32
	Handle(m, func(ctx context.Context, j *Job[ping]) error {
		calls.Add(1)
		if raw, ok := JobFrom(ctx); !ok || raw.Kind != "ping" {
			return errors.New("no job in context")
		}
		if _, ok := ClientFrom(ctx); ok {
			return errors.New("unexpected client")
		}
		switch j.Args.N {
		case 0:
			return j.SetOutput(j.Args)
		case 1:
			return errors.New("try again")
		case 2:
			return Snooze(time.Hour)
		case 3:
			<-ctx.Done()
			return ctx.Err()
		}
		return Permanent(errors.New("no"))
	}, Constant(time.Second))

	cases := []struct {
		args   ping
		opts   []InsertOption
		state  State
		delay  time.Duration
		refund bool
		is     error
	}{
		{ping{N: 0}, nil, Succeeded, 0, false, nil},
		{ping{N: 1}, nil, Scheduled, time.Second, false, nil},
		{ping{N: 1}, []InsertOption{MaxAttempts(1)}, Failed, 0, false, nil},
		{ping{N: 2}, nil, Scheduled, time.Hour, true, ErrSnoozed},
		{ping{N: 3}, []InsertOption{Timeout(time.Millisecond)}, Scheduled, time.Second, false, ErrTimeout},
		{ping{N: 4}, nil, Failed, 0, false, ErrPermanent},
		{ping{N: 0}, []InsertOption{Deadline(time.Now().Add(-time.Second))}, Deleted, 0, false, nil},
	}
	for i, tc := range cases {
		o, err := m.dispatch(context.Background(), tc.args, tc.opts...)
		if o.State != tc.state || o.Delay != tc.delay || o.Refund != tc.refund {
			t.Errorf("%d: outcome %+v", i, o)
		}
		if tc.is != nil && !errors.Is(err, tc.is) {
			t.Errorf("%d: err = %v, want %v", i, err, tc.is)
		}
	}
	if o, _ := m.dispatch(context.Background(), ping{N: 0}); string(o.Output) != `{"N":0}` {
		t.Errorf("output = %s", o.Output)
	}
	if n := calls.Load(); n != int32(len(cases)) {
		t.Errorf("calls = %d, want %d", n, len(cases))
	}
}

func TestUnknownKind(t *testing.T) {
	t.Parallel()
	m := NewMux()
	m.HandleFunc("known", func(context.Context, *RawJob) error { return nil })
	o, err := m.dispatch(context.Background(), ping{})
	if err == nil || o.State != Scheduled || !o.Refund || o.Delay != time.Minute || o.Reason != "unknown kind" {
		t.Fatalf("outcome %+v err %v", o, err)
	}
	old := &task{job: &RawJob{Kind: "ping", CreatedAt: time.Now().Add(-25 * time.Hour)}}
	old.Context, old.cancel = context.WithCancelCause(context.Background())
	if o, _ := m.exec(old, &ServerConfig{UnknownKindTTL: defaultUnknownKindTTL}); o.State != Failed || o.Refund || o.Reason != "unknown kind" {
		t.Fatalf("expired outcome %+v", o)
	}
}

func TestClean(t *testing.T) {
	t.Parallel()
	long := strings.Repeat("é", maxError)
	cases := []struct {
		in, want string
		n        int
	}{
		{"bareStore", "bareStore", maxError},
		{"a\x00b\x00", "ab", maxError},
		{"bad \xff byte", "bad � byte", maxError},
		{long, long[:maxError], maxError},
		{"héllo", "h", 2},
	}
	for _, tc := range cases {
		if got := clean(tc.in, tc.n); got != tc.want {
			t.Errorf("clean(%q, %d) = %q, want %q", tc.in, tc.n, got, tc.want)
		}
	}
}

func TestTimeoutResolution(t *testing.T) {
	t.Parallel()
	cases := []struct {
		job, handler, server, want time.Duration
	}{
		{time.Second, time.Minute, time.Hour, time.Second},
		{-1, time.Minute, time.Hour, -1},
		{0, time.Minute, time.Hour, time.Minute},
		{0, -1, time.Hour, -1},
		{0, 0, time.Hour, time.Hour},
		{0, 0, -1, -1},
	}
	for _, tc := range cases {
		r := &route{timeout: tc.handler}
		if got := r.timeoutFor(tc.job, tc.server); got != tc.want {
			t.Errorf("timeoutFor(%s, %s, %s) = %s, want %s", tc.job, tc.handler, tc.server, got, tc.want)
		}
	}
}

func TestOrphanOutcome(t *testing.T) {
	t.Parallel()
	m := NewMux()
	m.HandleFunc("slow", func(context.Context, *RawJob) error { return nil }, Constant(time.Minute))
	s := &Server{mux: m}
	cases := []struct {
		o     driver.Orphan
		state State
		delay time.Duration
	}{
		{driver.Orphan{Kind: "slow", Attempt: 1, MaxAttempts: 3}, Scheduled, time.Minute},
		{driver.Orphan{Kind: "slow", Attempt: 3, MaxAttempts: 3}, Failed, 0},
		{driver.Orphan{Kind: "slow", Attempt: 1, MaxAttempts: 3, Cancel: true}, Deleted, 0},
	}
	for _, tc := range cases {
		o := s.orphaned(tc.o)
		if o.State != tc.state || o.Delay != tc.delay || o.Reason != "orphaned" {
			t.Errorf("orphaned(%+v) = %+v", tc.o, o)
		}
	}
}

type nilErr struct{ msg string }

func (e *nilErr) Error() string { return e.msg }

func TestOutcomeRecoversPanics(t *testing.T) {
	t.Parallel()
	defer func() {
		if v := recover(); v != nil {
			t.Fatalf("panic escaped: %v", v)
		}
	}()
	snoozeOnly := Backoff(func(_ int, err error) time.Duration { return err.(*snoozeError).d })
	m := NewMux()
	m.HandleFunc("backoff", func(context.Context, *RawJob) error { return errors.New("x") }, snoozeOnly)
	m.HandleFunc("nil", func(context.Context, *RawJob) error {
		var e *nilErr
		return e
	})
	for _, kind := range []string{"backoff", "nil"} {
		o, err := m.dispatch(context.Background(), testArgs{K: kind})
		var pe *PanicError
		if o.State != Scheduled || o.Reason != "retry" || o.Delay <= 0 || !strings.Contains(o.Error, "kiln: panic") || !strings.Contains(o.Trace, "goroutine") || !errors.As(err, &pe) {
			t.Errorf("%s: outcome %+v err %v", kind, o, err)
		}
	}
	s := &Server{mux: m, log: slog.New(slog.DiscardHandler), logged: make(map[string]time.Time)}
	if o := s.orphaned(driver.Orphan{Kind: "backoff", Attempt: 1, MaxAttempts: 3}); o.State != Scheduled || o.Delay <= 0 {
		t.Errorf("orphaned outcome %+v", o)
	}
}

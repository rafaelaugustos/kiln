package kilntest_test

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln"
	"github.com/rafaelaugustos/kiln/kilntest"
	"github.com/rafaelaugustos/kiln/memstore"
)

type signup struct {
	Email string `json:"email"`
	Plan  string `json:"plan"`
}

func (signup) Kind() string { return "signup" }

func mux() *kiln.Mux {
	m := kiln.NewMux()
	kiln.Handle(m, func(ctx context.Context, j *kiln.Job[signup]) error {
		switch j.Args.Plan {
		case "pro":
			return j.SetOutput(map[string]string{"welcome": j.Args.Email})
		case "later":
			return kiln.Snooze(time.Minute)
		case "bad":
			return kiln.Permanent(errors.New("unknown plan"))
		case "boom":
			panic("boom")
		case "slow":
			<-ctx.Done()
			return ctx.Err()
		}
		return fmt.Errorf("plan %q unavailable", j.Args.Plan)
	}, kiln.Constant(time.Second))
	return m
}

func TestWork(t *testing.T) {
	t.Parallel()
	m := mux()
	ctx := context.Background()
	cases := []struct {
		plan  string
		opts  []kiln.InsertOption
		state kiln.State
		delay time.Duration
		check func(error) bool
	}{
		{"pro", nil, kiln.Succeeded, 0, func(err error) bool { return err == nil }},
		{"free", nil, kiln.Scheduled, time.Second, func(err error) bool { return strings.Contains(err.Error(), "unavailable") }},
		{"free", []kiln.InsertOption{kiln.MaxAttempts(1)}, kiln.Failed, 0, func(err error) bool { return err != nil }},
		{"later", nil, kiln.Scheduled, time.Minute, func(err error) bool { return errors.Is(err, kiln.ErrSnoozed) }},
		{"bad", nil, kiln.Failed, 0, func(err error) bool { return errors.Is(err, kiln.ErrPermanent) }},
		{"boom", nil, kiln.Scheduled, time.Second, func(err error) bool {
			var p *kiln.PanicError
			return errors.As(err, &p) && p.Value == "boom"
		}},
		{"slow", []kiln.InsertOption{kiln.Timeout(time.Millisecond)}, kiln.Scheduled, time.Second, func(err error) bool { return errors.Is(err, kiln.ErrTimeout) }},
	}
	for _, tc := range cases {
		r := kilntest.Work(ctx, m, signup{Email: "a@b.c", Plan: tc.plan}, tc.opts...)
		if r.State != tc.state || r.Delay != tc.delay || !tc.check(r.Err) {
			t.Errorf("%s: %+v", tc.plan, r)
		}
	}
	r := kilntest.Work(ctx, m, signup{Email: "a@b.c", Plan: "pro"})
	if string(r.Output) != `{"welcome":"a@b.c"}` {
		t.Errorf("output = %s", r.Output)
	}
}

func TestRequireEnqueued(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	c := kiln.NewClient(memstore.New())
	if _, err := c.Enqueue(ctx, signup{Email: "now@x.io", Plan: "pro"}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Enqueue(ctx, signup{Email: "later@x.io", Plan: "free"}, kiln.Delay(time.Hour)); err != nil {
		t.Fatal(err)
	}
	j := kilntest.RequireEnqueued(t, c, func(j *kiln.Job[signup]) bool { return j.Args.Plan == "free" })
	if j.Args.Email != "later@x.io" || j.Kind != "signup" || j.ID == 0 {
		t.Fatalf("job %+v", j)
	}
	if j := kilntest.RequireEnqueued[signup](t, c, nil); j.Args.Email != "now@x.io" {
		t.Fatalf("job %+v", j)
	}
	kilntest.RequireNotEnqueued(t, c, func(j *kiln.Job[signup]) bool { return j.Args.Plan == "team" })

	var fake fakeTB
	fake.run(func() {
		kilntest.RequireEnqueued(&fake, c, func(j *kiln.Job[signup]) bool { return j.Args.Plan == "team" })
	})
	if !strings.Contains(fake.msg, "no matching signup job") {
		t.Fatalf("RequireEnqueued failure = %q", fake.msg)
	}
	fake = fakeTB{}
	fake.run(func() { kilntest.RequireNotEnqueued[signup](&fake, c, nil) })
	if !strings.Contains(fake.msg, "signup job") {
		t.Fatalf("RequireNotEnqueued failure = %q", fake.msg)
	}
}

type fakeTB struct {
	testing.TB
	msg string
}

func (*fakeTB) Helper()                  {}
func (*fakeTB) Context() context.Context { return context.Background() }

func (f *fakeTB) Fatalf(format string, args ...any) {
	f.msg = fmt.Sprintf(format, args...)
	runtime.Goexit()
}

func (f *fakeTB) run(fn func()) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()
	<-done
}

func TestWorkLeavesMuxOpen(t *testing.T) {
	t.Parallel()
	m := mux()
	ctx := context.Background()
	if r := kilntest.Work(ctx, m, signup{Plan: "pro"}); r.State != kiln.Succeeded {
		t.Fatalf("first run: %+v", r)
	}
	var wrapped bool
	m.Use(func(next kiln.HandlerFunc) kiln.HandlerFunc {
		return func(ctx context.Context, j *kiln.RawJob) error {
			wrapped = true
			return next(ctx, j)
		}
	})
	if r := kilntest.Work(ctx, m, signup{Plan: "pro"}); r.State != kiln.Succeeded || !wrapped {
		t.Fatalf("after Use: %+v wrapped %v", r, wrapped)
	}
}

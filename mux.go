package kiln

import (
	"context"
	"fmt"
	"reflect"
	"runtime/debug"
	"slices"
	"sync"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
	"github.com/rafaelaugustos/kiln/internal/testhook"
)

func init() {
	testhook.Dispatch = (*Mux).dispatch
}

// HandlerFunc runs one attempt of a job. Returning nil marks the job succeeded. An error made by
// [Permanent] fails it, one made by [Snooze] reschedules it and one made by [Cancel] deletes it;
// any other error schedules a retry, or fails the job when no attempts are left. A panic is
// recovered and counts as a [*PanicError].
//
// ctx is canceled when the job is deleted, when the attempt times out, and when the server stops
// and its ShutdownTimeout has passed, with [ErrCanceled], [ErrTimeout] or [ErrShutdown] as the
// cause. It is also canceled when the server finds that it no longer holds the job.
type HandlerFunc func(ctx context.Context, j *RawJob) error

// Middleware wraps the handler of every kind in a [Mux]; see [Mux.Use]. It sees each attempt as a
// [RawJob], and the handler's error, a [*PanicError] after a panic.
type Middleware func(next HandlerFunc) HandlerFunc

// Mux maps job kinds to their handlers. Register handlers with [Handle] or [Mux.HandleFunc] and
// middleware with [Mux.Use], then give the mux to [NewServer], which freezes it.
type Mux struct {
	mu     sync.Mutex
	mw     []Middleware
	routes map[string]*route
	kinds  []string
	frozen bool
}

type route struct {
	h       HandlerFunc
	chain   HandlerFunc
	timeout time.Duration
	backoff Backoff
}

// NewMux returns a Mux with no handlers. The zero Mux is ready to use as well.
func NewMux() *Mux {
	return &Mux{routes: make(map[string]*route)}
}

// Use adds middleware around every handler of the mux, including handlers registered later. The
// first middleware added is the outermost. Use panics once the mux is frozen.
func (m *Mux) Use(mw ...Middleware) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.frozen {
		panic("kiln: mux is frozen")
	}
	m.mw = append(m.mw, mw...)
}

// Handle registers h for the kind of T, the value its Kind method returns. h follows the rules of
// [HandlerFunc], with the job's args decoded into a T; a job whose args do not decode fails
// without retrying. Handle panics if T is a pointer or an interface type, if h is nil, if the kind
// is invalid or already registered, or if the mux is frozen.
func Handle[T Args](m *Mux, h func(context.Context, *Job[T]) error, opts ...HandleOption) {
	t := reflect.TypeFor[T]()
	if k := t.Kind(); k == reflect.Pointer || k == reflect.Interface {
		panic(fmt.Sprintf("kiln: Handle[%s]: args must be a concrete non-pointer type", t))
	}
	if h == nil {
		panic("kiln: nil handler")
	}
	var zero T
	m.handle(zero.Kind(), func(ctx context.Context, raw *RawJob) error {
		j, err := typed[T](raw)
		if err != nil {
			return err
		}
		return h(ctx, j)
	}, opts)
}

// HandleFunc registers h for kind, leaving the job's args as JSON. It panics in the same cases as
// [Handle].
func (m *Mux) HandleFunc(kind string, h HandlerFunc, opts ...HandleOption) {
	if h == nil {
		panic("kiln: nil handler")
	}
	m.handle(kind, h, opts)
}

func (m *Mux) handle(kind string, h HandlerFunc, opts []HandleOption) {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch {
	case m.frozen:
		panic("kiln: mux is frozen")
	case !validKind(kind):
		panic(fmt.Sprintf("kiln: invalid kind %q", kind))
	case m.routes[kind] != nil:
		panic(fmt.Sprintf("kiln: kind %q is already registered", kind))
	}
	r := &route{h: recovered(h)}
	for _, o := range opts {
		switch o := o.(type) {
		case Timeout:
			r.timeout = time.Duration(o)
		case Backoff:
			r.backoff = o
		}
	}
	if m.routes == nil {
		m.routes = make(map[string]*route)
	}
	m.routes[kind] = r
}

func (m *Mux) snapshot() *Mux {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.frozen {
		return m
	}
	c := &Mux{mw: slices.Clone(m.mw), routes: make(map[string]*route, len(m.routes))}
	for kind, r := range m.routes {
		c.routes[kind] = &route{h: r.h, timeout: r.timeout, backoff: r.backoff}
	}
	c.freeze()
	return c
}

func (m *Mux) freeze() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.frozen {
		return m.kinds
	}
	m.frozen = true
	for kind, r := range m.routes {
		r.chain = r.h
		for _, v := range slices.Backward(m.mw) {
			r.chain = v(r.chain)
		}
		m.kinds = append(m.kinds, kind)
	}
	slices.Sort(m.kinds)
	return m.kinds
}

func (m *Mux) dispatch(ctx context.Context, args Args, opts ...InsertOption) (driver.Outcome, error) {
	p, err := buildParams(args, opts)
	if err != nil {
		return driver.Outcome{}, err
	}
	now := time.Now()
	dj := driver.Job{
		Claim:       1,
		Kind:        p.Kind,
		Queue:       p.Queue,
		Args:        p.Args,
		Meta:        p.Meta,
		Tags:        p.Tags,
		Priority:    p.Priority,
		Attempt:     1,
		MaxAttempts: p.MaxAttempts,
		Timeout:     p.Timeout,
		RunAt:       now,
		CreatedAt:   now,
		AttemptedAt: now,
		BatchID:     p.BatchID,
		RecurringID: p.RecurringID,
		LimitKey:    p.LimitKey,
	}
	t := &task{job: rawJob(&dj, nil), ref: dj.Ref, timeout: dj.Timeout}
	t.Context, t.cancel = context.WithCancelCause(ctx)
	defer t.cancel(context.Canceled)
	return m.snapshot().exec(t, &ServerConfig{Timeout: Timeout(defaultTimeout), UnknownKindTTL: defaultUnknownKindTTL})
}

func (r *route) call(ctx context.Context, j *RawJob) (err error) {
	defer func() {
		if v := recover(); v != nil {
			err = &PanicError{Value: v, Stack: debug.Stack()}
		}
	}()
	return r.chain(ctx, j)
}

func recovered(h HandlerFunc) HandlerFunc {
	return func(ctx context.Context, j *RawJob) (err error) {
		defer func() {
			if v := recover(); v != nil {
				err = &PanicError{Value: v, Stack: debug.Stack()}
			}
		}()
		return h(ctx, j)
	}
}

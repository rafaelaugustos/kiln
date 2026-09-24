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

type HandlerFunc func(ctx context.Context, j *RawJob) error

type Middleware func(next HandlerFunc) HandlerFunc

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

func NewMux() *Mux {
	return &Mux{routes: make(map[string]*route)}
}

func (m *Mux) Use(mw ...Middleware) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.frozen {
		panic("kiln: mux is frozen")
	}
	m.mw = append(m.mw, mw...)
}

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
		for i := len(m.mw) - 1; i >= 0; i-- {
			r.chain = m.mw[i](r.chain)
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
		Ref:         driver.Ref{Claim: 1},
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

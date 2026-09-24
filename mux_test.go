package kiln

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/rafaelaugustos/kiln/memstore"
)

type ptrArgs struct{}

func (*ptrArgs) Kind() string { return "ptr" }

type badKind struct{}

func (badKind) Kind() string { return "9lives" }

func mustPanic(t *testing.T, want string, f func()) {
	t.Helper()
	defer func() {
		t.Helper()
		v := recover()
		if v == nil {
			t.Fatalf("no panic, want %q", want)
		}
		if msg := fmt.Sprint(v); !strings.Contains(msg, want) {
			t.Fatalf("panic %q, want %q", msg, want)
		}
	}()
	f()
}

func TestMuxPanics(t *testing.T) {
	t.Parallel()
	nop := func(context.Context, *Job[ping]) error { return nil }
	m := NewMux()
	Handle(m, nop)
	mustPanic(t, "non-pointer", func() { Handle(m, func(context.Context, *Job[*ptrArgs]) error { return nil }) })
	mustPanic(t, "invalid kind", func() { Handle(m, func(context.Context, *Job[badKind]) error { return nil }) })
	mustPanic(t, "invalid kind", func() { m.HandleFunc("", func(context.Context, *RawJob) error { return nil }) })
	mustPanic(t, "already registered", func() { Handle(m, nop) })
	mustPanic(t, "nil handler", func() { m.HandleFunc("nil", nil) })

	if _, err := NewServer(NewClient(memstore.New()), m, ServerConfig{}); err != nil {
		t.Fatal(err)
	}
	mustPanic(t, "kiln: mux is frozen", func() { m.HandleFunc("late", func(context.Context, *RawJob) error { return nil }) })
	mustPanic(t, "kiln: mux is frozen", func() { Handle(m, func(context.Context, *Job[testArgs]) error { return nil }) })
	mustPanic(t, "kiln: mux is frozen", func() { m.Use(func(next HandlerFunc) HandlerFunc { return next }) })
}

func TestMuxMiddlewareOrder(t *testing.T) {
	t.Parallel()
	var trace []string
	mw := func(name string) Middleware {
		return func(next HandlerFunc) HandlerFunc {
			return func(ctx context.Context, j *RawJob) error {
				trace = append(trace, name+">")
				err := next(ctx, j)
				trace = append(trace, "<"+name)
				return err
			}
		}
	}
	m := NewMux()
	m.Use(mw("a"), mw("b"))
	m.Use(mw("c"))
	Handle(m, func(context.Context, *Job[ping]) error {
		trace = append(trace, "h")
		return nil
	})
	if o, err := m.dispatch(context.Background(), ping{}); err != nil || o.State != Succeeded {
		t.Fatalf("outcome %+v err %v", o, err)
	}
	if want := []string{"a>", "b>", "c>", "h", "<c", "<b", "<a"}; !slices.Equal(trace, want) {
		t.Fatalf("trace = %v, want %v", trace, want)
	}
}

func TestMuxPanicRecovery(t *testing.T) {
	t.Parallel()
	var seen error
	m := NewMux()
	m.Use(func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, j *RawJob) error {
			if j.Kind == "bad-mw" {
				panic("middleware")
			}
			seen = next(ctx, j)
			return seen
		}
	})
	Handle(m, func(context.Context, *Job[ping]) error { panic("handler") })
	m.HandleFunc("bad-mw", func(context.Context, *RawJob) error { return nil })

	o, err := m.dispatch(context.Background(), ping{})
	var pe *PanicError
	if !errors.As(seen, &pe) || pe.Value != "handler" {
		t.Fatalf("middleware saw %v", seen)
	}
	if !errors.As(err, &pe) || o.State != Scheduled || o.Reason != "retry" || !strings.Contains(o.Trace, "goroutine") {
		t.Fatalf("outcome %+v err %v", o, err)
	}
	o, err = m.dispatch(context.Background(), testArgs{K: "bad-mw"})
	if !errors.As(err, &pe) || pe.Value != "middleware" || o.Error != "kiln: panic: middleware" {
		t.Fatalf("outcome %+v err %v", o, err)
	}
}

type pingText struct{ N string }

func (pingText) Kind() string { return "ping" }

func TestMuxDecodeError(t *testing.T) {
	t.Parallel()
	m := NewMux()
	Handle(m, func(context.Context, *Job[ping]) error { return nil })
	o, err := m.dispatch(context.Background(), pingText{N: "seven"})
	if !errors.Is(err, ErrPermanent) || o.State != Failed || o.Reason != "permanent" {
		t.Fatalf("outcome %+v err %v", o, err)
	}
}

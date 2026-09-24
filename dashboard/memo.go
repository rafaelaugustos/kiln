package dashboard

import (
	"context"
	"sync"
	"time"
)

type flight[T any] struct {
	done chan struct{}
	v    T
	err  error
}

type memo[T any] struct {
	ttl  time.Duration
	load func(context.Context) (T, error)

	mu  sync.Mutex
	v   T
	exp time.Time
	f   *flight[T]
}

func newMemo[T any](ttl time.Duration, load func(context.Context) (T, error)) *memo[T] {
	return &memo[T]{ttl: ttl, load: load}
}

func (m *memo[T]) get(ctx context.Context) (T, error) {
	m.mu.Lock()
	if time.Now().Before(m.exp) {
		v := m.v
		m.mu.Unlock()
		return v, nil
	}
	f := m.f
	if f == nil {
		f = &flight[T]{done: make(chan struct{})}
		m.f = f
		go m.run(context.WithoutCancel(ctx), f)
	}
	m.mu.Unlock()
	select {
	case <-f.done:
		return f.v, f.err
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	}
}

func (m *memo[T]) run(ctx context.Context, f *flight[T]) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	f.v, f.err = m.load(ctx)
	m.mu.Lock()
	m.f = nil
	if f.err == nil {
		m.v, m.exp = f.v, time.Now().Add(m.ttl)
	}
	m.mu.Unlock()
	close(f.done)
}

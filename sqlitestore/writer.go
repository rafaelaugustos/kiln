package sqlitestore

import (
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"math/rand/v2"
	"sync"
	"time"
)

type querier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
}

var writers = struct {
	sync.Mutex
	m map[*sql.DB]*writer
}{m: make(map[*sql.DB]*writer)}

type writer struct {
	db    *sql.DB
	lock  chan struct{}
	refs  int
	conn  *sql.Conn
	stmts map[string]*sql.Stmt
}

func acquire(db *sql.DB) *writer {
	writers.Lock()
	defer writers.Unlock()
	w := writers.m[db]
	if w == nil {
		w = &writer{db: db, lock: make(chan struct{}, 1)}
		writers.m[db] = w
	}
	w.refs++
	return w
}

func (w *writer) release() {
	writers.Lock()
	w.refs--
	last := w.refs == 0
	if last {
		delete(writers.m, w.db)
	}
	writers.Unlock()
	if last {
		w.lock <- struct{}{}
		w.reset(false)
		<-w.lock
	}
}

func (s *Store) write(ctx context.Context, fn func(ctx context.Context, q querier) error) error {
	w := s.w
	select {
	case w.lock <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-w.lock }()
	for try := 0; ; try++ {
		if w.conn == nil {
			c, err := w.db.Conn(ctx)
			if err != nil {
				return err
			}
			w.conn, w.stmts = c, make(map[string]*sql.Stmt)
		}
		err := begin(ctx, w)
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			w.conn.ExecContext(context.Background(), "ROLLBACK")
			return err
		}
		w.reset(true)
		if try > 0 {
			return err
		}
	}
	ctx = context.WithoutCancel(ctx)
	if err := fn(ctx, w); err != nil {
		w.rollback()
		return err
	}
	if _, err := w.ExecContext(ctx, "COMMIT"); err != nil {
		w.rollback()
		return err
	}
	return nil
}

func begin(ctx context.Context, q querier) error {
	for pause := time.Millisecond; ; pause = min(2*pause, 64*time.Millisecond) {
		_, err := q.ExecContext(ctx, "BEGIN IMMEDIATE")
		if !busy(err) {
			return err
		}
		t := time.NewTimer(pause/2 + rand.N(pause))
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}
	}
}

func (w *writer) rollback() {
	if _, err := w.conn.ExecContext(context.Background(), "ROLLBACK"); err != nil {
		w.reset(true)
	}
}

func (w *writer) reset(broken bool) {
	if w.conn == nil {
		return
	}
	for _, st := range w.stmts {
		st.Close()
	}
	if broken {
		w.conn.Raw(func(any) error { return sqldriver.ErrBadConn })
	}
	w.conn.Close()
	w.conn, w.stmts = nil, nil
}

func (w *writer) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	if st := w.stmts[query]; st != nil {
		return st, nil
	}
	st, err := w.conn.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	w.stmts[query] = st
	return st, nil
}

func (w *writer) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	st, err := w.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return st.ExecContext(ctx, args...)
}

func (w *writer) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	st, err := w.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return st.QueryContext(ctx, args...)
}

func (w *writer) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	st, err := w.PrepareContext(ctx, query)
	if err != nil {
		return w.conn.QueryRowContext(ctx, query, args...)
	}
	return st.QueryRowContext(ctx, args...)
}

func prepare(ctx context.Context, q querier, query string) (*sql.Stmt, func(), error) {
	st, err := q.PrepareContext(ctx, query)
	if err != nil {
		return nil, nil, err
	}
	if _, cached := q.(*writer); cached {
		return st, func() {}, nil
	}
	return st, func() { st.Close() }, nil
}

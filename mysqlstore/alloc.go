package mysqlstore

import (
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"
)

type side struct {
	db     *sql.DB
	mu     sync.Mutex
	conn   *sql.Conn
	closed bool
}

func (c *side) exec(ctx context.Context, query string) (sql.Result, error) {
	var r sql.Result
	err := c.run(ctx, func(conn *sql.Conn) (err error) {
		r, err = conn.ExecContext(ctx, query)
		return err
	})
	return r, err
}

func (c *side) run(ctx context.Context, fn func(conn *sql.Conn) error) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for try := 0; ; try++ {
		if c.closed {
			return sql.ErrConnDone
		}
		if c.conn == nil {
			conn, err := c.db.Conn(ctx)
			if err != nil {
				return err
			}
			c.conn = conn
		}
		err := fn(c.conn)
		if err == nil || try > 0 || !broken(err) {
			return err
		}
		c.conn.Close()
		c.conn = nil
	}
}

func (c *side) close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
}

func broken(err error) bool {
	if me := mysqlError(err); me != nil {
		return me.Number == errKilled || me.Number == errIdle
	}
	return errors.Is(err, sqldriver.ErrBadConn) || errors.Is(err, sql.ErrConnDone) || errors.Is(err, mysql.ErrInvalidConn)
}

type grant struct {
	n     int64
	first int64
	err   error
	done  chan struct{}
}

type allocator struct {
	side    *side
	stmt    string
	mu      sync.Mutex
	queue   []*grant
	running bool
}

func (a *allocator) take(ctx context.Context, n int64) (int64, error) {
	g := &grant{n: n, done: make(chan struct{})}
	a.mu.Lock()
	a.queue = append(a.queue, g)
	start := !a.running
	a.running = true
	a.mu.Unlock()
	if start {
		go a.run()
	}
	select {
	case <-g.done:
		return g.first, g.err
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func (a *allocator) run() {
	for {
		a.mu.Lock()
		batch := a.queue
		a.queue = nil
		if len(batch) == 0 {
			a.running = false
			a.mu.Unlock()
			return
		}
		a.mu.Unlock()
		var total int64
		for _, g := range batch {
			total += g.n
		}
		last, err := a.reserve(total)
		next := last - total + 1
		for _, g := range batch {
			g.first, g.err = next, err
			next += g.n
			close(g.done)
		}
	}
}

func (a *allocator) reserve(n int64) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	r, err := a.side.exec(ctx, render(a.stmt, n))
	if err != nil {
		return 0, err
	}
	last, err := r.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("kiln: allocate ids: %w", err)
	}
	return last, nil
}

package sqlitestore

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"

	"github.com/rafaelaugustos/kiln/driver"
)

var (
	_ driver.Store      = (*Store)(nil)
	_ driver.Notifier   = (*Store)(nil)
	_ driver.Transactor = (*Store)(nil)
	_ driver.Writer     = (*TxWriter)(nil)
)

type Store struct {
	db     *sql.DB
	prefix string
	q      statements
	w      *writer
	hub    *hub

	mu      sync.Mutex
	cursors struct {
		parent   int64
		awaiting int64
		limit    string
		holder   []byte
	}
}

func New(ctx context.Context, db *sql.DB, opts ...Option) (*Store, error) {
	c := newConfig(opts)
	if !validPrefix(c.prefix) {
		return nil, fmt.Errorf("%w: prefix %q", driver.ErrInvalid, c.prefix)
	}
	if db.Stats().MaxOpenConnections == 1 {
		return nil, fmt.Errorf("%w: sqlitestore keeps one connection for writes, allow at least two open connections", driver.ErrInvalid)
	}
	if err := configure(ctx, db); err != nil {
		return nil, err
	}
	var err error
	if c.noMigrate {
		err = checkSchema(ctx, db, c.prefix)
	} else {
		err = migrate(ctx, db, c.prefix)
	}
	if err != nil {
		return nil, err
	}
	return &Store{
		db:     db,
		prefix: c.prefix,
		q:      render(c.prefix),
		w:      acquire(db),
		hub:    newHub(),
	}, nil
}

func configure(ctx context.Context, db *sql.DB) error {
	c, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("kiln: connect: %w", err)
	}
	defer c.Close()
	var ms int64
	if err := c.QueryRowContext(ctx, "PRAGMA busy_timeout").Scan(&ms); err != nil {
		return fmt.Errorf("kiln: busy timeout: %w", err)
	}
	if ms <= 0 {
		return fmt.Errorf("%w: busy_timeout is 0; it is per connection, so set it in the DSN "+
			"(modernc.org/sqlite: _pragma=busy_timeout(5000), mattn/go-sqlite3: _busy_timeout=5000)", driver.ErrInvalid)
	}
	var mode string
	if err := c.QueryRowContext(ctx, "PRAGMA journal_mode = WAL").Scan(&mode); err != nil {
		return fmt.Errorf("kiln: journal mode: %w", err)
	}
	if !strings.EqualFold(mode, "wal") {
		return fmt.Errorf("%w: journal_mode is %s, sqlitestore needs WAL and therefore a database file", driver.ErrInvalid, mode)
	}
	return nil
}

func (s *Store) Close() {
	s.hub.close()
	s.w.release()
}

func (s *Store) Tx(tx *sql.Tx) *TxWriter {
	w := &TxWriter{s: s}
	if tx != nil {
		w.q = tx
	}
	return w
}

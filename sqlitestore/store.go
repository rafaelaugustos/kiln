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
	_ driver.Store       = (*Store)(nil)
	_ driver.Notifier    = (*Store)(nil)
	_ driver.Transactor  = (*Store)(nil)
	_ driver.LimitReader = (*Store)(nil)
	_ driver.TxWriter    = (*TxWriter)(nil)
)

// Store is a [driver.Store] on SQLite, which also implements [driver.Notifier],
// [driver.Transactor] and [driver.LimitReader]. It is safe for concurrent use. The Stores opened
// on one [sql.DB] share the connection kept for writes, so their writes take turns.
type Store struct {
	db     *sql.DB
	prefix string
	q      statements
	w      *writer
	hub    *hub
	closed sync.Once

	mu      sync.Mutex
	cursors struct {
		parent   int64
		awaiting int64
		limit    string
		holder   []byte
	}
}

// New returns a store on db, after switching the file to WAL. It fails with [driver.ErrInvalid]
// when db allows a single open connection, when its connections have no busy_timeout, or when the
// database cannot use WAL, as an in-memory one cannot. Unless [NoMigrate] is given, New also
// migrates the tables as [Migrate] does; with NoMigrate it only checks them and fails when a
// migration or change of this release is missing. Either way it fails when the tables' migration
// number is newer than this release knows.
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
		hub:    newHub(c.bus),
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

// Close publishes the events still pending, when the store has a bus, makes Subscribe calls
// return with an error, and gives back the connection kept for writes once every Store on db is
// closed. It closes neither db nor the bus. Call it after nothing uses the store any more; calling
// it again does nothing.
func (s *Store) Close() {
	s.closed.Do(func() {
		s.hub.close()
		s.w.release()
	})
}

// Tx returns a writer that inserts jobs and opens and seals batches inside tx, so that they
// commit or roll back with the application's work. Begin tx with BEGIN IMMEDIATE, which the DSN
// parameter _txlock=immediate makes the default, so that it holds the write lock from the start,
// and keep it short: while it is open, every other writer on the file waits, kiln's servers
// included. A nil tx gives a writer whose writes fail with [driver.ErrNilTx].
func (s *Store) Tx(tx *sql.Tx) *TxWriter {
	w := &TxWriter{s: s}
	if tx != nil {
		w.q = tx
	}
	return w
}

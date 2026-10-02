package pgstore

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rafaelaugustos/kiln/driver"
)

var (
	_ driver.Store      = (*Store)(nil)
	_ driver.Notifier   = (*Store)(nil)
	_ driver.Transactor = (*Store)(nil)
	_ driver.TxWriter   = (*TxWriter)(nil)
)

// Store is a [driver.Store] on PostgreSQL. It also implements [driver.Notifier], through LISTEN
// and NOTIFY, and [driver.Transactor], and is safe for concurrent use.
type Store struct {
	pool   *pgxpool.Pool
	schema string
	listen *pgx.ConnConfig
	q      statements
	claims sync.Map
	nt     *notifier

	mu          sync.Mutex
	sweepKey    string
	sweepParent int64
	pruneKey    []byte
}

// New returns a store on the database pool connects to. It copies pool's configuration into a
// pool of its own, with [MaxConns] connections, the statement mode of [ExecMode] in place of
// pool's, and application_name set to "kiln", and does not use pool again, so the application may
// close it. Unless [NoMigrate] is given, New then migrates the schema as [Migrate] does; with
// NoMigrate it only checks the schema and fails when a migration or change of this release is
// missing. Either way it fails when the schema's migration number is newer than this release
// knows.
func New(ctx context.Context, pool *pgxpool.Pool, opts ...Option) (*Store, error) {
	c := newConfig(opts)
	if !validSchema(c.schema) {
		return nil, fmt.Errorf("%w: schema %q", driver.ErrInvalid, c.schema)
	}
	if c.maxConns < 1 {
		return nil, fmt.Errorf("%w: max conns %d", driver.ErrInvalid, c.maxConns)
	}
	pc := pool.Config().Copy()
	pc.MaxConns = c.maxConns
	pc.MinConns = min(pc.MinConns, c.maxConns)
	pc.ConnConfig.DefaultQueryExecMode = c.mode
	pc.ConnConfig.RuntimeParams["application_name"] = "kiln"

	listen := pc.ConnConfig.Copy()
	if c.listen != "" {
		lc, err := pgx.ParseConfig(c.listen)
		if err != nil {
			return nil, fmt.Errorf("kiln: listen conn: %w", err)
		}
		listen = lc
	}
	listen.RuntimeParams["application_name"] = "kiln"

	own, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return nil, fmt.Errorf("kiln: pool: %w", err)
	}
	if c.noMigrate {
		err = checkSchema(ctx, own, c.schema)
	} else {
		err = migrate(ctx, own, c.schema)
	}
	if err != nil {
		own.Close()
		return nil, err
	}
	s := &Store{
		pool:   own,
		schema: c.schema,
		listen: listen,
		q:      render(c.schema),
	}
	s.nt = newNotifier(s)
	return s, nil
}

// Close sends the notifications still pending and closes the store's pool, waiting for the
// queries in progress. It closes neither the pool given to New nor the connections of Subscribe
// calls, which end with their contexts. Call it only once, after nothing uses the store any more.
func (s *Store) Close() {
	s.nt.close()
	s.pool.Close()
}

// Tx returns a writer that inserts jobs and opens and seals batches inside tx, so that they
// commit or roll back with the application's work. tx must be open on the store's database; it
// may come from any pgx connection or pool. A nil tx gives a writer whose writes fail with
// [driver.ErrNilTx].
func (s *Store) Tx(tx pgx.Tx) *TxWriter {
	if tx == nil {
		return &TxWriter{s: s}
	}
	return &TxWriter{s: s, tx: tx}
}

// SQLTx is [Store.Tx] for an application on database/sql: tx must be open on the store's
// database, in a [sql.DB] opened with pgx's stdlib driver (github.com/jackc/pgx/v5/stdlib), which
// sqlx, bun and GORM can use. The writer works like one from Tx, except that each statement is a
// round trip of its own. A nil tx gives a writer whose writes fail with [driver.ErrNilTx].
func (s *Store) SQLTx(tx *sql.Tx) *TxWriter {
	if tx == nil {
		return &TxWriter{s: s}
	}
	return &TxWriter{s: s, tx: &sqlTx{tx: tx}}
}

func (s *Store) channel(name string) string {
	return s.schema + "_" + name
}

func render(schema string) statements {
	r := strings.NewReplacer("{s}", schema)
	var q statements
	q.each(func(p *string, text string) { *p = r.Replace(text) })
	return q
}

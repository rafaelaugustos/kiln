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

func (s *Store) Close() {
	s.nt.close()
	s.pool.Close()
}

func (s *Store) Tx(tx pgx.Tx) *TxWriter {
	if tx == nil {
		return &TxWriter{s: s}
	}
	return &TxWriter{s: s, tx: tx}
}

// SQLTx is [Store.Tx] for an application on database/sql: tx must come from a [sql.DB] opened
// with pgx's stdlib driver (github.com/jackc/pgx/v5/stdlib), which sqlx, bun and GORM can use.
// The writer works like one from Tx, except that each statement is a round trip of its own.
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

package pgstore

import "github.com/jackc/pgx/v5"

// Option configures [New] and [Migrate].
type Option func(*config)

type config struct {
	schema    string
	maxConns  int32
	noMigrate bool
	listen    string
	mode      pgx.QueryExecMode
}

func newConfig(opts []Option) config {
	c := config{schema: "kiln", maxConns: 8, mode: pgx.QueryExecModeCacheStatement}
	for _, o := range opts {
		o(&c)
	}
	return c
}

// Schema sets the PostgreSQL schema that holds the store's tables, types, functions and
// sequences, "kiln" by default. A name has 1 to 48 lowercase letters, digits and underscores and
// does not start with a digit; New and Migrate reject others with [driver.ErrInvalid]. Each schema
// is a separate installation, with notification channels of its own.
func Schema(name string) Option {
	return func(c *config) { c.schema = name }
}

// MaxConns caps the pool New creates for the store at n connections, 8 by default. The
// connection Subscribe opens for LISTEN is not part of it. n must be at least 1.
func MaxConns(n int32) Option {
	return func(c *config) { c.maxConns = n }
}

// NoMigrate makes New check the schema instead of migrating it: New fails, naming the version it
// found or the first change it lacks, when a migration or change of this release has not been
// applied. Run [Migrate] from a deploy step instead.
func NoMigrate() Option {
	return func(c *config) { c.noMigrate = true }
}

// ListenConn makes Subscribe connect with connString rather than with the configuration of the
// pool given to New. Use it when that pool goes through PgBouncer in transaction mode, which
// cannot deliver notifications. New fails when connString does not parse.
func ListenConn(connString string) Option {
	return func(c *config) { c.listen = connString }
}

// ExecMode sets how the store's pool sends statements, [pgx.QueryExecModeCacheStatement] by
// default, which prepares each statement once per connection, whatever mode the pool given to New
// uses. Poolers that do not support prepared statements need [pgx.QueryExecModeExec]. The mode
// does not reach the transactions given to [Store.Tx], which keep that of their connection.
func ExecMode(m pgx.QueryExecMode) Option {
	return func(c *config) { c.mode = m }
}

func validSchema(s string) bool {
	if s == "" || len(s) > 48 || s[0] >= '0' && s[0] <= '9' {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '_') {
			return false
		}
	}
	return true
}

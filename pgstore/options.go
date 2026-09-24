package pgstore

import "github.com/jackc/pgx/v5"

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

func Schema(name string) Option {
	return func(c *config) { c.schema = name }
}

func MaxConns(n int32) Option {
	return func(c *config) { c.maxConns = n }
}

func NoMigrate() Option {
	return func(c *config) { c.noMigrate = true }
}

func ListenConn(connString string) Option {
	return func(c *config) { c.listen = connString }
}

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

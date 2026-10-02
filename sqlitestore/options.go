package sqlitestore

import "github.com/rafaelaugustos/kiln/driver"

// Option configures [New] and [Migrate].
type Option func(*config)

type config struct {
	prefix    string
	noMigrate bool
	bus       driver.Bus
}

func newConfig(opts []Option) config {
	c := config{prefix: "kiln_"}
	for _, o := range opts {
		o(&c)
	}
	return c
}

// Prefix sets the prefix of the store's table names, "kiln_" by default. It may be empty or hold
// up to 32 lowercase letters, digits and underscores, not starting with a digit; New and Migrate
// reject others with [driver.ErrInvalid]. Stores with different prefixes in one file are separate
// installations.
func Prefix(p string) Option {
	return func(c *config) { c.prefix = p }
}

// NoMigrate makes New check the tables instead of migrating them: New fails, naming what is
// missing, when a migration or change of this release has not been applied. Run [Migrate] from a
// deploy step instead.
func NoMigrate() Option {
	return func(c *config) { c.noMigrate = true }
}

// Bus makes the store publish its events on b after each commit, and [Store.Subscribe] deliver
// the events b carries along with those of the store's own writes, so that servers in other
// processes are woken too. Give the same bus to every process that uses the file.
func Bus(b driver.Bus) Option {
	return func(c *config) { c.bus = b }
}

func validPrefix(p string) bool {
	if len(p) > 32 || p != "" && p[0] >= '0' && p[0] <= '9' {
		return false
	}
	for i := 0; i < len(p); i++ {
		c := p[i]
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '_') {
			return false
		}
	}
	return true
}

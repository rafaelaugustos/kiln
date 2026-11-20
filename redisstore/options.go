package redisstore

import (
	"fmt"

	"github.com/rafaelaugustos/kiln/driver"
)

type config struct {
	prefix   string
	eviction bool
}

// Option configures a [Store].
type Option func(*config)

func newConfig(opts []Option) (config, error) {
	cfg := config{prefix: "kiln"}
	for _, o := range opts {
		o(&cfg)
	}
	if !validPrefix(cfg.prefix) {
		return cfg, fmt.Errorf("%w: prefix %q", driver.ErrInvalid, cfg.prefix)
	}
	return cfg, nil
}

// Prefix names the store inside the Redis server; the default is "kiln". Every key the store
// writes starts with "{name}:", so two stores with different prefixes share a server without
// seeing each other's jobs, and on Redis Cluster all the keys of a store land in one slot. A
// name may hold letters, digits, '-', '_', '.' and ':', and at most 64 bytes.
func Prefix(name string) Option {
	return func(c *config) { c.prefix = name }
}

// AllowEviction lets the store run on a server whose maxmemory-policy evicts any key, which [New]
// refuses by default. Under memory pressure such a server deletes jobs and the store's indexes
// without telling anyone, so this is only for servers that will never reach their memory limit.
func AllowEviction() Option {
	return func(c *config) { c.eviction = true }
}

func validPrefix(s string) bool {
	if len(s) == 0 || len(s) > 64 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case 'a' <= c && c <= 'z', 'A' <= c && c <= 'Z', '0' <= c && c <= '9':
		case c == '-', c == '_', c == '.', c == ':':
		default:
			return false
		}
	}
	return true
}

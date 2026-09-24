package sqlitestore

type Option func(*config)

type config struct {
	prefix    string
	noMigrate bool
}

func newConfig(opts []Option) config {
	c := config{prefix: "kiln_"}
	for _, o := range opts {
		o(&c)
	}
	return c
}

func Prefix(p string) Option {
	return func(c *config) { c.prefix = p }
}

func NoMigrate() Option {
	return func(c *config) { c.noMigrate = true }
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

package sqlitestore

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"path"
	"slices"
	"strconv"
	"strings"

	"github.com/rafaelaugustos/kiln/driver"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

type migration struct {
	version int
	stmts   []string
}

var migrations = loadMigrations()

func loadMigrations() []migration {
	names, err := fs.Glob(migrationFS, "migrations/*.sql")
	if err != nil {
		panic(err)
	}
	var ms []migration
	for _, name := range names {
		base := path.Base(name)
		v, err := strconv.Atoi(base[:strings.IndexByte(base, '_')])
		if err != nil {
			panic(fmt.Sprintf("sqlitestore: bad migration name %s", name))
		}
		b, err := migrationFS.ReadFile(name)
		if err != nil {
			panic(err)
		}
		m := migration{version: v}
		for stmt := range strings.SplitSeq(string(b), ";\n") {
			if stmt = strings.TrimSpace(stmt); stmt != "" {
				m.stmts = append(m.stmts, stmt)
			}
		}
		ms = append(ms, m)
	}
	slices.SortFunc(ms, func(a, b migration) int { return a.version - b.version })
	return ms
}

func latest() int {
	return migrations[len(migrations)-1].version
}

func Migrate(ctx context.Context, db *sql.DB, opts ...Option) error {
	c := newConfig(opts)
	if !validPrefix(c.prefix) {
		return fmt.Errorf("%w: prefix %q", driver.ErrInvalid, c.prefix)
	}
	return migrate(ctx, db, c.prefix)
}

func migrate(ctx context.Context, db *sql.DB, prefix string) error {
	c, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("kiln: migrate: %w", err)
	}
	defer c.Close()
	if err := begin(ctx, c); err != nil {
		return fmt.Errorf("kiln: migrate: %w", err)
	}
	if err := upgrade(ctx, c, prefix); err != nil {
		c.ExecContext(context.WithoutCancel(ctx), "ROLLBACK")
		return err
	}
	if _, err := c.ExecContext(ctx, "COMMIT"); err != nil {
		c.ExecContext(context.WithoutCancel(ctx), "ROLLBACK")
		return fmt.Errorf("kiln: migrate: %w", err)
	}
	return nil
}

func upgrade(ctx context.Context, c *sql.Conn, prefix string) error {
	create := "CREATE TABLE IF NOT EXISTS " + prefix + "migrations (version INTEGER PRIMARY KEY, applied_at INTEGER NOT NULL)"
	if _, err := c.ExecContext(ctx, create); err != nil {
		return fmt.Errorf("kiln: migrate: %w", err)
	}
	v, err := version(ctx, c, prefix)
	if err != nil {
		return err
	}
	r := strings.NewReplacer("{p}", prefix, "{now}", clock)
	for _, m := range migrations {
		if m.version <= v {
			continue
		}
		for _, stmt := range m.stmts {
			if _, err := c.ExecContext(ctx, r.Replace(stmt)); err != nil {
				return fmt.Errorf("kiln: migrate to %d: %w", m.version, err)
			}
		}
		done := "INSERT INTO " + prefix + "migrations (version, applied_at) VALUES (?, " + clock + ")"
		if _, err := c.ExecContext(ctx, done, m.version); err != nil {
			return fmt.Errorf("kiln: migrate to %d: %w", m.version, err)
		}
	}
	return checkVersion(ctx, c, prefix, true)
}

func version(ctx context.Context, q querier, prefix string) (int, error) {
	var n int
	err := q.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = ?", prefix+"migrations").Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("kiln: schema version: %w", err)
	}
	if n == 0 {
		return 0, nil
	}
	var v int
	if err := q.QueryRowContext(ctx, "SELECT COALESCE(MAX(version), 0) FROM "+prefix+"migrations").Scan(&v); err != nil {
		return 0, fmt.Errorf("kiln: schema version: %w", err)
	}
	return v, nil
}

func checkVersion(ctx context.Context, q querier, prefix string, migrated bool) error {
	v, err := version(ctx, q, prefix)
	if err != nil {
		return err
	}
	switch {
	case v > latest():
		return fmt.Errorf("kiln: tables %s* are at version %d, this binary supports up to %d", prefix, v, latest())
	case v < latest() && !migrated:
		return fmt.Errorf("kiln: tables %s* are at version %d, want %d: run sqlitestore.Migrate", prefix, v, latest())
	}
	return nil
}

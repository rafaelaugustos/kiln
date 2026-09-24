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

//go:embed migrations/*.sql changes/*.sql
var schemaFS embed.FS

type migration struct {
	version int
	stmts   []string
}

type change struct {
	name  string
	stmts []string
}

var (
	migrations = loadMigrations()
	changes    = loadChanges()
)

func loadMigrations() []migration {
	names, err := fs.Glob(schemaFS, "migrations/*.sql")
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
		ms = append(ms, migration{version: v, stmts: script(name)})
	}
	slices.SortFunc(ms, func(a, b migration) int { return a.version - b.version })
	return ms
}

func loadChanges() []change {
	names, err := fs.Glob(schemaFS, "changes/*.sql")
	if err != nil {
		panic(err)
	}
	cs := make([]change, len(names))
	for i, name := range names {
		cs[i] = change{name: strings.TrimSuffix(path.Base(name), ".sql"), stmts: script(name)}
	}
	return cs
}

func script(name string) []string {
	b, err := schemaFS.ReadFile(name)
	if err != nil {
		panic(err)
	}
	var stmts []string
	for stmt := range strings.SplitSeq(string(b), ";\n") {
		if stmt = strings.TrimSpace(stmt); stmt != "" {
			stmts = append(stmts, stmt)
		}
	}
	return stmts
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
	r := strings.NewReplacer("{p}", prefix, "{now}", clock)
	if err := migrateVersions(ctx, c, prefix, r); err != nil {
		return err
	}
	return applyChanges(ctx, c, prefix, r)
}

func migrateVersions(ctx context.Context, c *sql.Conn, prefix string, r *strings.Replacer) error {
	create := "CREATE TABLE IF NOT EXISTS " + prefix + "migrations (version INTEGER PRIMARY KEY, applied_at INTEGER NOT NULL)"
	if _, err := c.ExecContext(ctx, create); err != nil {
		return fmt.Errorf("kiln: migrate: %w", err)
	}
	v, err := version(ctx, c, prefix)
	if err != nil {
		return err
	}
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

func applyChanges(ctx context.Context, c *sql.Conn, prefix string, r *strings.Replacer) error {
	create := "CREATE TABLE IF NOT EXISTS " + prefix + "schema_changes (name TEXT PRIMARY KEY, applied_at INTEGER)"
	if _, err := c.ExecContext(ctx, create); err != nil {
		return fmt.Errorf("kiln: migrate: %w", err)
	}
	applied, err := appliedChanges(ctx, c, prefix)
	if err != nil {
		return err
	}
	for _, ch := range changes {
		if applied[ch.name] {
			continue
		}
		for _, stmt := range ch.stmts {
			if _, err := c.ExecContext(ctx, r.Replace(stmt)); err != nil {
				return fmt.Errorf("kiln: schema change %s: %w", ch.name, err)
			}
		}
		done := "INSERT INTO " + prefix + "schema_changes (name, applied_at) VALUES (?, " + clock + ")"
		if _, err := c.ExecContext(ctx, done, ch.name); err != nil {
			return fmt.Errorf("kiln: schema change %s: %w", ch.name, err)
		}
	}
	return nil
}

func exists(ctx context.Context, q querier, table string) (bool, error) {
	var n int
	err := q.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = ?", table).Scan(&n)
	return n > 0, err
}

func version(ctx context.Context, q querier, prefix string) (int, error) {
	found, err := exists(ctx, q, prefix+"migrations")
	if err != nil {
		return 0, fmt.Errorf("kiln: schema version: %w", err)
	}
	if !found {
		return 0, nil
	}
	var v int
	if err := q.QueryRowContext(ctx, "SELECT COALESCE(MAX(version), 0) FROM "+prefix+"migrations").Scan(&v); err != nil {
		return 0, fmt.Errorf("kiln: schema version: %w", err)
	}
	return v, nil
}

func appliedChanges(ctx context.Context, q querier, prefix string) (map[string]bool, error) {
	found, err := exists(ctx, q, prefix+"schema_changes")
	if err != nil {
		return nil, fmt.Errorf("kiln: schema changes: %w", err)
	}
	if !found {
		return nil, nil
	}
	applied := make(map[string]bool, len(changes))
	rows, err := q.QueryContext(ctx, "SELECT name FROM "+prefix+"schema_changes")
	err = each(rows, err, func() error {
		var name string
		if err := rows.Scan(&name); err != nil {
			return err
		}
		applied[name] = true
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("kiln: schema changes: %w", err)
	}
	return applied, nil
}

func checkSchema(ctx context.Context, q querier, prefix string) error {
	if err := checkVersion(ctx, q, prefix, false); err != nil {
		return err
	}
	applied, err := appliedChanges(ctx, q, prefix)
	if err != nil {
		return err
	}
	for _, ch := range changes {
		if !applied[ch.name] {
			return fmt.Errorf("kiln: tables %s* lack schema change %s: run sqlitestore.Migrate", prefix, ch.name)
		}
	}
	return nil
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

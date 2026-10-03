package mssqlstore

import (
	"cmp"
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
var schemaFS embed.FS

type migration struct {
	version int
	name    string
	stmts   []string
}

var (
	migrations = load("migrations")
	changes    = load("changes")
)

func load(dir string) []migration {
	names, err := fs.Glob(schemaFS, dir+"/*.sql")
	if err != nil {
		panic(err)
	}
	var ms []migration
	for _, name := range names {
		base := strings.TrimSuffix(path.Base(name), ".sql")
		num, _, _ := strings.Cut(base, "_")
		v, err := strconv.Atoi(num)
		if err != nil {
			panic(fmt.Sprintf("mssqlstore: bad migration name %s", name))
		}
		b, err := schemaFS.ReadFile(name)
		if err != nil {
			panic(err)
		}
		m := migration{version: v, name: base}
		for stmt := range strings.SplitSeq(string(b), ";\n") {
			if stmt = strings.TrimSpace(stmt); stmt != "" {
				m.stmts = append(m.stmts, stmt)
			}
		}
		ms = append(ms, m)
	}
	slices.SortFunc(ms, func(a, b migration) int {
		return cmp.Or(cmp.Compare(a.version, b.version), strings.Compare(a.name, b.name))
	})
	return ms
}

func latest() int {
	return migrations[len(migrations)-1].version
}

// Migrate creates the store's tables and sequences, or brings them up to date, in the
// connection's default schema; [New] does the same unless [NoMigrate] is given. Of the options it
// reads only [Prefix]. Each migration runs in a transaction of its own holding an application
// lock taken with sp_getapplock, waiting up to 30s for it, so that callers running at the same
// time migrate once and a failed migration leaves nothing behind. Migrate changes nothing when the
// tables' migration number is newer than this release knows, and returns an error saying so.
func Migrate(ctx context.Context, db *sql.DB, opts ...Option) error {
	c := newConfig(opts)
	if !validPrefix(c.prefix) {
		return fmt.Errorf("%w: prefix %q", driver.ErrInvalid, c.prefix)
	}
	return migrate(ctx, db, c.prefix)
}

func migrate(ctx context.Context, db *sql.DB, prefix string) error {
	r := strings.NewReplacer("{p}", prefix)
	create := "IF OBJECT_ID(N'" + prefix + "migrations', N'U') IS NULL CREATE TABLE " + prefix +
		"migrations (version INT NOT NULL PRIMARY KEY, applied_at DATETIME2(6) NOT NULL)"
	for _, m := range migrations {
		err := locked(ctx, db, prefix, func(tx *sql.Tx) error {
			if _, err := tx.ExecContext(ctx, create); err != nil {
				return err
			}
			v, err := version(ctx, tx, prefix)
			if err != nil || v >= m.version {
				return err
			}
			for _, stmt := range m.stmts {
				if _, err := tx.ExecContext(ctx, r.Replace(stmt)); err != nil {
					return err
				}
			}
			done := "INSERT INTO " + prefix + "migrations (version, applied_at) VALUES (@v, SYSUTCDATETIME())"
			_, err = tx.ExecContext(ctx, done, sql.Named("v", m.version))
			return err
		})
		if err != nil {
			return fmt.Errorf("kiln: migrate %s* to %d: %w", prefix, m.version, err)
		}
	}
	if err := checkVersion(ctx, db, prefix, true); err != nil {
		return err
	}
	return amend(ctx, db, prefix)
}

func locked(ctx context.Context, db *sql.DB, prefix string, fn func(tx *sql.Tx) error) error {
	tx, err := db.BeginTx(ctx, readCommitted)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var got int
	lock := "DECLARE @r INT; EXEC @r = sp_getapplock @Resource = @name, @LockMode = 'Exclusive', " +
		"@LockOwner = 'Transaction', @LockTimeout = 30000; SELECT @r"
	if err := tx.QueryRowContext(ctx, lock, sql.Named("name", "kiln."+prefix+"migrate")).Scan(&got); err != nil {
		return err
	}
	if got < 0 {
		return fmt.Errorf("timed out waiting for the migration lock")
	}
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func amend(ctx context.Context, db *sql.DB, prefix string) error {
	todo, err := missing(ctx, db, prefix)
	if err != nil || len(todo) == 0 {
		return err
	}
	r := strings.NewReplacer("{p}", prefix)
	for _, c := range todo {
		err := locked(ctx, db, prefix, func(tx *sql.Tx) error {
			var n int
			done := "SELECT COUNT(*) FROM " + prefix + "schema_changes WHERE name = @name"
			if err := tx.QueryRowContext(ctx, done, sql.Named("name", c.name)).Scan(&n); err != nil || n > 0 {
				return err
			}
			for _, stmt := range c.stmts {
				if _, err := tx.ExecContext(ctx, r.Replace(stmt)); err != nil {
					return err
				}
			}
			record := "INSERT INTO " + prefix + "schema_changes (name, applied_at) VALUES (@name, SYSUTCDATETIME())"
			_, err := tx.ExecContext(ctx, record, sql.Named("name", c.name))
			return err
		})
		if err != nil {
			return fmt.Errorf("kiln: migrate %s*: change %s: %w", prefix, c.name, err)
		}
	}
	return nil
}

func missing(ctx context.Context, q querier, prefix string) ([]migration, error) {
	if len(changes) == 0 {
		return nil, nil
	}
	rows, err := q.QueryContext(ctx, "SELECT name FROM "+prefix+"schema_changes")
	if err != nil {
		return nil, fmt.Errorf("kiln: schema changes: %w", err)
	}
	defer rows.Close()
	done := make(map[string]bool, len(changes))
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("kiln: schema changes: %w", err)
		}
		done[name] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("kiln: schema changes: %w", err)
	}
	var todo []migration
	for _, c := range changes {
		if !done[c.name] {
			todo = append(todo, c)
		}
	}
	return todo, nil
}

func version(ctx context.Context, q querier, prefix string) (int, error) {
	var v int
	err := q.QueryRowContext(ctx, "SELECT COALESCE(MAX(version), 0) FROM "+prefix+"migrations").Scan(&v)
	if e := sqlError(err); e != nil && e.Number == errNoObject {
		return 0, nil
	}
	if err != nil {
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
		return fmt.Errorf("kiln: tables %s* are at version %d, want %d: run mssqlstore.Migrate", prefix, v, latest())
	}
	return nil
}

func checkSchema(ctx context.Context, q querier, prefix string) error {
	if err := checkVersion(ctx, q, prefix, false); err != nil {
		return err
	}
	todo, err := missing(ctx, q, prefix)
	if err != nil || len(todo) == 0 {
		return err
	}
	return fmt.Errorf("kiln: tables %s* lack schema change %s: run mssqlstore.Migrate", prefix, todo[0].name)
}

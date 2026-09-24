package pgstore

import (
	"cmp"
	"context"
	"embed"
	"fmt"
	"io/fs"
	"path"
	"slices"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rafaelaugustos/kiln/driver"
)

//go:embed migrations/*.sql changes/*.sql
var schemaFS embed.FS

const setupSQL = `SET LOCAL lock_timeout = '5s';
SELECT pg_advisory_xact_lock(hashtextextended('{s}.migrate', 0));
CREATE SCHEMA IF NOT EXISTS {s};
CREATE TABLE IF NOT EXISTS {s}.migrations (version int PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS {s}.schema_changes (name text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now());`

type migration struct {
	version int
	sql     string
}

type change struct {
	name string
	sql  string
}

type ledger struct {
	table  string
	column string
}

var (
	migrations = loadMigrations()
	changes    = loadChanges()
	versioned  = ledger{"migrations", "version"}
	named      = ledger{"schema_changes", "name"}
)

func loadMigrations() []migration {
	var ms []migration
	for name, sql := range embedded("migrations") {
		v, err := strconv.Atoi(name[:strings.IndexByte(name, '_')])
		if err != nil {
			panic(fmt.Sprintf("pgstore: bad migration name %s", name))
		}
		ms = append(ms, migration{version: v, sql: sql})
	}
	slices.SortFunc(ms, func(a, b migration) int { return a.version - b.version })
	return ms
}

func loadChanges() []change {
	var cs []change
	for name, sql := range embedded("changes") {
		cs = append(cs, change{name: strings.TrimSuffix(name, ".sql"), sql: sql})
	}
	slices.SortFunc(cs, func(a, b change) int { return cmp.Compare(a.name, b.name) })
	return cs
}

func embedded(dir string) map[string]string {
	names, err := fs.Glob(schemaFS, dir+"/*.sql")
	if err != nil {
		panic(err)
	}
	files := make(map[string]string, len(names))
	for _, name := range names {
		b, err := schemaFS.ReadFile(name)
		if err != nil {
			panic(err)
		}
		files[path.Base(name)] = string(b)
	}
	return files
}

func latest() int {
	return migrations[len(migrations)-1].version
}

func Migrate(ctx context.Context, pool *pgxpool.Pool, opts ...Option) error {
	c := newConfig(opts)
	if !validSchema(c.schema) {
		return fmt.Errorf("%w: schema %q", driver.ErrInvalid, c.schema)
	}
	return migrate(ctx, pool, c.schema)
}

func migrate(ctx context.Context, pool *pgxpool.Pool, schema string) error {
	v, err := version(ctx, pool, schema)
	if err != nil {
		return err
	}
	for _, m := range migrations {
		if m.version <= v {
			continue
		}
		if err := versioned.apply(ctx, pool, schema, m.version, m.sql); err != nil {
			return fmt.Errorf("kiln: migrate %s to %d: %w", schema, m.version, err)
		}
	}
	if err := checkVersion(ctx, pool, schema, true); err != nil {
		return err
	}
	todo, err := pending(ctx, pool, schema)
	if err != nil {
		return err
	}
	for _, c := range todo {
		if err := named.apply(ctx, pool, schema, c.name, c.sql); err != nil {
			return fmt.Errorf("kiln: migrate %s with change %s: %w", schema, c.name, err)
		}
	}
	return nil
}

func (l ledger) apply(ctx context.Context, pool *pgxpool.Pool, schema string, key any, sql string) error {
	return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		pc := tx.Conn().PgConn()
		if _, err := pc.Exec(ctx, strings.ReplaceAll(setupSQL, "{s}", schema)).ReadAll(); err != nil {
			return err
		}
		var done bool
		q := fmt.Sprintf("SELECT EXISTS (SELECT 1 FROM %s.%s WHERE %s = $1)", schema, l.table, l.column)
		if err := tx.QueryRow(ctx, q, pgx.QueryExecModeExec, key).Scan(&done); err != nil {
			return err
		}
		if done {
			return nil
		}
		if _, err := pc.Exec(ctx, strings.ReplaceAll(sql, "{s}", schema)).ReadAll(); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES ($1)", schema, l.table, l.column), pgx.QueryExecModeExec, key)
		return err
	})
}

func version(ctx context.Context, pool *pgxpool.Pool, schema string) (int, error) {
	var v int
	q := fmt.Sprintf("SELECT coalesce(max(version), 0) FROM %s.migrations", schema)
	err := pool.QueryRow(ctx, q, pgx.QueryExecModeExec).Scan(&v)
	switch {
	case undefined(err):
		return 0, nil
	case err != nil:
		return 0, fmt.Errorf("kiln: schema version: %w", err)
	}
	return v, nil
}

func pending(ctx context.Context, pool *pgxpool.Pool, schema string) ([]change, error) {
	rows, _ := pool.Query(ctx, fmt.Sprintf("SELECT name FROM %s.schema_changes", schema), pgx.QueryExecModeExec)
	done, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil && !undefined(err) {
		return nil, fmt.Errorf("kiln: schema changes: %w", err)
	}
	var todo []change
	for _, c := range changes {
		if !slices.Contains(done, c.name) {
			todo = append(todo, c)
		}
	}
	return todo, nil
}

func undefined(err error) bool {
	pe := pgError(err)
	return pe != nil && (pe.Code == "42P01" || pe.Code == "3F000")
}

func checkVersion(ctx context.Context, pool *pgxpool.Pool, schema string, migrated bool) error {
	v, err := version(ctx, pool, schema)
	if err != nil {
		return err
	}
	switch {
	case v > latest():
		return fmt.Errorf("kiln: schema %s is at version %d, this binary supports up to %d", schema, v, latest())
	case v < latest() && !migrated:
		return fmt.Errorf("kiln: schema %s is at version %d, want %d: run pgstore.Migrate", schema, v, latest())
	}
	return nil
}

func checkSchema(ctx context.Context, pool *pgxpool.Pool, schema string) error {
	if err := checkVersion(ctx, pool, schema, false); err != nil {
		return err
	}
	todo, err := pending(ctx, pool, schema)
	switch {
	case err != nil:
		return err
	case len(todo) > 0:
		return fmt.Errorf("kiln: schema %s lacks change %s: run pgstore.Migrate", schema, todo[0].name)
	}
	return nil
}

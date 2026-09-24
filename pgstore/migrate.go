package pgstore

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"slices"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rafaelaugustos/kiln/driver"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

type migration struct {
	version int
	sql     string
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
			panic(fmt.Sprintf("pgstore: bad migration name %s", name))
		}
		b, err := migrationFS.ReadFile(name)
		if err != nil {
			panic(err)
		}
		ms = append(ms, migration{version: v, sql: string(b)})
	}
	slices.SortFunc(ms, func(a, b migration) int { return a.version - b.version })
	return ms
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
		if err := apply(ctx, pool, schema, m); err != nil {
			return fmt.Errorf("kiln: migrate %s to %d: %w", schema, m.version, err)
		}
	}
	return checkVersion(ctx, pool, schema, true)
}

func apply(ctx context.Context, pool *pgxpool.Pool, schema string, m migration) error {
	return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		pc := tx.Conn().PgConn()
		setup := fmt.Sprintf(`SET LOCAL lock_timeout = '5s';
SELECT pg_advisory_xact_lock(hashtextextended('%[1]s.migrate', 0));
CREATE SCHEMA IF NOT EXISTS %[1]s;
CREATE TABLE IF NOT EXISTS %[1]s.migrations (version int PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now());`, schema)
		if _, err := pc.Exec(ctx, setup).ReadAll(); err != nil {
			return err
		}
		var done bool
		q := fmt.Sprintf("SELECT EXISTS (SELECT 1 FROM %s.migrations WHERE version = $1)", schema)
		if err := tx.QueryRow(ctx, q, pgx.QueryExecModeExec, m.version).Scan(&done); err != nil {
			return err
		}
		if done {
			return nil
		}
		if _, err := pc.Exec(ctx, strings.ReplaceAll(m.sql, "{s}", schema)).ReadAll(); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, fmt.Sprintf("INSERT INTO %s.migrations (version) VALUES ($1)", schema), pgx.QueryExecModeExec, m.version)
		return err
	})
}

func version(ctx context.Context, pool *pgxpool.Pool, schema string) (int, error) {
	var v int
	q := fmt.Sprintf("SELECT coalesce(max(version), 0) FROM %s.migrations", schema)
	err := pool.QueryRow(ctx, q, pgx.QueryExecModeExec).Scan(&v)
	var pe *pgconn.PgError
	switch {
	case errors.As(err, &pe) && (pe.Code == "42P01" || pe.Code == "3F000"):
		return 0, nil
	case err != nil:
		return 0, fmt.Errorf("kiln: schema version: %w", err)
	}
	return v, nil
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

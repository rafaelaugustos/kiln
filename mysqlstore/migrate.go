package mysqlstore

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"embed"
	"encoding/hex"
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
			panic(fmt.Sprintf("mysqlstore: bad migration name %s", name))
		}
		b, err := migrationFS.ReadFile(name)
		if err != nil {
			panic(err)
		}
		m := migration{version: v}
		for _, stmt := range strings.Split(string(b), ";\n") {
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
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("kiln: migrate: %w", err)
	}
	defer conn.Close()
	var schema sql.NullString
	if err := conn.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&schema); err != nil {
		return fmt.Errorf("kiln: migrate: %w", err)
	}
	if !schema.Valid {
		return fmt.Errorf("%w: the connection has no default database", driver.ErrInvalid)
	}
	lock := "kiln." + schema.String + "." + prefix
	if len(lock) > 64 {
		sum := sha256.Sum256([]byte(lock))
		lock = "kiln." + hex.EncodeToString(sum[:16])
	}
	var got sql.NullInt64
	if err := conn.QueryRowContext(ctx, render("SELECT GET_LOCK(?, 30)", lock)).Scan(&got); err != nil {
		return fmt.Errorf("kiln: migrate: %w", err)
	}
	if got.Int64 != 1 {
		return fmt.Errorf("kiln: migrate: timed out waiting for the migration lock %s", lock)
	}
	defer conn.ExecContext(context.WithoutCancel(ctx), render("DO RELEASE_LOCK(?)", lock))

	create := "CREATE TABLE IF NOT EXISTS " + prefix + "migrations (version INT NOT NULL, applied_at DATETIME(6) NOT NULL, PRIMARY KEY (version))"
	if _, err := conn.ExecContext(ctx, create); err != nil {
		return fmt.Errorf("kiln: migrate: %w", err)
	}
	v, err := version(ctx, conn, prefix)
	if err != nil {
		return err
	}
	r := strings.NewReplacer("{p}", prefix)
	for _, m := range migrations {
		if m.version <= v {
			continue
		}
		for _, stmt := range m.stmts {
			if _, err := conn.ExecContext(ctx, r.Replace(stmt)); err != nil {
				return fmt.Errorf("kiln: migrate %s to %d: %w", schema.String, m.version, err)
			}
		}
		done := render("INSERT INTO "+prefix+"migrations (version, applied_at) VALUES (?, UTC_TIMESTAMP(6))", m.version)
		if _, err := conn.ExecContext(ctx, done); err != nil {
			return fmt.Errorf("kiln: migrate %s to %d: %w", schema.String, m.version, err)
		}
	}
	return checkVersion(ctx, conn, prefix, true)
}

type queryRower interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func version(ctx context.Context, q queryRower, prefix string) (int, error) {
	var v int
	err := q.QueryRowContext(ctx, "SELECT COALESCE(MAX(version), 0) FROM "+prefix+"migrations").Scan(&v)
	if me := mysqlError(err); me != nil && me.Number == errNoTable {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("kiln: schema version: %w", err)
	}
	return v, nil
}

func checkVersion(ctx context.Context, q queryRower, prefix string, migrated bool) error {
	v, err := version(ctx, q, prefix)
	if err != nil {
		return err
	}
	switch {
	case v > latest():
		return fmt.Errorf("kiln: tables %s* are at version %d, this binary supports up to %d", prefix, v, latest())
	case v < latest() && !migrated:
		return fmt.Errorf("kiln: tables %s* are at version %d, want %d: run mysqlstore.Migrate", prefix, v, latest())
	}
	return nil
}

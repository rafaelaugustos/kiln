package mysqlstore

import (
	"cmp"
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

//go:embed migrations/*.sql changes/*.sql
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
			panic(fmt.Sprintf("mysqlstore: bad migration name %s", name))
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
	if err := upgrade(ctx, conn, prefix, schema.String); err != nil {
		return err
	}
	if err := checkVersion(ctx, conn, prefix, true); err != nil {
		return err
	}
	return amend(ctx, conn, prefix, schema.String)
}

func upgrade(ctx context.Context, conn *sql.Conn, prefix, schema string) error {
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
				return fmt.Errorf("kiln: migrate %s to %d: %w", schema, m.version, err)
			}
		}
		done := render("INSERT INTO "+prefix+"migrations (version, applied_at) VALUES (?, UTC_TIMESTAMP(6))", m.version)
		if _, err := conn.ExecContext(ctx, done); err != nil {
			return fmt.Errorf("kiln: migrate %s to %d: %w", schema, m.version, err)
		}
	}
	return nil
}

func amend(ctx context.Context, conn *sql.Conn, prefix, schema string) error {
	create := "CREATE TABLE IF NOT EXISTS " + prefix + "schema_changes (name VARCHAR(255) NOT NULL, " +
		"applied_at DATETIME(6) NOT NULL, PRIMARY KEY (name)) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_bin"
	if _, err := conn.ExecContext(ctx, create); err != nil {
		return fmt.Errorf("kiln: migrate %s: %w", schema, err)
	}
	todo, err := missing(ctx, conn, prefix)
	if err != nil || len(todo) == 0 {
		return err
	}
	if _, err := conn.ExecContext(ctx, "SET SESSION lock_wait_timeout = 5"); err != nil {
		return fmt.Errorf("kiln: migrate %s: %w", schema, err)
	}
	defer conn.ExecContext(context.WithoutCancel(ctx), "SET SESSION lock_wait_timeout = DEFAULT")
	r := strings.NewReplacer("{p}", prefix)
	for _, c := range todo {
		for _, stmt := range c.stmts {
			if _, err := conn.ExecContext(ctx, r.Replace(stmt)); err != nil && !redundant(err) {
				return fmt.Errorf("kiln: migrate %s: change %s: %w", schema, c.name, err)
			}
		}
		record := render("INSERT INTO "+prefix+"schema_changes (name, applied_at) VALUES (?, UTC_TIMESTAMP(6))", c.name)
		if _, err := conn.ExecContext(ctx, record); err != nil {
			return fmt.Errorf("kiln: migrate %s: change %s: %w", schema, c.name, err)
		}
	}
	return nil
}

func missing(ctx context.Context, q querier, prefix string) ([]migration, error) {
	rows, err := q.QueryContext(ctx, "SELECT name FROM "+prefix+"schema_changes")
	if me := mysqlError(err); me != nil && me.Number == errNoTable {
		return changes, nil
	}
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
	if me := mysqlError(err); me != nil && me.Number == errNoTable {
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
		return fmt.Errorf("kiln: tables %s* are at version %d, want %d: run mysqlstore.Migrate", prefix, v, latest())
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
	return fmt.Errorf("kiln: tables %s* lack schema change %s: run mysqlstore.Migrate", prefix, todo[0].name)
}

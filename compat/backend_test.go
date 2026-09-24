package compat

import (
	"cmp"
	"context"
	"database/sql"
	"fmt"
	"math/rand/v2"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rafaelaugustos/kiln/driver"
	"github.com/rafaelaugustos/kiln/mysqlstore"
	"github.com/rafaelaugustos/kiln/pgstore"
)

type backend interface {
	flags() []string
	open(ctx context.Context, migrate bool) (driver.Store, func(), error)
	migrate(ctx context.Context) error
	objects(ctx context.Context) (map[string]string, error)
	digest(ctx context.Context, table string, columns []string) (string, error)
	versions(ctx context.Context) (map[int]string, error)
	addVersion(ctx context.Context, v int) error
	count(ctx context.Context, cond string) (int, error)
}

type pgBackend struct {
	dsn    string
	pool   *pgxpool.Pool
	schema string
}

func newPostgres(t *testing.T) backend {
	dsn := cmp.Or(os.Getenv("KILN_DATABASE_URL"), "postgres://kiln:kiln@localhost:55432/kiln?sslmode=disable")
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Skipf("postgres unavailable: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("postgres unavailable: %v", err)
	}
	b := &pgBackend{dsn: dsn, pool: pool, schema: fmt.Sprintf("compat_%x", rand.Uint64())}
	t.Cleanup(func() {
		if _, err := pool.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+b.schema+" CASCADE"); err != nil {
			t.Errorf("drop schema %s: %v", b.schema, err)
		}
		pool.Close()
	})
	return b
}

func (b *pgBackend) flags() []string {
	return []string{"-backend=postgres", "-dsn=" + b.dsn, "-schema=" + b.schema}
}

func (b *pgBackend) open(ctx context.Context, migrate bool) (driver.Store, func(), error) {
	opts := []pgstore.Option{pgstore.Schema(b.schema)}
	if !migrate {
		opts = append(opts, pgstore.NoMigrate())
	}
	s, err := pgstore.New(ctx, b.pool, opts...)
	if err != nil {
		return nil, nil, err
	}
	return s, s.Close, nil
}

func (b *pgBackend) migrate(ctx context.Context) error {
	return pgstore.Migrate(ctx, b.pool, pgstore.Schema(b.schema))
}

const pgObjects = `SELECT 'column ' || table_name || '.' || column_name,
	concat_ws(' ', data_type, udt_name, is_nullable, coalesce(column_default, 'NULL'))
FROM information_schema.columns WHERE table_schema = $1::text
UNION ALL
SELECT 'index ' || indexname, indexdef FROM pg_indexes WHERE schemaname = $1::text
UNION ALL
SELECT 'constraint ' || c.conrelid::regclass::text || '.' || c.conname, pg_get_constraintdef(c.oid)
FROM pg_constraint c JOIN pg_namespace n ON n.oid = c.connamespace WHERE n.nspname = $1::text
UNION ALL
SELECT 'type ' || t.typname, string_agg(e.enumlabel, ',' ORDER BY e.enumsortorder)
FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace JOIN pg_enum e ON e.enumtypid = t.oid
WHERE n.nspname = $1::text GROUP BY t.typname
UNION ALL
SELECT 'function ' || p.oid::regprocedure::text, pg_get_functiondef(p.oid)
FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = $1::text
UNION ALL
SELECT 'sequence ' || sequencename, concat_ws(' ', data_type, increment_by, min_value, max_value)
FROM pg_sequences WHERE schemaname = $1::text`

func (b *pgBackend) objects(ctx context.Context) (map[string]string, error) {
	rows, err := b.pool.Query(ctx, pgObjects, b.schema)
	if err != nil {
		return nil, err
	}
	objs := make(map[string]string)
	var k, v string
	_, err = pgx.ForEachRow(rows, []any{&k, &v}, func() error {
		objs[k] = v
		return nil
	})
	return objs, err
}

func (b *pgBackend) digest(ctx context.Context, table string, columns []string) (string, error) {
	q := fmt.Sprintf(`SELECT count(*) || ' ' || md5(coalesce(string_agg(h, '' ORDER BY h), ''))
FROM (SELECT md5(ROW("%s")::text) AS h FROM %s.%s) t`, strings.Join(columns, `", "`), b.schema, table)
	var d string
	err := b.pool.QueryRow(ctx, q).Scan(&d)
	return d, err
}

func (b *pgBackend) versions(ctx context.Context) (map[int]string, error) {
	rows, err := b.pool.Query(ctx, "SELECT version, applied_at::text FROM "+b.schema+".migrations")
	if err != nil {
		return nil, err
	}
	vs := make(map[int]string)
	var (
		v  int
		at string
	)
	_, err = pgx.ForEachRow(rows, []any{&v, &at}, func() error {
		vs[v] = at
		return nil
	})
	return vs, err
}

func (b *pgBackend) addVersion(ctx context.Context, v int) error {
	_, err := b.pool.Exec(ctx, "INSERT INTO "+b.schema+".migrations (version) VALUES ($1)", v)
	return err
}

func (b *pgBackend) count(ctx context.Context, cond string) (int, error) {
	var n int
	err := b.pool.QueryRow(ctx, "SELECT count(*) FROM "+b.schema+".jobs WHERE "+cond).Scan(&n)
	return n, err
}

type myBackend struct {
	dsn    string
	db     *sql.DB
	prefix string
}

func newMySQL(t *testing.T) backend {
	cfg, err := mysql.ParseDSN(cmp.Or(os.Getenv("KILN_MYSQL_DSN"), "kiln:kiln@tcp(localhost:53306)/kiln"))
	if err != nil {
		t.Fatalf("KILN_MYSQL_DSN: %v", err)
	}
	admin, err := connect(cfg)
	if err != nil {
		t.Skipf("mysql unavailable: %v", err)
	}
	name := fmt.Sprintf("compat_%x", rand.Uint64())
	if _, err := admin.Exec("CREATE DATABASE " + name); err != nil {
		admin.Close()
		t.Fatalf("create database: %v", err)
	}
	cfg = cfg.Clone()
	cfg.DBName = name
	db, err := connect(cfg)
	if err != nil {
		t.Fatalf("connect to %s: %v", name, err)
	}
	db.SetMaxIdleConns(16)
	t.Cleanup(func() {
		db.Close()
		if _, err := admin.Exec("DROP DATABASE IF EXISTS " + name); err != nil {
			t.Errorf("drop database %s: %v", name, err)
		}
		admin.Close()
	})
	return &myBackend{dsn: cfg.FormatDSN(), db: db, prefix: "kiln_"}
}

func connect(cfg *mysql.Config) (*sql.DB, error) {
	conn, err := mysql.NewConnector(cfg)
	if err != nil {
		return nil, err
	}
	db := sql.OpenDB(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func (b *myBackend) flags() []string {
	return []string{"-backend=mysql", "-dsn=" + b.dsn, "-prefix=" + b.prefix}
}

func (b *myBackend) open(ctx context.Context, migrate bool) (driver.Store, func(), error) {
	opts := []mysqlstore.Option{mysqlstore.Prefix(b.prefix)}
	if !migrate {
		opts = append(opts, mysqlstore.NoMigrate())
	}
	s, err := mysqlstore.New(ctx, b.db, opts...)
	if err != nil {
		return nil, nil, err
	}
	return s, s.Close, nil
}

func (b *myBackend) migrate(ctx context.Context) error {
	return mysqlstore.Migrate(ctx, b.db, mysqlstore.Prefix(b.prefix))
}

var myObjects = []string{
	`SELECT 'column', TABLE_NAME, COLUMN_NAME,
	CONCAT_WS(' ', COLUMN_TYPE, IS_NULLABLE, COALESCE(COLUMN_DEFAULT, 'NULL'), EXTRA, COALESCE(COLLATION_NAME, ''))
FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE()`,
	`SELECT 'index', TABLE_NAME, INDEX_NAME,
	CONCAT(NON_UNIQUE, ' ', GROUP_CONCAT(CONCAT_WS(' ', COLUMN_NAME, COLLATION, SUB_PART) ORDER BY SEQ_IN_INDEX SEPARATOR ', '))
FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() GROUP BY TABLE_NAME, INDEX_NAME, NON_UNIQUE`,
	`SELECT 'table', TABLE_NAME, '', CONCAT_WS(' ', ENGINE, TABLE_COLLATION)
FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE()`,
}

func (b *myBackend) objects(ctx context.Context) (map[string]string, error) {
	objs := make(map[string]string)
	for _, q := range myObjects {
		if err := b.collect(ctx, q, objs); err != nil {
			return nil, err
		}
	}
	return objs, nil
}

func (b *myBackend) collect(ctx context.Context, q string, objs map[string]string) error {
	rows, err := b.db.QueryContext(ctx, q)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var kind, table, name, def string
		if err := rows.Scan(&kind, &table, &name, &def); err != nil {
			return err
		}
		key := kind + " " + strings.TrimPrefix(table, b.prefix)
		if name != "" {
			key += "." + name
		}
		objs[key] = def
	}
	return rows.Err()
}

func (b *myBackend) digest(ctx context.Context, table string, columns []string) (string, error) {
	q := fmt.Sprintf("SELECT CONCAT(COUNT(*), ' ', COALESCE(BIT_XOR(CRC32(CONCAT_WS(',', QUOTE(`%s`)))), 0)) FROM %s%s",
		strings.Join(columns, "`), QUOTE(`"), b.prefix, table)
	var d string
	err := b.db.QueryRowContext(ctx, q).Scan(&d)
	return d, err
}

func (b *myBackend) versions(ctx context.Context) (map[int]string, error) {
	rows, err := b.db.QueryContext(ctx, "SELECT version, CAST(applied_at AS CHAR) FROM "+b.prefix+"migrations")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	vs := make(map[int]string)
	for rows.Next() {
		var (
			v  int
			at string
		)
		if err := rows.Scan(&v, &at); err != nil {
			return nil, err
		}
		vs[v] = at
	}
	return vs, rows.Err()
}

func (b *myBackend) addVersion(ctx context.Context, v int) error {
	_, err := b.db.ExecContext(ctx, "INSERT INTO "+b.prefix+"migrations (version, applied_at) VALUES (?, UTC_TIMESTAMP(6))", v)
	return err
}

func (b *myBackend) count(ctx context.Context, cond string) (int, error) {
	var n int
	err := b.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+b.prefix+"jobs WHERE "+cond).Scan(&n)
	return n, err
}

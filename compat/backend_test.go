package compat

import (
	"cmp"
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rafaelaugustos/kiln/driver"
	"github.com/rafaelaugustos/kiln/mysqlstore"
	"github.com/rafaelaugustos/kiln/pgstore"
	"github.com/rafaelaugustos/kiln/sqlitestore"
	_ "modernc.org/sqlite"
)

type backend interface {
	module() string
	flags() []string
	open(ctx context.Context, migrate bool) (driver.Store, func(), error)
	migrate(ctx context.Context) error
	objects(ctx context.Context) (map[string]string, error)
	digest(ctx context.Context, table string, columns []string) (string, error)
	versions(ctx context.Context) (map[int]string, error)
	addVersion(ctx context.Context, v int) error
	changes(ctx context.Context) ([]string, error)
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

func (b *pgBackend) module() string { return "pgstore" }

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

const pgObjects = `SELECT 'column ' || table_name || '.' || column_name, concat_ws(' ',
	CASE WHEN is_nullable = 'YES' OR column_default IS NOT NULL OR is_identity = 'YES' OR is_generated = 'ALWAYS' THEN 'optional' ELSE 'required' END,
	data_type, udt_name, is_nullable, coalesce(column_default, 'NULL'))
FROM information_schema.columns WHERE table_schema = $1::text
UNION ALL
SELECT 'index ' || tablename || '.' || indexname, indexdef FROM pg_indexes WHERE schemaname = $1::text
UNION ALL
SELECT 'constraint ' || r.relname || '.' || c.conname, pg_get_constraintdef(c.oid)
FROM pg_constraint c JOIN pg_class r ON r.oid = c.conrelid JOIN pg_namespace n ON n.oid = r.relnamespace
WHERE n.nspname = $1::text
UNION ALL
SELECT 'storage ' || r.relname, r.relfilenode::text
FROM pg_class r JOIN pg_namespace n ON n.oid = r.relnamespace WHERE n.nspname = $1::text AND r.relkind = 'r'
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

func (b *pgBackend) changes(ctx context.Context) ([]string, error) {
	rows, _ := b.pool.Query(ctx, "SELECT name FROM "+b.schema+".schema_changes")
	return pgx.CollectRows(rows, pgx.RowTo[string])
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

func (b *myBackend) module() string { return "mysqlstore" }

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
	`SELECT 'column', TABLE_NAME, COLUMN_NAME, CONCAT_WS(' ',
	IF(IS_NULLABLE = 'YES' OR COLUMN_DEFAULT IS NOT NULL OR EXTRA LIKE '%auto_increment%' OR EXTRA LIKE '%GENERATED%', 'optional', 'required'),
	COLUMN_TYPE, IS_NULLABLE, COALESCE(COLUMN_DEFAULT, 'NULL'), EXTRA, COALESCE(COLLATION_NAME, ''))
FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE()`,
	`SELECT 'index', TABLE_NAME, INDEX_NAME,
	CONCAT(NON_UNIQUE, ' ', GROUP_CONCAT(CONCAT_WS(' ', COLUMN_NAME, COLLATION, SUB_PART) ORDER BY SEQ_IN_INDEX SEPARATOR ', '))
FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() GROUP BY TABLE_NAME, INDEX_NAME, NON_UNIQUE`,
	`SELECT 'table', TABLE_NAME, '', CONCAT_WS(' ', ENGINE, TABLE_COLLATION)
FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE()`,
	`SELECT 'storage', SUBSTRING_INDEX(NAME, '/', -1), '', CONCAT(TABLE_ID, ' ', SPACE)
FROM information_schema.INNODB_TABLES WHERE SUBSTRING_INDEX(NAME, '/', 1) = DATABASE()`,
}

func (b *myBackend) objects(ctx context.Context) (map[string]string, error) {
	objs := make(map[string]string)
	for _, q := range myObjects {
		if err := collect(ctx, b.db, q, b.prefix, objs); err != nil {
			return nil, err
		}
	}
	return objs, nil
}

func (b *myBackend) digest(ctx context.Context, table string, columns []string) (string, error) {
	q := fmt.Sprintf("SELECT CONCAT(COUNT(*), ' ', COALESCE(BIT_XOR(CRC32(CONCAT_WS(',', QUOTE(`%s`)))), 0)) FROM %s%s",
		strings.Join(columns, "`), QUOTE(`"), b.prefix, table)
	var d string
	err := b.db.QueryRowContext(ctx, q).Scan(&d)
	return d, err
}

func (b *myBackend) versions(ctx context.Context) (map[int]string, error) {
	return versions(ctx, b.db, "SELECT version, CAST(applied_at AS CHAR) FROM "+b.prefix+"migrations")
}

func (b *myBackend) addVersion(ctx context.Context, v int) error {
	_, err := b.db.ExecContext(ctx, "INSERT INTO "+b.prefix+"migrations (version, applied_at) VALUES (?, UTC_TIMESTAMP(6))", v)
	return err
}

func (b *myBackend) changes(ctx context.Context) ([]string, error) {
	return names(ctx, b.db, "SELECT name FROM "+b.prefix+"schema_changes")
}

func (b *myBackend) count(ctx context.Context, cond string) (int, error) {
	var n int
	err := b.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+b.prefix+"jobs WHERE "+cond).Scan(&n)
	return n, err
}

const pragmas = "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_txlock=immediate"

type liteBackend struct {
	path   string
	db     *sql.DB
	prefix string
}

func newSQLite(t *testing.T) backend {
	path := filepath.Join(t.TempDir(), "kiln.db")
	db, err := sql.Open("sqlite", "file:"+path+pragmas)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return &liteBackend{path: path, db: db, prefix: "kiln_"}
}

func (b *liteBackend) module() string { return "sqlitestore" }

func (b *liteBackend) flags() []string {
	return []string{"-backend=sqlite", "-dsn=" + b.path, "-prefix=" + b.prefix}
}

func (b *liteBackend) open(ctx context.Context, migrate bool) (driver.Store, func(), error) {
	opts := []sqlitestore.Option{sqlitestore.Prefix(b.prefix)}
	if !migrate {
		opts = append(opts, sqlitestore.NoMigrate())
	}
	s, err := sqlitestore.New(ctx, b.db, opts...)
	if err != nil {
		return nil, nil, err
	}
	return s, s.Close, nil
}

func (b *liteBackend) migrate(ctx context.Context) error {
	return sqlitestore.Migrate(ctx, b.db, sqlitestore.Prefix(b.prefix))
}

var liteObjects = []string{
	`SELECT 'column', m.name, c.name, concat_ws(' ',
	CASE WHEN c."notnull" = 0 OR c.dflt_value IS NOT NULL THEN 'optional' ELSE 'required' END,
	c.type, c."notnull", coalesce(c.dflt_value, 'NULL'), c.pk)
FROM sqlite_schema m JOIN pragma_table_info(m.name) c WHERE m.type = 'table'`,
	`SELECT 'index', m.tbl_name, m.name, coalesce(m.sql, 'auto') FROM sqlite_schema m WHERE m.type = 'index'`,
	`SELECT 'table', t.name, '', concat_ws(' ', t.type, t.wr, t.strict)
FROM pragma_table_list t WHERE t.schema = 'main' AND t.name NOT LIKE 'sqlite%'`,
	`SELECT 'storage', m.name, '', m.rootpage FROM sqlite_schema m WHERE m.type = 'table'`,
}

func (b *liteBackend) objects(ctx context.Context) (map[string]string, error) {
	objs := make(map[string]string)
	for _, q := range liteObjects {
		if err := collect(ctx, b.db, q, b.prefix, objs); err != nil {
			return nil, err
		}
	}
	return objs, nil
}

func (b *liteBackend) digest(ctx context.Context, table string, columns []string) (string, error) {
	q := fmt.Sprintf(`SELECT count(*), coalesce(group_concat(r, char(10) ORDER BY r), '')
FROM (SELECT quote("%s") AS r FROM %s%s)`, strings.Join(columns, `") || ',' || quote("`), b.prefix, table)
	var (
		n    int
		rows string
	)
	if err := b.db.QueryRowContext(ctx, q).Scan(&n, &rows); err != nil {
		return "", err
	}
	return fmt.Sprintf("%d %x", n, sha256.Sum256([]byte(rows))), nil
}

func (b *liteBackend) versions(ctx context.Context) (map[int]string, error) {
	return versions(ctx, b.db, "SELECT version, CAST(applied_at AS TEXT) FROM "+b.prefix+"migrations")
}

func (b *liteBackend) addVersion(ctx context.Context, v int) error {
	_, err := b.db.ExecContext(ctx, "INSERT INTO "+b.prefix+"migrations (version, applied_at) VALUES (?, unixepoch() * 1000000)", v)
	return err
}

func (b *liteBackend) changes(ctx context.Context) ([]string, error) {
	return names(ctx, b.db, "SELECT name FROM "+b.prefix+"schema_changes")
}

func (b *liteBackend) count(ctx context.Context, cond string) (int, error) {
	var n int
	err := b.db.QueryRowContext(ctx, "SELECT count(*) FROM "+b.prefix+"jobs WHERE "+cond).Scan(&n)
	return n, err
}

func collect(ctx context.Context, db *sql.DB, q, prefix string, objs map[string]string) error {
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var kind, table, name, def string
		if err := rows.Scan(&kind, &table, &name, &def); err != nil {
			return err
		}
		key := kind + " " + strings.TrimPrefix(table, prefix)
		if name != "" {
			key += "." + name
		}
		objs[key] = def
	}
	return rows.Err()
}

func versions(ctx context.Context, db *sql.DB, q string) (map[int]string, error) {
	rows, err := db.QueryContext(ctx, q)
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

func names(ctx context.Context, db *sql.DB, q string) ([]string, error) {
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

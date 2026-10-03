package main

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/rafaelaugustos/kiln/driver"
	"github.com/rafaelaugustos/kiln/mysqlstore"
	"github.com/rafaelaugustos/kiln/pgstore"
)

const (
	pgSchema    = "kiln_soak"
	mysqlPrefix = "kiln_soak_"
)

type backend struct {
	db        *sql.DB
	store     driver.Store
	writer    func(*sql.Tx) driver.TxWriter
	close     func()
	poll      time.Duration
	pg        bool
	enqueued  string
	runs      string
	insertRun string
	updateRun string
}

func open(ctx context.Context, o options, reset bool) (*backend, error) {
	if o.backend == "mysql" {
		return openMySQL(ctx, o.dsn, reset)
	}
	return openPostgres(ctx, o.dsn, reset)
}

func openPostgres(ctx context.Context, dsn string, reset bool) (*backend, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(8)
	if reset {
		err = execAll(ctx, db,
			"DROP SCHEMA IF EXISTS "+pgSchema+" CASCADE",
			"DROP TABLE IF EXISTS public.soak_enqueued, public.soak_runs",
			"CREATE TABLE public.soak_enqueued (id bigint PRIMARY KEY, kind text NOT NULL, lkey text NOT NULL)",
			"CREATE TABLE public.soak_runs (run text PRIMARY KEY, job bigint NOT NULL, attempt int NOT NULL, inst text NOT NULL, started bigint NOT NULL, finished bigint, result text)",
			"CREATE INDEX ON public.soak_runs (job, attempt)",
		)
		if err != nil {
			db.Close()
			return nil, err
		}
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		db.Close()
		return nil, err
	}
	defer pool.Close()
	st, err := pgstore.New(ctx, pool, pgstore.Schema(pgSchema))
	if err != nil {
		db.Close()
		return nil, err
	}
	return &backend{
		db:        db,
		store:     st,
		writer:    func(tx *sql.Tx) driver.TxWriter { return st.SQLTx(tx) },
		close:     func() { st.Close(); db.Close() },
		poll:      time.Second,
		pg:        true,
		enqueued:  "public.soak_enqueued",
		runs:      "public.soak_runs",
		insertRun: "INSERT INTO public.soak_runs (run, job, attempt, inst, started) VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING",
		updateRun: "UPDATE public.soak_runs SET finished = $1, result = $2 WHERE run = $3",
	}, nil
}

func openMySQL(ctx context.Context, dsn string, reset bool) (*backend, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(8)
	if reset {
		if err := resetMySQL(ctx, db); err != nil {
			db.Close()
			return nil, err
		}
	}
	sdb, err := sql.Open("mysql", dsn)
	if err != nil {
		db.Close()
		return nil, err
	}
	sdb.SetMaxOpenConns(16)
	st, err := mysqlstore.New(ctx, sdb, mysqlstore.Prefix(mysqlPrefix))
	if err != nil {
		sdb.Close()
		db.Close()
		return nil, err
	}
	return &backend{
		db:        db,
		store:     st,
		writer:    func(tx *sql.Tx) driver.TxWriter { return st.Tx(tx) },
		close:     func() { st.Close(); sdb.Close(); db.Close() },
		poll:      100 * time.Millisecond,
		enqueued:  "soak_enqueued",
		runs:      "soak_runs",
		insertRun: "INSERT IGNORE INTO soak_runs (run, job, attempt, inst, started) VALUES (?, ?, ?, ?, ?)",
		updateRun: "UPDATE soak_runs SET finished = ?, result = ? WHERE run = ?",
	}, nil
}

func resetMySQL(ctx context.Context, db *sql.DB) error {
	rows, err := db.QueryContext(ctx, "SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name LIKE ?", strings.ReplaceAll(mysqlPrefix, "_", `\_`)+"%")
	if err != nil {
		return err
	}
	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			rows.Close()
			return err
		}
		names = append(names, "`"+n+"`")
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	stmts := []string{
		"DROP TABLE IF EXISTS soak_enqueued, soak_runs",
		"CREATE TABLE soak_enqueued (id BIGINT PRIMARY KEY, kind VARCHAR(16) NOT NULL, lkey VARCHAR(32) NOT NULL)",
		"CREATE TABLE soak_runs (run VARCHAR(48) PRIMARY KEY, job BIGINT NOT NULL, attempt INT NOT NULL, inst VARCHAR(16) NOT NULL, started BIGINT NOT NULL, finished BIGINT NULL, result VARCHAR(16) NULL, INDEX (job, attempt))",
	}
	if len(names) > 0 {
		stmts = append([]string{"DROP TABLE IF EXISTS " + strings.Join(names, ", ")}, stmts...)
	}
	return execAll(ctx, db, stmts...)
}

func execAll(ctx context.Context, db *sql.DB, stmts ...string) error {
	for _, s := range stmts {
		if _, err := db.ExecContext(ctx, s); err != nil {
			return fmt.Errorf("%s: %w", s, err)
		}
	}
	return nil
}

func (b *backend) values(rows, cols int) string {
	var sb strings.Builder
	for r := range rows {
		if r > 0 {
			sb.WriteString(", ")
		}
		sb.WriteByte('(')
		for c := range cols {
			if c > 0 {
				sb.WriteString(", ")
			}
			if b.pg {
				sb.WriteString("$" + strconv.Itoa(r*cols+c+1))
			} else {
				sb.WriteByte('?')
			}
		}
		sb.WriteByte(')')
	}
	return sb.String()
}

package mysqlstore

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand/v2"
	"os"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/rafaelaugustos/kiln/driver"
)

func dsn() string {
	if v := os.Getenv("KILN_MYSQL_DSN"); v != "" {
		return v
	}
	return "kiln:kiln@tcp(localhost:53306)/kiln?transaction_isolation=%27REPEATABLE-READ%27"
}

func connect(tb testing.TB) *sql.DB {
	tb.Helper()
	db, err := sql.Open("mysql", dsn())
	if err != nil {
		tb.Skipf("mysql unavailable: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		tb.Skipf("mysql unavailable: %v", err)
	}
	return db
}

func database(tb testing.TB, edit ...func(*mysql.Config)) *sql.DB {
	tb.Helper()
	admin := connect(tb)
	name := fmt.Sprintf("kt_%x", rand.Uint64())
	if _, err := admin.Exec("CREATE DATABASE " + name); err != nil {
		admin.Close()
		tb.Fatal(err)
	}
	cfg, err := mysql.ParseDSN(dsn())
	if err != nil {
		tb.Fatal(err)
	}
	cfg.DBName = name
	for _, e := range edit {
		e(cfg)
	}
	conn, err := mysql.NewConnector(cfg)
	if err != nil {
		tb.Fatal(err)
	}
	db := sql.OpenDB(conn)
	db.SetMaxIdleConns(64)
	tb.Cleanup(func() {
		db.Close()
		if _, err := admin.Exec("DROP DATABASE " + name); err != nil {
			tb.Errorf("drop database: %v", err)
		}
		admin.Close()
	})
	return db
}

func open(tb testing.TB, opts ...Option) *Store {
	tb.Helper()
	s, err := New(context.Background(), database(tb), opts...)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(s.Close)
	return s
}

func job(kind string, opts ...func(*driver.InsertParams)) driver.InsertParams {
	p := driver.InsertParams{Kind: kind, Queue: "default", Args: []byte(`{}`), MaxAttempts: 3}
	for _, o := range opts {
		o(&p)
	}
	return p
}

func insert(tb testing.TB, s *Store, jobs ...driver.InsertParams) []driver.Inserted {
	tb.Helper()
	res, err := s.Insert(context.Background(), jobs)
	if err != nil {
		tb.Fatalf("insert: %v", err)
	}
	return res
}

func claim(tb testing.TB, s *Store, limit int, queues ...string) []driver.Job {
	tb.Helper()
	if len(queues) == 0 {
		queues = []string{"default"}
	}
	jobs, err := s.Claim(context.Background(), driver.ClaimQuery{Queues: queues, Limit: limit, Server: "srv"})
	if err != nil {
		tb.Fatalf("claim: %v", err)
	}
	return jobs
}

func finish(tb testing.TB, s *Store, outs ...driver.Outcome) []driver.Result {
	tb.Helper()
	res, err := s.Finish(context.Background(), "srv", outs)
	if err != nil {
		tb.Fatalf("finish: %v", err)
	}
	return res
}

func record(tb testing.TB, s *Store, id int64) driver.Record {
	tb.Helper()
	r, err := s.Job(context.Background(), id)
	if err != nil {
		tb.Fatalf("job %d: %v", id, err)
	}
	return r
}

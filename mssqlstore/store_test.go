package mssqlstore

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand/v2"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/microsoft/go-mssqldb"
	"github.com/rafaelaugustos/kiln/driver"
)

func dsn() string {
	if v := os.Getenv("KILN_MSSQL_DSN"); v != "" {
		return v
	}
	return "sqlserver://sa:Kiln-Passw0rd@localhost:51433?database=kiln"
}

var pool struct {
	once sync.Once
	db   *sql.DB
	err  error
}

func connect(tb testing.TB) *sql.DB {
	tb.Helper()
	pool.once.Do(func() {
		db, err := sql.Open("sqlserver", dsn())
		if err != nil {
			pool.err = err
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := db.PingContext(ctx); err != nil {
			db.Close()
			pool.err = err
			return
		}
		db.SetMaxIdleConns(64)
		pool.db = db
	})
	if pool.err != nil {
		tb.Skipf("sql server unavailable: %v", pool.err)
	}
	return pool.db
}

func fresh(tb testing.TB) (*sql.DB, string) {
	tb.Helper()
	db := connect(tb)
	prefix := fmt.Sprintf("kt%08x_", rand.Uint32())
	tb.Cleanup(func() {
		if err := drop(db, prefix); err != nil {
			tb.Errorf("drop %s*: %v", prefix, err)
		}
	})
	return db, prefix
}

func drop(db *sql.DB, prefix string) error {
	ctx := context.Background()
	rows, err := db.QueryContext(ctx, "SELECT name, type FROM sys.objects WHERE type IN ('U', 'SO') AND LEFT(name, LEN(@p)) = @p",
		sql.Named("p", prefix))
	if err != nil {
		return err
	}
	var tables, seqs []string
	for rows.Next() {
		var name, kind string
		if err := rows.Scan(&name, &kind); err != nil {
			rows.Close()
			return err
		}
		if strings.TrimSpace(kind) == "U" {
			tables = append(tables, name)
		} else {
			seqs = append(seqs, name)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	var stmt string
	if len(tables) > 0 {
		stmt = "DROP TABLE " + strings.Join(tables, ", ") + ";"
	}
	if len(seqs) > 0 {
		stmt += "DROP SEQUENCE " + strings.Join(seqs, ", ") + ";"
	}
	if stmt == "" {
		return nil
	}
	_, err = db.ExecContext(ctx, stmt)
	return err
}

func open(tb testing.TB, opts ...Option) *Store {
	tb.Helper()
	db, prefix := fresh(tb)
	s, err := New(context.Background(), db, append([]Option{Prefix(prefix)}, opts...)...)
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

func limited(key string, n int) func(*driver.InsertParams) {
	return func(p *driver.InsertParams) { p.LimitKey, p.LimitMax = key, n }
}

func insert(tb testing.TB, s *Store, jobs ...driver.InsertParams) []driver.Inserted {
	tb.Helper()
	res, err := s.Insert(context.Background(), jobs)
	if err != nil {
		tb.Fatalf("insert: %v", err)
	}
	return res
}

func claim(tb testing.TB, s *Store, limit int) []driver.Job {
	tb.Helper()
	jobs, err := s.Claim(context.Background(), driver.ClaimQuery{Queues: []string{"default"}, Limit: limit, Server: "srv"})
	if err != nil {
		tb.Fatalf("claim: %v", err)
	}
	return jobs
}

func record(tb testing.TB, s *Store, id int64) driver.Record {
	tb.Helper()
	r, err := s.Job(context.Background(), id)
	if err != nil {
		tb.Fatalf("job %d: %v", id, err)
	}
	return r
}

func TestCloseTwice(t *testing.T) {
	t.Parallel()
	s := open(t, Bus(newBus()))
	s.Close()
	s.Close()
}

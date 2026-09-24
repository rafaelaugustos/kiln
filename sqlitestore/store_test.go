package sqlitestore

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/rafaelaugustos/kiln/driver"
	_ "modernc.org/sqlite"
)

const pragmas = "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_txlock=immediate"

func connect(tb testing.TB, path string) *sql.DB {
	tb.Helper()
	db, err := sql.Open("sqlite", "file:"+path+pragmas)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { db.Close() })
	return db
}

func database(tb testing.TB) *sql.DB {
	tb.Helper()
	return connect(tb, filepath.Join(tb.TempDir(), "kiln.db"))
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

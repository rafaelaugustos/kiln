package pgstore

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rafaelaugustos/kiln/driver"
)

func dbURL() string {
	if u := os.Getenv("KILN_DATABASE_URL"); u != "" {
		return u
	}
	return "postgres://kiln:kiln@localhost:55432/kiln?sslmode=disable"
}

func connect(t testing.TB) *pgxpool.Pool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dbURL())
	if err != nil {
		t.Skipf("postgres unavailable: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("postgres unavailable: %v", err)
	}
	return pool
}

var execModes = map[string]pgx.QueryExecMode{
	"simple":   pgx.QueryExecModeSimpleProtocol,
	"exec":     pgx.QueryExecModeExec,
	"describe": pgx.QueryExecModeCacheDescribe,
}

func open(t testing.TB, opts ...Option) *Store {
	t.Helper()
	pool := connect(t)
	schema := fmt.Sprintf("kt_%x", rand.Uint64())
	opts = append([]Option{Schema(schema)}, opts...)
	if m, ok := execModes[os.Getenv("KILN_EXEC_MODE")]; ok {
		opts = append(opts, ExecMode(m))
	}
	s, err := New(context.Background(), pool, opts...)
	if err != nil {
		pool.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		s.Close()
		if _, err := pool.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Errorf("drop schema: %v", err)
		}
		pool.Close()
	})
	return s
}

func job(kind string, opts ...func(*driver.InsertParams)) driver.InsertParams {
	p := driver.InsertParams{Kind: kind, Queue: "default", Args: []byte(`{}`), MaxAttempts: 3}
	for _, o := range opts {
		o(&p)
	}
	return p
}

func insert(t testing.TB, s *Store, jobs ...driver.InsertParams) []driver.Inserted {
	t.Helper()
	res, err := s.Insert(context.Background(), jobs)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	return res
}

func claim(t testing.TB, s *Store, limit int, queues ...string) []driver.Job {
	t.Helper()
	if len(queues) == 0 {
		queues = []string{"default"}
	}
	jobs, err := s.Claim(context.Background(), driver.ClaimQuery{Queues: queues, Limit: limit, Server: "srv"})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	return jobs
}

func finish(t testing.TB, s *Store, outs ...driver.Outcome) []driver.Result {
	t.Helper()
	res, err := s.Finish(context.Background(), "srv", outs)
	if err != nil {
		t.Fatalf("finish: %v", err)
	}
	return res
}

func record(t testing.TB, s *Store, id int64) driver.Record {
	t.Helper()
	r, err := s.Job(context.Background(), id)
	if err != nil {
		t.Fatalf("job %d: %v", id, err)
	}
	return r
}

func TestStatementsPrepare(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	c, err := s.pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Release()
	s.q.each(func(_ *string, text string) {
		sql := strings.ReplaceAll(strings.ReplaceAll(text, "{s}", s.schema), "%s", "true")
		if _, err := c.Conn().Prepare(ctx, "", sql); err != nil {
			t.Errorf("%v\n%s", err, sql)
		}
	})
	for _, shape := range []claimShape{{1, false}, {3, true}} {
		if _, err := c.Conn().Prepare(ctx, "", s.claimSQL(shape)); err != nil {
			t.Errorf("claim %+v: %v", shape, err)
		}
	}
}

package pgstore

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

func TestPlansUsePartialIndexes(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	gen := `
INSERT INTO {s}.jobs (id, state, queue, kind, priority, max_attempts, run_at, args, server, attempted_at, limit_key, claim, attempt)
SELECT g,
	(CASE WHEN g % 50 < 20 THEN 'enqueued' WHEN g % 50 < 45 THEN 'scheduled' WHEN g % 50 < 47 THEN 'succeeded'
		WHEN g % 50 = 47 THEN 'processing' WHEN g % 50 = 48 THEN 'throttled' ELSE 'failed' END)::{s}.state,
	'q' || (g % 5), 'k' || (g % 20), (g % 3)::smallint, 10,
	CASE WHEN g % 50 BETWEEN 20 AND 44 THEN now() + (g % 86400) * interval '1 second' ELSE now() END,
	'{}', CASE WHEN g % 50 = 47 THEN 'srv' || (g % 20) END, now(),
	CASE WHEN g % 50 = 48 THEN 'key' || (g % 50) END, 1, 1
FROM generate_series(1, 60000) g;
DELETE FROM {s}.jobs WHERE state = 'succeeded';
INSERT INTO {s}.limits (key, max, active) SELECT 'key' || g, 1, 1 FROM generate_series(0, 49) g;
INSERT INTO {s}.deps (batch, parent_id, job_id, mask) SELECT false, g % 20000 + 1, 100000 + g, 1 FROM generate_series(1, 40000) g;
INSERT INTO {s}.uniques (key, job_id, expires_at)
SELECT decode(md5(g::text), 'hex'), g, CASE WHEN g % 100 = 0 THEN now() - interval '1 hour' END FROM generate_series(1, 20000) g;
ANALYZE {s}.jobs;
ANALYZE {s}.limits;
ANALYZE {s}.deps;
ANALYZE {s}.uniques;`
	if _, err := s.pool.Exec(ctx, strings.ReplaceAll(gen, "{s}", s.schema)); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name, index, sql string
		args             []any
	}{
		{"claim", "jobs_fetch", s.claimSQL(claimShape{queues: 2}), []any{[]string{"q1", "q2"}, 10, "x"}},
		{"promote", "jobs_due", s.q.promote, []any{100}},
		{"next due", "jobs_due", s.q.nextDue, nil},
		{"leases", "jobs_running", s.q.leases, []any{"srv3"}},
		{"admit", "jobs_throttled", s.q.admit, []any{[]string{"key48", "key98"}, nil}},
		{"throttled keys", "jobs_throttled", s.q.throttledKeys, []any{100}},
		{"reconcile", "jobs_limit", s.q.reconcile, []any{[]string{"key48", "key98"}}},
		{"unused limits", "archive_limit", s.q.unusedLimits, []any{100}},
		{"prune limits", "jobs_limit", s.q.pruneLimits, []any{[]string{"key48", "key98"}}},
		{"stranded parents", "deps_open", s.q.strandedParents, []any{int64(0), 100}},
		{"resolve", "deps_open", s.q.resolve, []any{[]int64{49, 99}}},
		{"prune uniques", "uniques_expires", s.q.pruneUniques, []any{100}},
		{"prune holders", "uniques_pkey", s.q.pruneHolders, []any{[]byte(nil), 100}},
		{"failed page", "jobs_failed", "SELECT id FROM " + s.schema + ".jobs WHERE state = 'failed' ORDER BY finalized_at DESC, id DESC LIMIT 20", nil},
	}
	explain := func(name, sql string, args ...any) string {
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		rows, err := tx.Query(ctx, "EXPLAIN (ANALYZE, BUFFERS) "+sql, args...)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		plan, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		return strings.Join(plan, "\n")
	}
	for _, c := range cases {
		text := explain(c.name, c.sql, c.args...)
		if !strings.Contains(text, c.index) {
			t.Errorf("%s does not use %s:\n%s", c.name, c.index, text)
		}
		if strings.Contains(text, "Seq Scan on jobs") {
			t.Errorf("%s scans jobs sequentially:\n%s", c.name, text)
		}
	}
	if text := explain("counts", s.q.counts); !strings.Contains(text, "jobs_retries") {
		t.Errorf("counts does not use jobs_retries:\n%s", text)
	}
}

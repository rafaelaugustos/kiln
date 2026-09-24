package pgstore

import (
	"context"
	"fmt"
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
		{"admit", "jobs_throttled", s.q.admit, rules{keys: []string{"key48", "key98"}}.args()},
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
	for _, c := range cases {
		text := explain(t, s, c.name, c.sql, c.args...)
		if !strings.Contains(text, c.index) {
			t.Errorf("%s does not use %s:\n%s", c.name, c.index, text)
		}
		if strings.Contains(text, "Seq Scan on jobs") {
			t.Errorf("%s scans jobs sequentially:\n%s", c.name, text)
		}
	}
	if text := explain(t, s, "counts", s.q.counts); !strings.Contains(text, "jobs_retries") {
		t.Errorf("counts does not use jobs_retries:\n%s", text)
	}
}

func explain(t *testing.T, s *Store, name, sql string, args ...any) string {
	t.Helper()
	ctx := context.Background()
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

func TestAdmissionPlans(t *testing.T) {
	t.Parallel()
	s := open(t)
	gen := `
INSERT INTO {s}.limits (key, max, active, rate, per_us, burst, tat)
SELECT 'key' || g, CASE WHEN g % 3 = 0 THEN 0 ELSE 2 END, 0, CASE WHEN g % 3 = 2 THEN 0 ELSE 20 END,
	CASE WHEN g % 3 = 2 THEN 0 ELSE 1000000 END, CASE WHEN g % 3 = 2 THEN 0 ELSE 1 END,
	CASE WHEN g % 3 <> 2 THEN now() + interval '1 second' END
FROM generate_series(0, 2999) g;
INSERT INTO {s}.jobs (id, state, queue, kind, max_attempts, run_at, args, limit_key, granted, claim, attempt, server, attempted_at)
SELECT g, (CASE WHEN g % 10 < 5 THEN 'throttled' WHEN g % 10 < 8 THEN 'scheduled' WHEN g % 10 = 8 THEN 'enqueued'
		ELSE 'processing' END)::{s}.state,
	'q' || (g % 5), 'k', 10, now(), '{}', 'key' || (g % 3000), g % 4 = 0, 1, 1, 'srv', now()
FROM generate_series(1, 60000) g;
ANALYZE {s}.jobs;
ANALYZE {s}.limits;`
	if _, err := s.pool.Exec(context.Background(), strings.ReplaceAll(gen, "{s}", s.schema)); err != nil {
		t.Fatal(err)
	}
	override := rules{
		keys: []string{"key0", "key1", "key2"},
		max:  []int64{0, 3, 2}, rate: []int64{10, 20, 0}, per: []int64{1e6, 1e6, 0}, burst: []int64{2, 1, 0},
	}
	cases := []struct {
		name, sql string
		args      []any
	}{
		{"admit with rules", s.q.admit, override.args()},
		{"admit", s.q.admit, rules{keys: []string{"key3", "key4", "key5"}}.args()},
		{"admit skip", s.q.admitSkip, []any{[]string{"key6", "key7", "key8"}}},
		{"admit finished", s.q.admitFinished, []any{[]int64{9, 19, 6029}}},
	}
	for _, c := range cases {
		text := explain(t, s, c.name, c.sql, c.args...)
		for _, index := range []string{"limits_pkey", "jobs_throttled"} {
			if !strings.Contains(text, index) {
				t.Errorf("%s does not use %s:\n%s", c.name, index, text)
			}
		}
		for _, scan := range []string{"Seq Scan on jobs", "Seq Scan on limits"} {
			if strings.Contains(text, scan) {
				t.Errorf("%s has a %s:\n%s", c.name, scan, text)
			}
		}
	}
	if text := explain(t, s, "promote", s.q.promote, 100); !strings.Contains(text, "jobs_granted") || strings.Contains(text, "Seq Scan on jobs") {
		t.Errorf("promote does not find throttled grants through jobs_granted:\n%s", text)
	}
}

func TestAdmissionLocks(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	const waiting = 500
	cases := []struct {
		name    string
		active  int
		burst   int
		granted bool
		ahead   *int
		moved   int
	}{
		{"granted jobs waiting for a slot", 1, 1, true, new(3600), 1},
		{"rated jobs within the burst", 1, 3, false, nil, 1},
		{"no slot left", 2, 1, true, new(3600), 0},
		{"future slots reserved without a slot", 2, 1, false, new(3600), waiting},
	}
	for i, c := range cases {
		key := fmt.Sprintf("k%d", i)
		setup := `INSERT INTO {s}.limits (key, max, active, rate, per_us, burst, tat)
VALUES ($1, 2, $2, 20, 1000000, $3, now() + $4::int * interval '1 second')`
		if _, err := s.pool.Exec(ctx, strings.ReplaceAll(setup, "{s}", s.schema), key, c.active, c.burst, c.ahead); err != nil {
			t.Fatal(err)
		}
		jobs := `INSERT INTO {s}.jobs (state, queue, kind, max_attempts, run_at, args, limit_key, granted)
SELECT 'throttled', 'default', 'k', 3, now(), '{}', $1, $2 FROM generate_series(1, $3::int)`
		if _, err := s.pool.Exec(ctx, strings.ReplaceAll(jobs, "{s}", s.schema), key, c.granted, waiting); err != nil {
			t.Fatal(err)
		}
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		rows, _ := tx.Query(ctx, s.q.admit, rules{keys: []string{key}}.args()...)
		var (
			queue    string
			n, moved int
		)
		if _, err := pgx.ForEachRow(rows, []any{&queue, &n}, func() error { moved += n; return nil }); err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		var free int
		q := "SELECT count(*) FROM (SELECT 1 FROM " + s.schema + ".jobs WHERE limit_key = $1 AND state = 'throttled' FOR UPDATE SKIP LOCKED) x"
		if err := s.pool.QueryRow(ctx, q, key).Scan(&free); err != nil {
			t.Fatal(err)
		}
		tx.Rollback(ctx)
		if moved != c.moved || free != waiting-c.moved {
			t.Errorf("%s: moved %d and left %d of %d waiting jobs unlocked, want %d moved and the rest unlocked",
				c.name, moved, free, waiting, c.moved)
		}
	}
}

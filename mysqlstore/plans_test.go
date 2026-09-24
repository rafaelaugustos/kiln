package mysqlstore

import (
	"context"
	"database/sql"
	"strings"
	"testing"
)

type step struct {
	table, key, extra string
}

func explain(t *testing.T, s *Store, query string) []step {
	t.Helper()
	rows, err := s.db.QueryContext(context.Background(), "EXPLAIN "+query)
	if err != nil {
		t.Fatalf("explain: %v\n%s", err, query)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	var out []step
	for rows.Next() {
		vals := make([]sql.NullString, len(cols))
		dst := make([]any, len(cols))
		for i := range vals {
			dst[i] = &vals[i]
		}
		if err := rows.Scan(dst...); err != nil {
			t.Fatal(err)
		}
		var st step
		for i, c := range cols {
			switch c {
			case "table":
				st.table = vals[i].String
			case "key":
				st.key = vals[i].String
			case "Extra":
				st.extra = vals[i].String
			}
		}
		out = append(out, st)
	}
	return out
}

func TestPlansUseIndexes(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	gen := []string{
		`SET SESSION cte_max_recursion_depth = 100000`,
		`INSERT INTO kiln_jobs (id, state, queue, kind, priority, max_attempts, run_at, created_at, args, server,
	attempted_at, limit_key, claim, attempt, finalized_at, deps_pending)
WITH RECURSIVE g(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM g WHERE n < 60000)
SELECT n, ELT(1 + (n % 50 >= 20) + (n % 50 >= 45) + (n % 50 >= 47) + (n % 50 >= 48) + (n % 50 >= 49),
		'enqueued', 'scheduled', 'awaiting', 'processing', 'throttled', 'failed'),
	CONCAT('q', n % 5), CONCAT('k', n % 20), n % 3, 10, UTC_TIMESTAMP(6) + INTERVAL (n % 86400) SECOND,
	UTC_TIMESTAMP(6), '{}', CONCAT('srv', n % 20), UTC_TIMESTAMP(6), IF(n % 50 = 48, CONCAT('key', n % 50), NULL),
	1, 1, IF(n % 50 = 49, UTC_TIMESTAMP(6), NULL), IF(n % 50 BETWEEN 45 AND 46, 1, 0)
FROM g`,
		`INSERT INTO kiln_deps (batch, parent_id, job_id, mask)
WITH RECURSIVE g(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM g WHERE n < 40000)
SELECT FALSE, n % 20000 + 1, 100000 + n, 1 FROM g`,
		`INSERT INTO kiln_uniques (unique_key, job_id, expires_at)
WITH RECURSIVE g(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM g WHERE n < 2000)
SELECT UNHEX(SHA2(n, 256)), n, IF(n % 2 = 0, UTC_TIMESTAMP(6) + INTERVAL n SECOND, NULL) FROM g`,
		`INSERT INTO kiln_stats (bucket, server, succeeded)
WITH RECURSIVE g(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM g WHERE n < 2000)
SELECT UTC_TIMESTAMP() - INTERVAL n MINUTE, CONCAT('srv', n % 7), n FROM g`,
		`INSERT INTO kiln_batches (description, created_at, sealed, finished_at)
WITH RECURSIVE g(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM g WHERE n < 2000)
SELECT '', UTC_TIMESTAMP(6), TRUE, IF(n % 10 = 0, NULL, UTC_TIMESTAMP(6)) FROM g`,
		`ANALYZE TABLE kiln_jobs, kiln_deps, kiln_uniques, kiln_stats, kiln_batches`,
	}
	for _, q := range gen {
		if _, err := s.db.ExecContext(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	cases := []struct {
		name, key, sql string
	}{
		{"claim", "jobs_fetch", render(s.q.claim, "q1", "q1", 10)},
		{"claim kinds", "jobs_fetch", render(s.q.claimKinds, "q1", "q1", []string{"k1", "k2"}, 10)},
		{"promote", "jobs_due", render(s.q.due, 100)},
		{"leases", "jobs_running", render(s.q.directives, "srv3")},
		{"admit", "jobs_throttled", render(s.q.waiting, "key48", 5)},
		{"throttled keys", "jobs_throttled", render(s.q.throttledKeys, 100)},
		{"stuck", "jobs_state", render(s.q.awaiting, 0, 100)},
		{"delete by state", "jobs_state", render(s.q.lockTargets, raw("jobs_state"), raw("j.state = 'enqueued'"), 0)},
		{"requeue failed", "jobs_state", render(s.q.lockFailed, raw("jobs_state"), raw("j.state = 'failed'"), 0)},
		{"prune failed", "jobs_failed", render(s.q.expiredFailed, 1000, 100)},
		{"finish", "PRIMARY", render(s.q.lockRunning, []int64{1047, 2047, 3047})},
		{"children", "PRIMARY", render(s.q.lockChildren, []int64{1045, 1046})},
		{"resolve", "PRIMARY", render(s.q.openDeps, []int64{49, 99}, raw(""))},
		{"reconcile", "jobs_throttled", render(s.q.active, []string{"key48"})},
		{"failed page", "jobs_failed", "SELECT id FROM kiln_jobs j WHERE j.state = 'failed' ORDER BY j.finalized_at DESC, j.id DESC LIMIT 21"},
		{"key state", "PRIMARY", render(s.q.keyState, [][]byte{{1}, {2}})},
		{"expired keys", "uniques_expires", render(s.q.expiredUniques, 100)},
		{"prune keys", "PRIMARY", render(s.q.pruneUniques, [][]byte{{1}, {2}})},
		{"prune holders", "PRIMARY", render(s.q.dropHolders, [][]byte{{1}, {2}})},
		{"prune stats", "PRIMARY", s.q.dropStats + string(appendSQL(nil, "(?, ?), (?, ?))", totals, "a", totals, "wörker"))},
		{"prune batches", "batches_open", render(s.q.doneBatches, 100)},
	}
	for _, c := range cases {
		plan := explain(t, s, c.sql)
		if plan[0].key != c.key {
			t.Errorf("%s uses %q, want %s: %+v", c.name, plan[0].key, c.key, plan)
		}
		for _, st := range plan {
			if st.table == "kiln_jobs" && strings.Contains(st.extra, "filesort") {
				t.Errorf("%s sorts jobs rows: %+v", c.name, plan)
			}
		}
	}
	for _, q := range []string{s.q.counts, s.q.nextDue, render(s.q.orphans, 1000000, 100)} {
		for _, st := range explain(t, s, q) {
			if st.table == "kiln_jobs" && st.key == "" {
				t.Errorf("full scan of jobs: %+v\n%s", st, q)
			}
		}
	}
}

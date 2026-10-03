package mssqlstore

import (
	"context"
	"database/sql"
	"encoding/xml"
	"strings"
	"testing"
)

type access struct {
	op, table, index string
}

func showplan(t *testing.T, conn *sql.Conn, query string) []access {
	t.Helper()
	rows, err := conn.QueryContext(context.Background(), query)
	if err != nil {
		t.Fatalf("showplan: %v\n%s", err, query)
	}
	defer rows.Close()
	var out []access
	for {
		for rows.Next() {
			var plan string
			if err := rows.Scan(&plan); err != nil {
				t.Fatal(err)
			}
			out = append(out, accesses(t, plan)...)
		}
		if !rows.NextResultSet() {
			break
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func accesses(t *testing.T, plan string) []access {
	t.Helper()
	d := xml.NewDecoder(strings.NewReader(plan))
	var (
		ops []string
		out []access
	)
	for {
		tok, err := d.Token()
		if err != nil {
			return out
		}
		switch e := tok.(type) {
		case xml.StartElement:
			attr := func(name string) string {
				for _, a := range e.Attr {
					if a.Name.Local == name {
						return strings.Trim(a.Value, "[]")
					}
				}
				return ""
			}
			switch e.Name.Local {
			case "RelOp":
				ops = append(ops, attr("PhysicalOp"))
			case "Object":
				if len(ops) > 0 && attr("Table") != "" {
					out = append(out, access{op: ops[len(ops)-1], table: attr("Table"), index: attr("Index")})
				}
			}
		case xml.EndElement:
			if e.Name.Local == "RelOp" && len(ops) > 0 {
				ops = ops[:len(ops)-1]
			}
		}
	}
}

func TestPlansUseIndexes(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	p := s.prefix
	gen := []string{
		`WITH g AS (SELECT TOP (60000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n FROM sys.all_columns a CROSS JOIN sys.all_columns b)
INSERT INTO ` + p + `jobs (id, state, queue, kind, priority, max_attempts, run_at, created_at, args, server, attempted_at,
	limit_key, claim, attempt, finalized_at, deps_pending, granted)
SELECT n, CASE WHEN n % 50 < 20 THEN 'enqueued' WHEN n % 50 < 45 THEN 'scheduled' WHEN n % 50 < 47 THEN 'awaiting'
		WHEN n % 50 = 47 THEN 'processing' WHEN n % 50 = 48 THEN 'throttled' ELSE 'failed' END,
	CONCAT(N'q', n % 5), CONCAT(N'k', n % 20), n % 3, 10, DATEADD(SECOND, n % 86400, SYSUTCDATETIME()), SYSUTCDATETIME(),
	N'{}', CONCAT(N'srv', n % 20), SYSUTCDATETIME(), CASE WHEN n % 50 = 48 THEN CONCAT(N'key', n % 1000) END, 1, 1,
	CASE WHEN n % 50 = 49 THEN SYSUTCDATETIME() END, CASE WHEN n % 50 IN (45, 46) THEN 1 ELSE 0 END, n % 100 / 99
FROM g`,
		`WITH g AS (SELECT TOP (40000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n FROM sys.all_columns a CROSS JOIN sys.all_columns b)
INSERT INTO ` + p + `deps (batch, parent_id, job_id, mask) SELECT 0, n % 20000 + 1, 100000 + n, 1 FROM g`,
		`WITH g AS (SELECT TOP (2000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n FROM sys.all_columns a CROSS JOIN sys.all_columns b)
INSERT INTO ` + p + `limits (limit_key, [max], rate, per_us, burst) SELECT CONCAT(N'key', n), n % 3, n % 2 * 10, 1000000, 1 FROM g`,
		`UPDATE STATISTICS ` + p + `jobs WITH FULLSCAN`,
		`UPDATE STATISTICS ` + p + `deps WITH FULLSCAN`,
		`UPDATE STATISTICS ` + p + `limits WITH FULLSCAN`,
	}
	for _, q := range gen {
		if _, err := s.db.ExecContext(ctx, q); err != nil {
			t.Fatalf("%v\n%s", err, q)
		}
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "SET SHOWPLAN_XML ON"); err != nil {
		t.Fatal(err)
	}
	defer conn.ExecContext(ctx, "SET SHOWPLAN_XML OFF")
	ids := `DECLARE @ids NVARCHAR(MAX) = N'[1047, 2047, 3047]';`
	queues := `DECLARE @n INT = 10, @queues NVARCHAR(MAX) = N'[{"q":"q1","o":0}]', @server NVARCHAR(255) = N'srv', `
	cases := []struct {
		name, table, index, vars, sql string
	}{
		{"claim", "jobs", "jobs_fetch", queues + `@kinds NVARCHAR(MAX) = NULL;`, s.q.claim},
		{"claim kinds", "jobs", "jobs_fetch", queues + `@kinds NVARCHAR(MAX) = N'["k1","k2"]';`, s.q.claim},
		{"promote", "jobs", "jobs_due", `DECLARE @n INT = 100;`, s.q.promote},
		{"leases", "jobs", "jobs_running", `DECLARE @id NVARCHAR(255) = N'srv3';`, s.q.directives},
		{"admit", "jobs", "jobs_throttled", `DECLARE @walk NVARCHAR(MAX) = N'[{"k":"key48","n":5}]';`, s.q.waiting},
		{"throttled keys", "jobs", "jobs_throttled", `DECLARE @n INT = 100;`, s.q.throttledKeys},
		{"stuck", "jobs", "jobs_state", `DECLARE @n INT = 100, @after BIGINT = 0;`, s.q.awaiting},
		{"prune failed", "jobs", "jobs_failed", `DECLARE @n INT = 100, @us BIGINT = 1000000;`, s.q.expiredFailed},
		{"finish", "jobs", "", ids, s.q.lockRunning},
		{"children", "jobs", "", ids, s.q.lockChildren},
		{"busy", "jobs", "", ids, s.q.busy},
		{"lock limits", "limits", "", `DECLARE @keys NVARCHAR(MAX) = N'["key48","key7"]';`, s.q.lockLimits},
		{"resolve", "deps", "", `DECLARE @batch BIT = 0, @parents NVARCHAR(MAX) = N'[{"id":49},{"id":99}]';`, s.q.resolve},
		{"reconcile", "jobs", "", `DECLARE @keys NVARCHAR(MAX) = N'["key48"]';`, s.q.active},
		{"counts", "jobs", "", "", s.q.counts},
		{"next due", "jobs", "jobs_due", "", s.q.nextDue},
		{"pending", "jobs", "", "", s.q.pending},
		{"orphans", "jobs", "", `DECLARE @n INT = 100, @us BIGINT = 1000000;`, s.q.orphans},
	}
	for _, c := range cases {
		plan := showplan(t, conn, c.vars+"\n"+c.sql)
		table := p + c.table
		seek := false
		for _, a := range plan {
			if a.table != table {
				continue
			}
			switch a.op {
			case "Table Scan", "Index Scan", "Clustered Index Scan":
				t.Errorf("%s scans %s through %s: %+v", c.name, table, a.index, plan)
			case "Index Seek", "Clustered Index Seek":
				seek = seek || c.index == "" || a.index == c.index
			}
		}
		if !seek {
			t.Errorf("%s does not seek %s through %q: %+v", c.name, table, c.index, plan)
		}
	}
}

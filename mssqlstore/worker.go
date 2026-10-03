package mssqlstore

import (
	"context"
	"database/sql"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

const sqlHeartbeat = `MERGE {p}servers WITH (HOLDLOCK) AS s
USING (SELECT @id AS id, SYSUTCDATETIME() AS now) AS n ON s.id = n.id COLLATE Latin1_General_100_BIN2
WHEN MATCHED THEN UPDATE SET host = @host, pid = @pid, version = @version, queues = @queues, kinds = @kinds,
	workers = @workers, running = @running, started_at = COALESCE(@started, s.started_at), heartbeat_at = n.now
WHEN NOT MATCHED THEN INSERT (id, host, pid, version, queues, kinds, workers, running, started_at, heartbeat_at)
	VALUES (n.id, @host, @pid, @version, @queues, @kinds, @workers, @running, COALESCE(@started, n.now), n.now);`

const sqlDirectives = `SELECT id, claim, cancel_requested, DATEDIFF_BIG(MICROSECOND, attempted_at, SYSUTCDATETIME()), NULL
FROM {p}jobs WITH (INDEX(jobs_running)) WHERE state = 'processing' AND server = @id
UNION ALL
SELECT 0, 0, CAST(0 AS BIT), 0, name FROM {p}queues WHERE paused = 1`

const sqlUnregister = `DELETE FROM {p}servers WHERE id = @id`

const sqlSetMeta = `UPDATE {p}jobs SET meta = `

const setMetaTail = ` WHERE id = @id AND claim = @claim AND state = 'processing'`

// Heartbeat records that the server si describes is alive and returns the leases of its
// processing jobs and the paused queues; see [driver.Worker.Heartbeat].
func (s *Store) Heartbeat(ctx context.Context, si driver.ServerInfo) (driver.Directives, error) {
	_, err := s.db.ExecContext(ctx, s.q.heartbeat, sql.Named("id", si.ID), sql.Named("host", si.Host),
		sql.Named("pid", si.PID), sql.Named("version", si.Version), sql.Named("queues", text(encodeStrings(si.Queues))),
		sql.Named("kinds", text(encodeStrings(si.Kinds))), sql.Named("workers", si.Workers),
		sql.Named("running", si.Running), sql.Named("started", stamp(si.StartedAt)))
	if err != nil {
		return driver.Directives{}, wrap("heartbeat", err)
	}
	rows, err := s.db.QueryContext(ctx, s.q.directives, sql.Named("id", si.ID))
	if err != nil {
		return driver.Directives{}, wrap("heartbeat", err)
	}
	defer rows.Close()
	var d driver.Directives
	for rows.Next() {
		var (
			l     driver.Lease
			age   int64
			queue sql.NullString
		)
		if err := rows.Scan(&l.ID, &l.Claim, &l.Cancel, &age, &queue); err != nil {
			return driver.Directives{}, wrap("heartbeat", err)
		}
		if queue.Valid {
			d.Paused = append(d.Paused, queue.String)
			continue
		}
		l.Age = time.Duration(age) * time.Microsecond
		d.Leases = append(d.Leases, l)
	}
	if err := rows.Err(); err != nil {
		return driver.Directives{}, wrap("heartbeat", err)
	}
	slices.Sort(d.Paused)
	return d, nil
}

func text(b []byte) any {
	if b == nil {
		return nil
	}
	return string(b)
}

// Unregister deletes the server's row. Jobs it still has processing become orphans at once.
func (s *Store) Unregister(ctx context.Context, server string) error {
	if _, err := s.db.ExecContext(ctx, s.q.unregister, sql.Named("id", server)); err != nil {
		return wrap("unregister", err)
	}
	return nil
}

// SetMeta merges meta into the job's metadata with JSON_MODIFY while the job is processing under
// ref's claim, and fails with [driver.ErrLost] otherwise.
func (s *Store) SetMeta(ctx context.Context, ref driver.Ref, meta map[string]string) error {
	expr := "COALESCE(meta, N'{}')"
	args := []any{sql.Named("id", ref.ID), sql.Named("claim", ref.Claim)}
	for i, k := range slices.Sorted(maps.Keys(meta)) {
		n := strconv.Itoa(i)
		expr = "JSON_MODIFY(" + expr + ", @k" + n + ", @v" + n + ")"
		path := append([]byte("$."), appendJSONString(nil, k)...)
		args = append(args, sql.Named("k"+n, string(path)), sql.Named("v"+n, meta[k]))
	}
	r, err := s.db.ExecContext(ctx, s.q.setMeta+expr+setMetaTail, args...)
	if err != nil {
		return wrap("set meta", err)
	}
	if n, err := r.RowsAffected(); err != nil || n == 0 {
		return fmt.Errorf("%w: job %d claim %d", driver.ErrLost, ref.ID, ref.Claim)
	}
	return nil
}

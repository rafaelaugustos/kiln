package mssqlstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"slices"

	"github.com/rafaelaugustos/kiln/driver"
)

const sqlLockRunning = `SELECT CAST(SYSUTCDATETIME() AS DATETIME2(6)), j.id, j.claim, j.attempt, j.queue,
	COALESCE(j.server, N''), COALESCE(j.limit_key, N''), COALESCE(j.batch_id, 0), j.cancel_requested, j.unique_key,
	CASE WHEN EXISTS (SELECT 1 FROM {p}deps d WHERE d.batch = 0 AND d.parent_id = j.id AND d.resolved = 0) THEN 1 ELSE 0 END
FROM OPENJSON(@ids) WITH (id BIGINT '$') v
JOIN {p}jobs j WITH (XLOCK, READPAST, ROWLOCK, FORCESEEK) ON j.id = v.id
WHERE j.state = 'processing'
ORDER BY j.id`

const sqlUpdateLive = `UPDATE j SET state = v.state, attempt = v.attempt,
	run_at = CASE WHEN v.state = 'failed' THEN j.run_at
		ELSE DATEADD(MICROSECOND, ISNULL(v.delay, 0) % 1000000, DATEADD(SECOND, ISNULL(v.delay, 0) / 1000000, @now)) END,
	finalized_at = CASE WHEN v.state = 'failed' THEN @now END, granted = 0, history = ` + pushHistory + `
FROM OPENJSON(@live) WITH (id BIGINT '$.id', state VARCHAR(10) '$.s', attempt INT '$.a', delay BIGINT '$.d',
	entry NVARCHAR(MAX) '$.e' AS JSON) v
JOIN {p}jobs j WITH (FORCESEEK) ON j.id = v.id;
`

const sqlBusy = `SELECT j.id, j.claim FROM OPENJSON(@ids) WITH (id BIGINT '$') v
JOIN {p}jobs j ON j.id = v.id
WHERE j.state = 'processing'`

type running struct {
	id       int64
	claim    int32
	attempt  int
	queue    string
	server   string
	limit    string
	batch    int64
	cancel   bool
	key      []byte
	children bool
}

// Finish applies outcomes in one transaction, as [driver.Worker.Finish] describes. An outcome
// whose job row another transaction has locked is [driver.Busy] while the job is still processing
// under its claim. An outcome with a State Finish cannot apply, or with an Output that is neither
// empty nor valid JSON, is [driver.Rejected] at once. When SQL Server refuses a value, Finish
// retries the outcomes in halves, each in a transaction of its own, until every outcome at fault
// is alone, and reports those [driver.Rejected].
func (s *Store) Finish(ctx context.Context, server string, outs []driver.Outcome) ([]driver.Result, error) {
	res := make([]driver.Result, len(outs))
	idx := make([]int, 0, len(outs))
	for i := range outs {
		switch o := &outs[i]; o.State {
		case driver.Succeeded, driver.Failed, driver.Deleted, driver.Scheduled, driver.Enqueued:
			if len(o.Output) == 0 || json.Valid(o.Output) {
				idx = append(idx, i)
				continue
			}
		}
		res[i] = driver.Rejected
	}
	if err := s.finish(ctx, server, outs, idx, res); err != nil {
		return nil, fmt.Errorf("kiln: finish: %w", err)
	}
	return res, nil
}

func (s *Store) finish(ctx context.Context, server string, outs []driver.Outcome, idx []int, res []driver.Result) error {
	if len(idx) == 0 {
		return nil
	}
	err := s.finishOnce(ctx, server, outs, idx, res)
	if err == nil || !dataError(err) {
		return err
	}
	if len(idx) == 1 {
		res[idx[0]] = driver.Rejected
		return nil
	}
	h := len(idx) / 2
	if err := s.finish(ctx, server, outs, idx[:h], res); err != nil {
		return err
	}
	return s.finish(ctx, server, outs, idx[h:], res)
}

func (s *Store) finishOnce(ctx context.Context, server string, outs []driver.Outcome, idx []int, res []driver.Result) error {
	ids := make([]int64, 0, len(idx))
	for _, i := range idx {
		ids = append(ids, outs[i].ID)
	}
	slices.Sort(ids)
	ids = slices.Compact(ids)
	var (
		locked  map[int64]*running
		applied map[int64]int
		queues  []string
	)
	err := s.txn(ctx, func(tx *sql.Tx) error {
		applied, queues = nil, nil
		f := &fallout{server: server}
		var err error
		if locked, err = s.lockRunning(ctx, tx, ids, f); err != nil || len(locked) == 0 {
			return err
		}
		applied = make(map[int64]int, len(locked))
		for _, i := range idx {
			o := &outs[i]
			if r := locked[o.ID]; r != nil && r.claim == o.Claim {
				if _, dup := applied[o.ID]; !dup {
					applied[o.ID] = i
				}
			}
		}
		if err := s.apply(ctx, tx, f, outs, applied, locked); err != nil {
			return err
		}
		queues = f.queues
		return nil
	})
	if err != nil {
		return err
	}
	s.nt.ready(queues)
	var missing []int64
	for _, id := range ids {
		if locked[id] == nil {
			missing = append(missing, id)
		}
	}
	busy, err := s.busy(ctx, missing)
	if err != nil {
		return err
	}
	for _, i := range idx {
		o := &outs[i]
		j, done := applied[o.ID]
		c, held := busy[o.ID]
		switch {
		case done && j == i:
			res[i] = driver.Applied
		case held && c == o.Claim:
			res[i] = driver.Busy
		default:
			res[i] = driver.Stale
		}
	}
	return nil
}

func (s *Store) lockRunning(ctx context.Context, tx *sql.Tx, ids []int64, f *fallout) (map[int64]*running, error) {
	rows, err := tx.QueryContext(ctx, s.q.lockRunning, sql.Named("ids", idList(ids)))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	locked := make(map[int64]*running, len(ids))
	for rows.Next() {
		var now moment
		r := &running{}
		err := rows.Scan(&now, &r.id, &r.claim, &r.attempt, &r.queue, &r.server, &r.limit, &r.batch, &r.cancel, &r.key, &r.children)
		if err != nil {
			return nil, err
		}
		f.now = now.Time
		locked[r.id] = r
	}
	return locked, rows.Err()
}

func (s *Store) busy(ctx context.Context, ids []int64) (map[int64]int32, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	rows, err := s.db.QueryContext(ctx, s.q.busy, sql.Named("ids", idList(ids)))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	busy := make(map[int64]int32, len(ids))
	for rows.Next() {
		var (
			id    int64
			claim int32
		)
		if err := rows.Scan(&id, &claim); err != nil {
			return nil, err
		}
		busy[id] = claim
	}
	return busy, rows.Err()
}

func (s *Store) apply(ctx context.Context, q querier, f *fallout, outs []driver.Outcome, applied map[int64]int, locked map[int64]*running) error {
	order := make([]int64, 0, len(applied))
	for id := range applied {
		order = append(order, id)
	}
	slices.Sort(order)
	var gone, live table
	for _, id := range order {
		o, r := &outs[applied[id]], locked[id]
		attempt := r.attempt
		if o.Refund {
			attempt = max(attempt-1, 0)
		}
		want := o.State
		canceled := r.cancel && want != driver.Succeeded && want != driver.Deleted
		delay := max(micros(o.Delay), 0)
		var fin driver.State
		switch {
		case r.cancel && want != driver.Succeeded:
			fin = driver.Deleted
		case want == driver.Scheduled && delay > 0:
			fin = driver.Scheduled
		case (want == driver.Scheduled || want == driver.Enqueued) && r.limit != "":
			fin = driver.Throttled
		case want == driver.Scheduled:
			fin = driver.Enqueued
		default:
			fin = want
		}
		e := entry{state: fin, attempt: attempt, reason: clean(o.Reason, 256), server: r.server}
		if canceled {
			e.reason = "canceled"
		}
		if e.reason != "" {
			e.err, e.trace = clean(o.Error, 2<<10), clean(o.Trace, 8<<10)
		}
		switch fin {
		case driver.Succeeded:
			f.stats.succeeded++
		case driver.Failed:
			f.stats.failed++
		case driver.Deleted:
			f.stats.deleted++
		case driver.Enqueued:
			f.queue(r.queue)
		}
		if want == driver.Scheduled && !o.Refund && !canceled {
			f.stats.retried++
		}
		final := fin == driver.Succeeded || fin == driver.Failed || fin == driver.Deleted
		if final {
			f.hold(r.key, id)
		}
		if final && r.children {
			f.parents = append(f.parents, parent{id: id, state: fin, label: string(fin)})
		}
		if r.limit != "" {
			f.free(r.limit)
		}
		if fin == driver.Succeeded || fin == driver.Deleted {
			gone.row()
			gone.int("id", id)
			gone.str("s", string(fin))
			gone.int("a", int64(attempt))
			gone.json("e", e.encode(f.now))
			if len(o.Output) > 0 {
				gone.text("o", o.Output)
			}
			f.batch(r.batch)
			continue
		}
		live.row()
		live.int("id", id)
		live.str("s", string(fin))
		live.int("a", int64(attempt))
		if fin == driver.Scheduled {
			live.int("d", delay)
		}
		live.json("e", e.encode(f.now))
	}
	var stmt string
	if gone.n > 0 {
		stmt = s.q.archive
	}
	if live.n > 0 {
		stmt += s.q.updateLive
	}
	if stmt != "" {
		_, err := q.ExecContext(ctx, stmt, sql.Named("now", stamp(f.now)), sql.Named("gone", gone.String()),
			sql.Named("live", live.String()))
		if err != nil {
			return err
		}
	}
	return s.settle(ctx, q, f)
}

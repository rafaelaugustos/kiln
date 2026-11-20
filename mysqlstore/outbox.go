package mysqlstore

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/rafaelaugustos/kiln/driver"
	"github.com/rafaelaugustos/kiln/internal/outbox"
)

var _ driver.Outbox = (*Outbox)(nil)

const sqlOutboxTable = `CREATE TABLE IF NOT EXISTS {p}outbox (
	seq BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
	id VARCHAR(191) CHARACTER SET ascii COLLATE ascii_bin NOT NULL UNIQUE,
	data LONGBLOB NOT NULL,
	created_at DATETIME(6) NOT NULL DEFAULT (NOW(6)),
	failed TEXT
)`

const sqlOutboxPut = `INSERT INTO {p}outbox (id, data) VALUES (?, ?) AS new
ON DUPLICATE KEY UPDATE data = new.data`

const sqlOutboxOne = `SELECT data FROM {p}outbox WHERE id = ? AND failed IS NULL FOR UPDATE SKIP LOCKED`

const sqlOutboxNext = `SELECT id, data FROM {p}outbox
WHERE failed IS NULL AND created_at < NOW(6) - INTERVAL 1 SECOND
ORDER BY seq LIMIT ? FOR UPDATE SKIP LOCKED`

const sqlOutboxDone = `DELETE FROM {p}outbox WHERE id = ?`

const sqlOutboxFail = `UPDATE {p}outbox SET failed = ? WHERE id = ?`

// Outbox is a [driver.Outbox] in MySQL, for applications that keep their jobs in another store,
// such as redisstore, and still want to enqueue inside their database transactions. It is one
// small table, kiln_outbox unless [Prefix] names another prefix: a row holds the writes of one
// transaction from its commit until the store has applied them, which normally takes as long as
// the call to Notify.
//
//	store, err := redisstore.New(ctx, rdb)
//	ob, err := mysqlstore.NewOutbox(ctx, db, store)
//	server, err := kiln.NewServer(kiln.NewClient(store), mux, kiln.ServerConfig{Outbox: ob})
//
//	tx, err := db.BeginTx(ctx, nil)
//	w := ob.Tx(tx)
//	_, err = client.EnqueueTx(ctx, w, SendEmail{To: "a@b.c"})
//	err = tx.Commit()
//	w.Notify(ctx)
//
// A row whose writes can never be applied keeps the reason in its failed column and stays in the
// table, where nothing touches it again.
type Outbox struct {
	db     *sql.DB
	target driver.Stager
	q      outboxSQL
}

type outboxSQL struct {
	put, one, next, done, fail string
}

// NewOutbox returns an outbox in db whose entries are staged for target and applied to it. It
// creates the table when it is missing, unless [NoMigrate] is given. Of the options, [Prefix] and
// [NoMigrate] apply.
func NewOutbox(ctx context.Context, db *sql.DB, target driver.Stager, opts ...Option) (*Outbox, error) {
	c := newConfig(opts)
	if !validPrefix(c.prefix) {
		return nil, fmt.Errorf("%w: prefix %q", driver.ErrInvalid, c.prefix)
	}
	if target == nil {
		return nil, fmt.Errorf("%w: outbox without a store to stage for", driver.ErrInvalid)
	}
	r := strings.NewReplacer("{p}", c.prefix)
	o := &Outbox{db: db, target: target, q: outboxSQL{
		put:  r.Replace(sqlOutboxPut),
		one:  r.Replace(sqlOutboxOne),
		next: r.Replace(sqlOutboxNext),
		done: r.Replace(sqlOutboxDone),
		fail: r.Replace(sqlOutboxFail),
	}}
	if c.noMigrate {
		return o, nil
	}
	if _, err := db.ExecContext(ctx, r.Replace(sqlOutboxTable)); err != nil {
		return nil, fmt.Errorf("kiln: outbox table: %w", err)
	}
	return o, nil
}

// Tx returns a writer bound to tx: its jobs and batches are staged in the outbox's store and
// stored in tx, so they commit or roll back with the application's work, and its Notify applies
// them after the commit. Each write is a call to the store and a statement in tx; a store that
// cannot be reached fails the write. A nil tx gives a writer whose writes fail with
// [driver.ErrNilTx].
func (o *Outbox) Tx(tx *sql.Tx) driver.TxWriter {
	if tx == nil {
		return outbox.Broken(driver.ErrNilTx)
	}
	return outbox.NewWriter(o.target, &outboxTx{o: o, tx: tx})
}

type outboxTx struct {
	o  *Outbox
	tx *sql.Tx
}

func (t *outboxTx) Put(ctx context.Context, id string, data []byte) error {
	if _, err := t.tx.ExecContext(ctx, t.o.q.put, id, data); err != nil {
		return fmt.Errorf("kiln: outbox: %w", err)
	}
	return nil
}

func (t *outboxTx) Deliver(ctx context.Context, id string) error {
	o := t.o
	var applyErr error
	err := o.inTx(ctx, func(tx *sql.Tx) error {
		var data []byte
		switch err := tx.QueryRowContext(ctx, o.q.one, id).Scan(&data); {
		case errors.Is(err, sql.ErrNoRows):
			return nil
		case err != nil:
			return err
		}
		applyErr = o.target.Apply(ctx, id, data)
		switch {
		case applyErr == nil:
			_, err := tx.ExecContext(ctx, o.q.done, id)
			return err
		case outbox.Permanent(applyErr):
			_, err := tx.ExecContext(ctx, o.q.fail, applyErr.Error(), id)
			return err
		}
		return applyErr
	})
	if applyErr != nil {
		return applyErr
	}
	if err != nil {
		return fmt.Errorf("kiln: outbox: %w", err)
	}
	return nil
}

func (o *Outbox) inTx(ctx context.Context, fn func(tx *sql.Tx) error) error {
	tx, err := o.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

// Drain applies the writes of up to limit committed entries, oldest first, as [driver.Outbox]
// describes, in one transaction that locks the entries it takes and skips those another Drain or
// a Notify holds. Its error is the first one an entry met; the other entries are applied all the
// same.
func (o *Outbox) Drain(ctx context.Context, limit int) (int, error) {
	type entry struct {
		id   string
		data []byte
	}
	n := 0
	var first error
	err := o.inTx(ctx, func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, o.q.next, max(limit, 1))
		if err != nil {
			return err
		}
		var entries []entry
		for rows.Next() {
			var e entry
			if err := rows.Scan(&e.id, &e.data); err != nil {
				rows.Close()
				return err
			}
			entries = append(entries, e)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		done := 0
		for _, e := range entries {
			err := o.target.Apply(ctx, e.id, e.data)
			switch {
			case err == nil:
				if _, err := tx.ExecContext(ctx, o.q.done, e.id); err != nil {
					return err
				}
				done++
			case outbox.Permanent(err):
				if _, ferr := tx.ExecContext(ctx, o.q.fail, err.Error(), e.id); ferr != nil {
					return ferr
				}
				first = cmp.Or(first, fmt.Errorf("kiln: outbox entry %s set aside: %w", e.id, err))
			default:
				first = cmp.Or(first, err)
			}
		}
		n = done
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("kiln: outbox: %w", err)
	}
	return n, first
}

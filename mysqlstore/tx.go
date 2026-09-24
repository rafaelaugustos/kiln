package mysqlstore

import (
	"context"
	"database/sql"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

const tries = 5

var readCommitted = &sql.TxOptions{Isolation: sql.LevelReadCommitted}

type querier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func (s *Store) txn(ctx context.Context, fn func(tx *sql.Tx) error) error {
	for try := 1; ; try++ {
		err := s.once(ctx, fn)
		if err == nil || try == tries || !retryable(err) {
			return err
		}
		pause := time.Duration(try*try)*time.Millisecond + rand.N(2*time.Millisecond)
		t := time.NewTimer(pause)
		select {
		case <-ctx.Done():
			t.Stop()
			return err
		case <-t.C:
		}
	}
}

func (s *Store) once(ctx context.Context, fn func(tx *sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, readCommitted)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

type TxWriter struct {
	s      *Store
	tx     *sql.Tx
	limits map[string]int
	lost   error
}

func (w *TxWriter) Insert(ctx context.Context, jobs []driver.InsertParams) ([]driver.Inserted, error) {
	var (
		res    []driver.Inserted
		limits map[string]int
	)
	err := w.atomic(ctx, func() (err error) {
		res, limits, err = w.s.insert(ctx, w.tx, jobs)
		return err
	})
	if err != nil {
		return nil, err
	}
	w.remember(limits)
	return res, nil
}

func (w *TxWriter) OpenBatch(ctx context.Context, nb driver.NewBatch) (int64, error) {
	var id int64
	err := w.atomic(ctx, func() (err error) {
		id, err = w.s.openBatch(ctx, w.tx, nb)
		return err
	})
	return id, err
}

func (w *TxWriter) SealBatch(ctx context.Context, id int64) error {
	return w.atomic(ctx, func() error { return w.s.sealBatch(ctx, w.tx, id) })
}

func (w *TxWriter) atomic(ctx context.Context, fn func() error) error {
	switch {
	case w.tx == nil:
		return driver.ErrNilTx
	case w.lost != nil:
		return w.lost
	}
	if _, err := w.tx.ExecContext(ctx, "SAVEPOINT kiln"); err != nil {
		return fmt.Errorf("kiln: savepoint: %w", err)
	}
	err := fn()
	if err == nil {
		return nil
	}
	if _, rerr := w.tx.ExecContext(context.WithoutCancel(ctx), "ROLLBACK TO SAVEPOINT kiln"); rerr != nil {
		w.lost = fmt.Errorf("kiln: the transaction was rolled back: %w", err)
		return w.lost
	}
	return err
}

func (s *Store) InTx(ctx context.Context, fn func(w driver.Writer) error) error {
	var w *TxWriter
	err := s.txn(ctx, func(tx *sql.Tx) error {
		w = &TxWriter{s: s, tx: tx}
		return fn(w)
	})
	if err != nil {
		return err
	}
	if len(w.limits) > 0 {
		s.admitKeys(ctx, w.limits)
	}
	return nil
}

func (w *TxWriter) remember(limits map[string]int) {
	if len(limits) == 0 {
		return
	}
	if w.limits == nil {
		w.limits = make(map[string]int, len(limits))
	}
	maps.Copy(w.limits, limits)
}

func merge(dst, src []string) []string {
	for _, k := range src {
		if !slices.Contains(dst, k) {
			dst = append(dst, k)
		}
	}
	return dst
}

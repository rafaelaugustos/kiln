package pgstore

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/rafaelaugustos/kiln/driver"
)

type TxWriter struct {
	s  *Store
	tx pgx.Tx
	w  wake
}

func (w *TxWriter) Insert(ctx context.Context, jobs []driver.InsertParams) ([]driver.Inserted, error) {
	if w.tx == nil {
		return nil, driver.ErrNilTx
	}
	res, wk, err := w.s.insert(ctx, w.tx, jobs)
	if err != nil {
		return nil, err
	}
	w.w.merge(wk)
	return res, nil
}

func (w *TxWriter) OpenBatch(ctx context.Context, nb driver.NewBatch) (int64, error) {
	if w.tx == nil {
		return 0, driver.ErrNilTx
	}
	return w.s.openBatch(ctx, w.tx, nb)
}

func (w *TxWriter) SealBatch(ctx context.Context, id int64) error {
	if w.tx == nil {
		return driver.ErrNilTx
	}
	wk, err := w.s.sealBatch(ctx, w.tx, id)
	if err != nil {
		return err
	}
	w.w.merge(wk)
	return nil
}

func (w *TxWriter) Notify(ctx context.Context) error {
	wk := w.w
	w.w = wake{}
	if len(wk.keys) > 0 {
		queues, err := w.s.admit(ctx, wk.keys, wk.maxes)
		if err != nil {
			return err
		}
		for _, q := range queues {
			wk.queue(q)
		}
	}
	if len(wk.queues) == 0 {
		return nil
	}
	if err := w.s.notifyNow(ctx, w.s.channel("jobs"), wk.queues); err != nil {
		return fmt.Errorf("kiln: notify: %w", err)
	}
	return nil
}

func (s *Store) InTx(ctx context.Context, fn func(w driver.Writer) error) error {
	var w *TxWriter
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		w = s.Tx(tx)
		return fn(w)
	})
	if err != nil {
		return err
	}
	if len(w.w.keys) > 0 {
		queues, err := s.admit(ctx, w.w.keys, w.w.maxes)
		if err == nil {
			for _, q := range queues {
				w.w.queue(q)
			}
		}
	}
	s.nt.jobs(w.w.queues...)
	return nil
}

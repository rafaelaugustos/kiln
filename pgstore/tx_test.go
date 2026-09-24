package pgstore

import (
	"context"
	"errors"
	"testing"

	"github.com/rafaelaugustos/kiln/driver"
)

func TestTxWriter(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	if _, err := s.Tx(nil).Insert(ctx, []driver.InsertParams{job("a")}); !errors.Is(err, driver.ErrNilTx) {
		t.Fatalf("nil tx: %v", err)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	w := s.Tx(tx)
	res, err := w.Insert(ctx, []driver.InsertParams{job("a"), job("a", limited("tx", 1))})
	if err != nil {
		t.Fatal(err)
	}
	if got := claim(t, s, 10); len(got) != 0 {
		t.Fatalf("uncommitted jobs visible: %v", got)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Job(ctx, res[0].ID); !errors.Is(err, driver.ErrNotFound) {
		t.Fatalf("rolled back job exists: %v", err)
	}

	tx, _ = s.pool.Begin(ctx)
	w = s.Tx(tx)
	res, err = w.Insert(ctx, []driver.InsertParams{job("a"), job("a", limited("tx", 1))})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := w.Notify(ctx); err != nil {
		t.Fatal(err)
	}
	if got := claim(t, s, 10); len(got) != 2 {
		t.Fatalf("claimed %d after commit + notify", len(got))
	}

	var batch int64
	err = s.InTx(ctx, func(w driver.Writer) error {
		var err error
		if batch, err = w.OpenBatch(ctx, driver.NewBatch{Description: "tx"}); err != nil {
			return err
		}
		if _, err := w.Insert(ctx, []driver.InsertParams{job("m", inBatch(batch))}); err != nil {
			return err
		}
		return w.SealBatch(ctx, batch)
	})
	if err != nil {
		t.Fatal(err)
	}
	if b, err := s.Batch(ctx, batch); err != nil || !b.Sealed || b.Total != 1 {
		t.Fatalf("batch %+v %v", b, err)
	}
	boom := errors.New("boom")
	if err := s.InTx(ctx, func(w driver.Writer) error {
		w.Insert(ctx, []driver.InsertParams{job("gone")})
		return boom
	}); !errors.Is(err, boom) {
		t.Fatalf("InTx error %v", err)
	}
	var n int
	s.pool.QueryRow(ctx, "SELECT count(*) FROM "+s.schema+".jobs WHERE kind = 'gone'").Scan(&n)
	if n != 0 {
		t.Fatal("rolled back InTx insert persisted")
	}
}

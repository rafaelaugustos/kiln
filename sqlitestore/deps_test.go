package sqlitestore

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/rafaelaugustos/kiln/driver"
)

func inBatch(id int64) func(*driver.InsertParams) {
	return func(p *driver.InsertParams) { p.BatchID = id }
}

func TestChildInsertRacesParentFinish(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	for round := range 40 {
		p := insert(t, s, job("p"))[0].ID
		j := claim(t, s, 1)[0]
		var (
			wg    sync.WaitGroup
			child driver.Inserted
		)
		wg.Go(func() {
			res, err := s.Insert(ctx, []driver.InsertParams{job("c", after(driver.OnSucceeded, p))})
			if err != nil {
				t.Error(err)
				return
			}
			child = res[0]
		})
		wg.Go(func() {
			if rs, err := s.Finish(ctx, "srv", []driver.Outcome{{Ref: j.Ref, State: driver.Succeeded}}); err != nil || rs[0] != driver.Applied {
				t.Errorf("finish %v %v", rs, err)
			}
		})
		wg.Wait()
		if t.Failed() {
			return
		}
		if r := record(t, s, child.ID); r.State != driver.Enqueued {
			t.Fatalf("round %d: child %s (inserted as %s)", round, r.State, child.State)
		}
		for _, j := range claim(t, s, 10) {
			finish(t, s, driver.Outcome{Ref: j.Ref, State: driver.Succeeded})
		}
	}
}

func TestFinishBoundsFanout(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	b, err := s.OpenBatch(ctx, driver.NewBatch{})
	if err != nil {
		t.Fatal(err)
	}
	p := insert(t, s, job("p", inBatch(b)))[0].ID
	if err := s.SealBatch(ctx, b); err != nil {
		t.Fatal(err)
	}
	kids := make([]driver.InsertParams, 2*(fanout+3))
	for i := range kids {
		kids[i] = job("c", queue("c"), after(driver.OnSucceeded, p))
		if i%2 == 1 {
			kids[i] = job("c", queue("c"), func(p *driver.InsertParams) { p.AfterBatch = b })
		}
	}
	insert(t, s, kids...)
	j := claim(t, s, 1)[0]
	finish(t, s, driver.Outcome{Ref: j.Ref, State: driver.Succeeded})
	awaiting := "SELECT COUNT(*) FROM kiln_jobs WHERE state = 'awaiting'"
	if n := count(t, s, awaiting); n == 0 || n == len(kids) {
		t.Fatalf("%d of %d children awaiting after finish, want the cascade bounded", n, len(kids))
	}
	for range 3 {
		if _, err := s.Sweep(ctx, 100); err != nil {
			t.Fatal(err)
		}
	}
	if n := count(t, s, awaiting); n != 0 {
		t.Fatalf("%d children awaiting after three sweeps", n)
	}
}

func TestSweepRepairsVanishedParents(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	b, err := s.OpenBatch(ctx, driver.NewBatch{})
	if err != nil {
		t.Fatal(err)
	}
	on := func(mask driver.Mask) func(*driver.InsertParams) {
		return func(p *driver.InsertParams) { p.Parents = []driver.Parent{{Index: 0, On: mask}} }
	}
	res := insert(t, s, job("p"), job("d", on(driver.OnSucceeded)), job("r", on(driver.OnDeleted)),
		job("t", func(p *driver.InsertParams) { p.AfterBatch = b }))
	for _, q := range []string{
		fmt.Sprintf("DELETE FROM kiln_jobs WHERE id = %d", res[0].ID),
		fmt.Sprintf("DELETE FROM kiln_batches WHERE id = %d", b),
	} {
		if _, err := s.db.ExecContext(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	if n, err := s.Sweep(ctx, 100); err != nil || n == 0 {
		t.Fatalf("sweep changed %d rows, %v", n, err)
	}
	want := map[int64]driver.State{res[1].ID: driver.Deleted, res[2].ID: driver.Enqueued, res[3].ID: driver.Enqueued}
	for id, st := range want {
		if r := record(t, s, id); r.State != st {
			t.Fatalf("job %d is %s, want %s", id, r.State, st)
		}
	}
	h := record(t, s, res[1].ID).History
	if reason := fmt.Sprintf("parent %d pruned", res[0].ID); len(h) != 1 || h[0].Reason != reason {
		t.Fatalf("doomed history %+v, want %q", h, reason)
	}
	if n, err := s.Sweep(ctx, 100); err != nil || n != 0 {
		t.Fatalf("second sweep changed %d rows, %v", n, err)
	}
}

func TestSweepRestoresMissingLimit(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	res := insert(t, s, job("a", limited("gone", 1)), job("a", limited("gone", 1)))
	if res[0].State != driver.Enqueued || res[1].State != driver.Throttled {
		t.Fatalf("inserted %+v", res)
	}
	if _, err := s.db.Exec("DELETE FROM kiln_limits"); err != nil {
		t.Fatal(err)
	}
	j := claim(t, s, 1)[0]
	finish(t, s, driver.Outcome{Ref: j.Ref, State: driver.Succeeded})
	if _, err := s.Sweep(ctx, 100); err != nil {
		t.Fatal(err)
	}
	if r := record(t, s, res[1].ID); r.State != driver.Enqueued {
		t.Fatalf("throttled job %s after sweep, want enqueued", r.State)
	}
	insert(t, s, job("a", limited("gone", 3)), job("a", limited("gone", 3)))
	if got := claim(t, s, 10); len(got) != 3 {
		t.Fatalf("claimed %d after the limit was declared again, want 3", len(got))
	}
}

func TestFanInCascadesInOneFinish(t *testing.T) {
	t.Parallel()
	s := open(t)
	ps := make([]driver.InsertParams, 6)
	ps[0] = job("root")
	for i := 1; i < len(ps); i++ {
		ps[i] = job("link", queue("later"), func(p *driver.InsertParams) {
			p.Parents = []driver.Parent{{Index: i - 1, On: driver.OnSucceeded}}
		})
	}
	ids := insert(t, s, ps...)
	j := claim(t, s, 1)[0]
	finish(t, s, driver.Outcome{Ref: j.Ref, State: driver.Deleted, Reason: "canceled"})
	for i, in := range ids[1:] {
		r := record(t, s, in.ID)
		if r.State != driver.Deleted || r.History[0].Reason != fmt.Sprintf("parent %d deleted", ids[i].ID) {
			t.Fatalf("link %d: %s %+v, want deleted by its parent in the same finish", in.ID, r.State, r.History)
		}
	}
	if c, err := s.Counts(context.Background()); err != nil || c.Deleted != int64(len(ids)) {
		t.Fatalf("counts %+v %v", c, err)
	}
}

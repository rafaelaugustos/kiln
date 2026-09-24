package mysqlstore

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/rafaelaugustos/kiln/driver"
)

func after(on driver.Mask, ids ...int64) func(*driver.InsertParams) {
	return func(p *driver.InsertParams) {
		for _, id := range ids {
			p.Parents = append(p.Parents, driver.Parent{ID: id, On: on})
		}
	}
}

func finishUntilDone(t *testing.T, s *Store, out driver.Outcome) {
	t.Helper()
	for {
		rs, err := s.Finish(context.Background(), "srv", []driver.Outcome{out})
		if err != nil {
			t.Error(err)
			return
		}
		if rs[0] != driver.Busy {
			if rs[0] != driver.Applied {
				t.Errorf("finish %d: %v", out.ID, rs[0])
			}
			return
		}
	}
}

func count(t *testing.T, s *Store, query string) int {
	t.Helper()
	var n int
	if err := s.db.QueryRow(query).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestFanInResolvedOnce(t *testing.T) {
	t.Parallel()
	s := open(t)
	const width = 16
	for round := range 5 {
		ps := make([]driver.InsertParams, width)
		for i := range ps {
			ps[i] = job("p")
		}
		parents := insert(t, s, ps...)
		ids := make([]int64, width)
		for i, p := range parents {
			ids[i] = p.ID
		}
		child := insert(t, s, job("child", func(p *driver.InsertParams) { p.Queue = "child" }, after(driver.OnSucceeded, ids...)))[0].ID
		jobs := claim(t, s, width)
		if len(jobs) != width {
			t.Fatalf("round %d: claimed %d parents", round, len(jobs))
		}
		var wg sync.WaitGroup
		for _, j := range jobs {
			wg.Go(func() {
				finishUntilDone(t, s, driver.Outcome{Ref: j.Ref, State: driver.Succeeded})
			})
		}
		wg.Wait()
		if r := record(t, s, child); r.State != driver.Enqueued || r.PendingDeps != 0 {
			t.Fatalf("round %d: child %s pending %d", round, r.State, r.PendingDeps)
		}
		if got := claim(t, s, 10, "child"); len(got) != 1 || got[0].ID != child {
			t.Fatalf("round %d: claimed %+v, want the child once", round, got)
		}
		if n := count(t, s, "SELECT COUNT(*) FROM kiln_deps WHERE resolved = FALSE"); n != 0 {
			t.Fatalf("round %d: %d deps unresolved", round, n)
		}
	}
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
		wg.Add(2)
		go func() {
			defer wg.Done()
			res, err := s.Insert(ctx, []driver.InsertParams{job("c", after(driver.OnSucceeded, p))})
			if err != nil {
				t.Error(err)
				return
			}
			child = res[0]
		}()
		go func() {
			defer wg.Done()
			finishUntilDone(t, s, driver.Outcome{Ref: j.Ref, State: driver.Succeeded})
		}()
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
	kids := make([]driver.InsertParams, 2*5003)
	for i := range kids {
		kids[i] = job("c", func(p *driver.InsertParams) { p.Queue = "c" }, after(driver.OnSucceeded, p))
		if i%2 == 1 {
			kids[i] = job("c", func(p *driver.InsertParams) { p.Queue, p.AfterBatch = "c", b })
		}
	}
	insert(t, s, kids...)
	j := claim(t, s, 1)[0]
	finish(t, s, driver.Outcome{Ref: j.Ref, State: driver.Succeeded})
	awaiting := "SELECT COUNT(*) FROM kiln_jobs WHERE state = 'awaiting'"
	if n := count(t, s, awaiting); n != 6 {
		t.Fatalf("%d children awaiting after finish, want 6 left to sweep", n)
	}
	if _, err := s.Sweep(ctx, 100); err != nil {
		t.Fatal(err)
	}
	if n := count(t, s, awaiting); n != 0 {
		t.Fatalf("%d children awaiting after sweep", n)
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

package pgstore

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/rafaelaugustos/kiln/driver"
)

func after(on driver.Mask, ids ...int64) func(*driver.InsertParams) {
	return func(p *driver.InsertParams) {
		for _, id := range ids {
			p.Parents = append(p.Parents, driver.Parent{ID: id, On: on})
		}
	}
}

func needs(idx ...int) func(*driver.InsertParams) {
	return func(p *driver.InsertParams) {
		for _, i := range idx {
			p.Parents = append(p.Parents, driver.Parent{Index: i, On: driver.OnSucceeded})
		}
	}
}

func run(t testing.TB, s *Store, id int64, state driver.State) {
	t.Helper()
	for _, j := range claim(t, s, 100) {
		out := driver.Outcome{Ref: j.Ref, State: driver.Enqueued, Refund: true}
		if j.ID == id {
			out = driver.Outcome{Ref: j.Ref, State: state, Reason: "test"}
			if state == driver.Succeeded {
				out.Reason = ""
			}
		}
		if res := finish(t, s, out); res[0] != driver.Applied {
			t.Fatalf("finish %d: %v", j.ID, res)
		}
	}
}

func TestDependencies(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	p := insert(t, s, job("parent"))[0].ID
	kids := insert(t, s,
		job("ok", after(driver.OnSucceeded, p)),
		job("any", after(driver.OnFinished, p)),
		job("onfail", after(driver.OnFailed, p)),
	)
	for _, k := range kids {
		if k.State != driver.Awaiting {
			t.Fatalf("child %+v", k)
		}
	}
	grand := insert(t, s, job("grand", after(driver.OnSucceeded, kids[2].ID)))[0]
	run(t, s, p, driver.Succeeded)
	want := map[int64]driver.State{
		kids[0].ID: driver.Enqueued,
		kids[1].ID: driver.Enqueued,
		kids[2].ID: driver.Deleted,
		grand.ID:   driver.Awaiting,
	}
	for id, st := range want {
		if r := record(t, s, id); r.State != st {
			t.Fatalf("job %d state %s, want %s", id, r.State, st)
		}
	}
	if r := record(t, s, kids[2].ID); r.History[len(r.History)-1].Reason == "" {
		t.Fatalf("doom reason missing: %+v", r.History)
	}
	if _, err := s.Sweep(ctx, 100); err != nil {
		t.Fatal(err)
	}
	if r := record(t, s, grand.ID); r.State != driver.Deleted {
		t.Fatalf("grandchild after sweep: %s", r.State)
	}
	if r := record(t, s, p); len(r.Children) != 3 {
		t.Fatalf("children %v", r.Children)
	}
	late := insert(t, s, job("late", after(driver.OnSucceeded, p)), job("late2", after(driver.OnDeleted, p)))
	if late[0].State != driver.Enqueued || late[1].State != driver.Deleted {
		t.Fatalf("late children %+v", late)
	}
}

func TestDependencyFailedParent(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	p := insert(t, s, job("parent"))[0].ID
	kids := insert(t, s, job("ok", after(driver.OnSucceeded, p)), job("fin", after(driver.OnFinished, p)))
	run(t, s, p, driver.Failed)
	if r := record(t, s, kids[0].ID); r.State != driver.Awaiting || r.PendingDeps != 1 {
		t.Fatalf("pending child %+v", r)
	}
	if r := record(t, s, kids[1].ID); r.State != driver.Enqueued {
		t.Fatalf("finished child %s", r.State)
	}
	if n, err := s.Requeue(ctx, driver.Filter{IDs: []int64{p}}); err != nil || n != 1 {
		t.Fatalf("requeue %d %v", n, err)
	}
	run(t, s, p, driver.Succeeded)
	if r := record(t, s, kids[0].ID); r.State != driver.Enqueued {
		t.Fatalf("child after requeue %s", r.State)
	}
}

func TestInsertFlow(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	res := insert(t, s, job("c", needs(1, 2)), job("a"), job("b", needs(1)))
	if res[0].State != driver.Awaiting || res[1].State != driver.Enqueued || res[2].State != driver.Awaiting {
		t.Fatalf("flow %+v", res)
	}
	if !(res[0].ID < res[1].ID && res[1].ID < res[2].ID) {
		t.Fatalf("ids %+v", res)
	}
	if r := record(t, s, res[0].ID); len(r.Parents) != 2 || r.Parents[0] != res[1].ID || r.PendingDeps != 2 {
		t.Fatalf("parents %+v", r)
	}
	run(t, s, res[1].ID, driver.Succeeded)
	run(t, s, res[2].ID, driver.Succeeded)
	if r := record(t, s, res[0].ID); r.State != driver.Enqueued {
		t.Fatalf("fan-in child %s", r.State)
	}

	bad := [][]driver.InsertParams{
		{job("x", needs(0))},
		{job("x", needs(1)), job("y", needs(0))},
		{job("x", needs(5))},
	}
	for _, jobs := range bad {
		if _, err := s.Insert(ctx, jobs); !errors.Is(err, driver.ErrInvalid) {
			t.Fatalf("invalid flow accepted: %v", err)
		}
	}
	if _, err := s.Insert(ctx, []driver.InsertParams{job("x"), job("y", after(driver.OnSucceeded, 1<<40))}); !errors.Is(err, driver.ErrNotFound) {
		t.Fatalf("missing parent: %v", err)
	}
	c, err := s.Counts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if c.Enqueued != 1 {
		t.Fatalf("partial insert leaked: %+v", c)
	}
}

func TestFanInConcurrent(t *testing.T) {
	t.Parallel()
	s := open(t)
	const n = 16
	ps := make([]driver.InsertParams, n)
	for i := range ps {
		ps[i] = job("p")
	}
	parents := insert(t, s, ps...)
	ids := make([]int64, n)
	for i, p := range parents {
		ids[i] = p.ID
	}
	child := insert(t, s, job("child", after(driver.OnSucceeded, ids...)))[0].ID
	jobs := claim(t, s, n)
	var wg sync.WaitGroup
	for _, j := range jobs {
		wg.Go(func() {
			for {
				res, err := s.Finish(context.Background(), "srv", []driver.Outcome{{Ref: j.Ref, State: driver.Succeeded}})
				if err != nil {
					t.Error(err)
					return
				}
				if res[0] != driver.Busy {
					return
				}
			}
		})
	}
	wg.Wait()
	r := record(t, s, child)
	if r.State != driver.Enqueued || r.PendingDeps != 0 {
		t.Fatalf("child %s pending %d", r.State, r.PendingDeps)
	}
}

func TestChildInsertRacesParentFinish(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	for i := range 40 {
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
			for {
				res, err := s.Finish(ctx, "srv", []driver.Outcome{{Ref: j.Ref, State: driver.Succeeded}})
				if err != nil {
					t.Error(err)
					return
				}
				if res[0] != driver.Busy {
					return
				}
			}
		}()
		wg.Wait()
		if r := record(t, s, child.ID); r.State != driver.Enqueued {
			pr := record(t, s, p)
			t.Fatalf("round %d: child %s (insert said %s) child run %v created %v parent attempted %v final %v hist %+v", i, r.State, child.State, r.RunAt, r.CreatedAt, pr.AttemptedAt, pr.FinalizedAt, r.History)
		}
		for _, j := range claim(t, s, 10) {
			finish(t, s, driver.Outcome{Ref: j.Ref, State: driver.Succeeded})
		}
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
	for _, q := range []string{"DELETE FROM " + s.schema + ".jobs WHERE id = $1", "DELETE FROM " + s.schema + ".batches WHERE id = $1"} {
		id := res[0].ID
		if strings.Contains(q, "batches") {
			id = b
		}
		if _, err := s.pool.Exec(ctx, q, id); err != nil {
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

func TestStatsAttribution(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	deleted := insert(t, s, job("p", func(p *driver.InsertParams) { p.Queue = "d" }))[0].ID
	insert(t, s, job("c", after(driver.OnSucceeded, deleted)))
	if n, err := s.Delete(ctx, driver.Filter{IDs: []int64{deleted}}); err != nil || n != 1 {
		t.Fatalf("delete %d %v", n, err)
	}
	succeeded := insert(t, s, job("p"))[0].ID
	insert(t, s, job("c", after(driver.OnFailed, succeeded)))
	run(t, s, succeeded, driver.Succeeded)
	failed := insert(t, s, job("p"))[0].ID
	insert(t, s, job("c", after(driver.OnSucceeded, failed)))
	run(t, s, failed, driver.Failed)
	if _, err := s.Prune(ctx, driver.PruneParams{Succeeded: -1, Deleted: -1}); err != nil {
		t.Fatal(err)
	}
	rows, err := s.pool.Query(ctx, "SELECT server, sum(succeeded), sum(failed), sum(deleted) FROM "+s.schema+".stats GROUP BY server ORDER BY server")
	if err != nil {
		t.Fatal(err)
	}
	type row struct {
		Server                     string
		Succeeded, Failed, Deleted int64
	}
	got, err := pgx.CollectRows(rows, pgx.RowToStructByPos[row])
	if err != nil {
		t.Fatal(err)
	}
	want := []row{{"", 0, 0, 3}, {"srv", 1, 1, 1}}
	if !slices.Equal(got, want) {
		t.Fatalf("stats %+v, want %+v", got, want)
	}
}

func TestTxMissingParentLeavesNoUnique(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	b, err := s.OpenBatch(ctx, driver.NewBatch{})
	if err != nil {
		t.Fatal(err)
	}
	missing := []driver.InsertParams{
		job("c", unique("orphan", 0), after(driver.OnSucceeded, 1<<40)),
		job("c", unique("orphan", 0), func(p *driver.InsertParams) { p.AfterBatch = b + 1 }),
	}
	for _, p := range missing {
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.Tx(tx).Insert(ctx, []driver.InsertParams{p}); !errors.Is(err, driver.ErrNotFound) {
			t.Fatalf("missing link: %v", err)
		}
		tx.Commit(ctx)
		if res := insert(t, s, job("c", unique("orphan", 0)))[0]; res.Duplicate {
			t.Fatalf("failed insert left its key held: %+v", res)
		}
		if _, err := s.pool.Exec(ctx, "DELETE FROM "+s.schema+".uniques"); err != nil {
			t.Fatal(err)
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
		kids[i] = job("c", after(driver.OnSucceeded, p))
		if i%2 == 1 {
			kids[i] = job("c", func(p *driver.InsertParams) { p.AfterBatch = b })
		}
	}
	insert(t, s, kids...)
	awaiting := func() int {
		var n int
		if err := s.pool.QueryRow(ctx, "SELECT count(*) FROM "+s.schema+".jobs WHERE state = 'awaiting'").Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	run(t, s, p, driver.Succeeded)
	if n := awaiting(); n != 6 {
		t.Fatalf("%d children awaiting after finish, want 6 left to sweep", n)
	}
	if _, err := s.Sweep(ctx, 100); err != nil {
		t.Fatal(err)
	}
	if n := awaiting(); n != 0 {
		t.Fatalf("%d children awaiting after sweep", n)
	}
}

func TestPruneLeftovers(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	p := insert(t, s, job("p"))[0].ID
	c := insert(t, s, job("c", after(driver.OnSucceeded, p)))[0].ID
	run(t, s, p, driver.Succeeded)
	run(t, s, c, driver.Succeeded)
	if _, err := s.pool.Exec(ctx, "INSERT INTO "+s.schema+".uniques (key, job_id) VALUES ('gone', $1)", int64(1<<40)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Prune(ctx, driver.PruneParams{Deleted: -1, Failed: -1}); err != nil {
		t.Fatal(err)
	}
	var deps, keys int
	q := "SELECT (SELECT count(*) FROM " + s.schema + ".deps), (SELECT count(*) FROM " + s.schema + ".uniques)"
	if err := s.pool.QueryRow(ctx, q).Scan(&deps, &keys); err != nil {
		t.Fatal(err)
	}
	if deps != 0 || keys != 0 {
		t.Fatalf("%d deps and %d unique keys left after prune", deps, keys)
	}
}

package mysqlstore

import (
	"context"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/rafaelaugustos/kiln/driver"
)

var keepAll = driver.PruneParams{Retention: driver.Retention{Succeeded: -1, Deleted: -1, Failed: -1}}

func repeatableRead(c *mysql.Config) {
	if c.Params == nil {
		c.Params = map[string]string{}
	}
	c.Params["transaction_isolation"] = "'REPEATABLE-READ'"
}

func TestPruneRepeatableRead(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, err := New(ctx, database(t, repeatableRead))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	var last int64
	for range 5 {
		if last, err = s.OpenBatch(ctx, driver.NewBatch{}); err != nil {
			t.Fatal(err)
		}
		if err := s.SealBatch(ctx, last); err != nil {
			t.Fatal(err)
		}
	}
	insert(t, s, job("a"))
	j := claim(t, s, 1)[0]
	tx := begin(t, s)
	if _, err := tx.ExecContext(ctx, render("SELECT id FROM kiln_batches WHERE id = ? FOR UPDATE", last)); err != nil {
		t.Fatal(err)
	}
	pruned := make(chan error, 1)
	go func() {
		_, err := s.Prune(ctx, keepAll)
		pruned <- err
	}()
	time.Sleep(300 * time.Millisecond)
	soon, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if _, err := s.Insert(soon, []driver.InsertParams{job("b")}); err != nil {
		t.Fatalf("insert while batches are pruned: %v", err)
	}
	finishSoon(t, s, driver.Outcome{Ref: j.Ref, State: driver.Succeeded})
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := <-pruned; err != nil {
		t.Fatal(err)
	}
	if _, err := s.Prune(ctx, keepAll); err != nil {
		t.Fatal(err)
	}
	if n := count(t, s, "SELECT COUNT(*) FROM kiln_batches"); n != 0 {
		t.Fatalf("%d finished batches left", n)
	}
}

func TestPruneStatsMixedServers(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	servers := []string{"api-1:42:0a0b", "wörker:42:0c0d", `it's\a`}
	for _, server := range servers {
		insert(t, s, job("a"))
		js, err := s.Claim(ctx, driver.ClaimQuery{Queues: []string{"default"}, Limit: 1, Server: server})
		if err != nil || len(js) != 1 {
			t.Fatalf("claim %v %v", js, err)
		}
		if rs, err := s.Finish(ctx, server, []driver.Outcome{{Ref: js[0].Ref, State: driver.Succeeded}}); err != nil || rs[0] != driver.Applied {
			t.Fatalf("finish %v %v", rs, err)
		}
	}
	insert(t, s, job("x", unique("expired", time.Microsecond)))
	time.Sleep(2 * time.Millisecond)
	p := keepAll
	p.Stats = time.Microsecond
	if _, err := s.Prune(ctx, p); err != nil {
		t.Fatal(err)
	}
	if n := count(t, s, "SELECT COUNT(*) FROM kiln_stats"); n != 1 {
		t.Fatalf("%d stats rows, want only the totals", n)
	}
	if n := count(t, s, "SELECT succeeded FROM kiln_stats"); n != len(servers) {
		t.Fatalf("totals succeeded %d, want %d", n, len(servers))
	}
	if n := count(t, s, "SELECT COUNT(*) FROM kiln_uniques"); n != 0 {
		t.Fatalf("%d expired keys left", n)
	}
}

func TestPruneUniquesRacesReclaim(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	key := func(b byte, d time.Duration) func(*driver.InsertParams) {
		return func(p *driver.InsertParams) { p.UniqueKey, p.UniqueFor = []byte{b}, d }
	}
	insert(t, s, job("a", key(2, time.Millisecond)), job("a", key(1, 2*time.Millisecond)))
	time.Sleep(20 * time.Millisecond)
	tx := begin(t, s)
	w := s.Tx(tx)
	if _, err := w.Insert(ctx, []driver.InsertParams{job("b", key(1, time.Hour))}); err != nil {
		t.Fatal(err)
	}
	pruned := make(chan error, 1)
	go func() {
		_, err := s.Prune(ctx, keepAll)
		pruned <- err
	}()
	time.Sleep(300 * time.Millisecond)
	res, err := w.Insert(ctx, []driver.InsertParams{job("b", key(2, time.Hour))})
	if err != nil {
		t.Fatalf("reclaim while expired keys are pruned: %v", err)
	}
	if res[0].Duplicate {
		t.Fatalf("expired key reported as held: %+v", res[0])
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := <-pruned; err != nil {
		t.Fatalf("prune: %v", err)
	}
	if n := count(t, s, "SELECT COUNT(*) FROM kiln_uniques WHERE expires_at > UTC_TIMESTAMP(6)"); n != 2 {
		t.Fatalf("%d live keys, want the 2 reclaimed ones", n)
	}
}

package mysqlstore

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/rafaelaugustos/kiln/driver"
)

func packet(n int) func(*mysql.Config) {
	return func(c *mysql.Config) { c.MaxAllowedPacket = n }
}

func TestFinishSplitsOversizedFlush(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, err := New(ctx, database(t, packet(4<<20)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	output, err := json.Marshal(strings.Repeat("ü", 400_000))
	if err != nil {
		t.Fatal(err)
	}
	trace := strings.Repeat("goroutine 1 [running]:\n\tmain.go:42 +0x1d\n", 250)
	const n = 256
	jobs := make([]driver.InsertParams, n)
	for i := range jobs {
		jobs[i] = job("a", func(p *driver.InsertParams) { p.MaxAttempts = 1 })
	}
	insert(t, s, jobs...)
	js := claim(t, s, n)
	outs := make([]driver.Outcome, n)
	for i, j := range js {
		outs[i] = driver.Outcome{Ref: j.Ref, State: driver.Failed, Reason: "boom", Error: strings.Repeat("é", 1000), Trace: trace}
		if i < 8 {
			outs[i] = driver.Outcome{Ref: j.Ref, State: driver.Succeeded, Output: output}
		}
	}
	rs, err := s.Finish(ctx, "srv", outs)
	if err != nil {
		t.Fatalf("finish of an oversized flush: %v", err)
	}
	for i, r := range rs {
		if r != driver.Applied {
			t.Fatalf("outcome %d: %v", i, r)
		}
	}
	if r := record(t, s, js[0].ID); r.State != driver.Succeeded || len(r.Output) != len(output) {
		t.Fatalf("succeeded job %s with %d output bytes", r.State, len(r.Output))
	}
}

func TestStatementsFitThePacket(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, err := New(ctx, database(t, packet(2<<20)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	if n := count(t, s, "SELECT @@max_allowed_packet"); s.budget != min(maxStatement, n-4<<10) {
		t.Fatalf("statement budget %d with max_allowed_packet %d", s.budget, n)
	}
	s.budget = 1 << 20
	args, err := json.Marshal(strings.Repeat("a", 700_000))
	if err != nil {
		t.Fatal(err)
	}
	jobs := make([]driver.InsertParams, 4)
	for i := range jobs {
		jobs[i] = job("a", func(p *driver.InsertParams) { p.Args = args })
	}
	insert(t, s, jobs...)
	if got := claim(t, s, 10); len(got) != 4 {
		t.Fatalf("claimed %d, want 4", len(got))
	}
}

func TestChunksStayUnderBudget(t *testing.T) {
	c := &chunks{head: "H", tail: "T", max: 10}
	for _, r := range []string{"aaa", "bbb", "ccc", "dddddddddddd", "e"} {
		c.row()
		c.b = append(c.b, r...)
	}
	got := c.done()
	want := []string{"Haaa,bbbT", "HcccT", "HddddddddddddT", "HeT"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("chunks %q, want %q", got, want)
	}
}

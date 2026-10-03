package drivertest

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

var consoleTests = []test{
	{"Fencing", testConsoleFencing},
	{"Order", testConsoleOrder},
	{"Attempts", testConsoleAttempts},
	{"Progress", testConsoleProgress},
	{"Prune", testConsolePrune},
}

func console(t *testing.T, s driver.Store) driver.Console {
	t.Helper()
	c, ok := s.(driver.Console)
	if !ok {
		t.Skip("store does not implement driver.Console")
	}
	return c
}

func write(t *testing.T, c driver.Console, ref driver.Ref, progress int, lines ...string) {
	t.Helper()
	if err := c.WriteConsole(t.Context(), ref, lines, progress); err != nil {
		t.Fatalf("write console of job %d: %v", ref.ID, err)
	}
}

func logs(t *testing.T, c driver.Console, id, after int64, limit int) []driver.LogLine {
	t.Helper()
	ls, err := c.Logs(t.Context(), id, after, limit)
	if err != nil {
		t.Fatalf("logs of job %d after %d: %v", id, after, err)
	}
	return ls
}

func texts(ls []driver.LogLine) []string {
	out := make([]string, len(ls))
	for i, l := range ls {
		out[i] = l.Text
	}
	return out
}

func wantLines(t *testing.T, c driver.Console, id int64, want ...string) []driver.LogLine {
	t.Helper()
	ls := logs(t, c, id, 0, 1000)
	if got := texts(ls); !slices.Equal(got, want) {
		t.Fatalf("job %d lines %q, want %q", id, got, want)
	}
	for i := 1; i < len(ls); i++ {
		if ls[i].Seq <= ls[i-1].Seq {
			t.Fatalf("job %d line %d has seq %d after %d", id, i, ls[i].Seq, ls[i-1].Seq)
		}
	}
	return ls
}

func testConsoleFencing(t *testing.T, s driver.Store) {
	c := console(t, s)
	ctx := t.Context()
	j := start(t, s, task("q"))
	write(t, c, j.Ref, 10, "kept")
	idle := add(t, s, task("idle"))
	for _, ref := range []driver.Ref{
		{ID: j.ID, Claim: j.Claim + 1},
		{ID: j.ID + 1000, Claim: 1},
		{ID: idle},
		{ID: idle, Claim: 1},
	} {
		wantErr(t, c.WriteConsole(ctx, ref, []string{"stale"}, 50), driver.ErrLost)
		wantErr(t, c.WriteConsole(ctx, ref, nil, 50), driver.ErrLost)
	}
	apply(t, s, outcome(j, driver.Succeeded))
	wantErr(t, c.WriteConsole(ctx, j.Ref, []string{"late"}, 90), driver.ErrLost)
	wantErr(t, c.WriteConsole(ctx, j.Ref, nil, 90), driver.ErrLost)
	wantLines(t, c, j.ID, "kept")
	wantLines(t, c, idle)
	if p := record(t, s, j.ID).Progress; p != 10 {
		t.Fatalf("progress %d after lost writes, want 10", p)
	}
}

func testConsoleOrder(t *testing.T, s driver.Store) {
	c := console(t, s)
	j := start(t, s, task("q"))
	want := []string{"", "tab\tquote'\"back\\slash", "two\nlines", "ação",
		strings.Repeat("x", 4<<10), strings.Repeat("é", 2<<10)}
	lo := now(t, s)
	write(t, c, j.Ref, -1, want...)
	for i := range 3 {
		var batch []string
		for k := range 50 {
			batch = append(batch, fmt.Sprintf("line %d", i*50+k))
		}
		write(t, c, j.Ref, -1, batch...)
		want = append(want, batch...)
	}
	hi := now(t, s)
	all := wantLines(t, c, j.ID, want...)
	for _, l := range all {
		if l.Attempt != 1 {
			t.Fatalf("line %d attempt %d, want 1", l.Seq, l.Attempt)
		}
		within(t, "line time", l.At, lo, hi)
	}
	if first := logs(t, c, j.ID, 0, 0); len(first) != 100 || first[99].Seq != all[99].Seq {
		t.Fatalf("logs with no limit returned %d lines, want the first 100", len(first))
	}
	var paged []driver.LogLine
	for after := int64(0); ; {
		page := logs(t, c, j.ID, after, 40)
		if len(page) > 40 {
			t.Fatalf("page of %d lines with limit 40", len(page))
		}
		if len(page) == 0 {
			break
		}
		paged = append(paged, page...)
		after = page[len(page)-1].Seq
	}
	if !slices.Equal(paged, all) {
		t.Fatalf("paging by seq returned %d lines, want the %d lines in order", len(paged), len(all))
	}
	if ls := logs(t, c, j.ID+1000, 0, 0); len(ls) != 0 {
		t.Fatalf("job without a console has %d lines", len(ls))
	}
}

func testConsoleAttempts(t *testing.T, s driver.Store) {
	c := console(t, s)
	j := start(t, s, task("q"))
	write(t, c, j.Ref, -1, "first")
	apply(t, s, driver.Outcome{Ref: j.Ref, State: driver.Scheduled, Reason: "retry"})
	k := claimOne(t, s, "q", j.ID)
	wantErr(t, c.WriteConsole(t.Context(), j.Ref, []string{"stale"}, -1), driver.ErrLost)
	write(t, c, k.Ref, -1, "second")
	apply(t, s, outcome(k, driver.Failed))
	requeueIDs(t, s, j.ID)
	r := claimOne(t, s, "q", j.ID)
	write(t, c, r.Ref, -1, "third")
	ls := wantLines(t, c, j.ID, "first", "second", "third")
	if got := []int{ls[0].Attempt, ls[1].Attempt, ls[2].Attempt}; !slices.Equal(got, []int{1, 2, 1}) {
		t.Fatalf("line attempts %v, want [1 2 1]", got)
	}
}

func testConsoleProgress(t *testing.T, s driver.Store) {
	c := console(t, s)
	j := start(t, s, task("q"))
	wantProgress := func(want int) {
		t.Helper()
		if p := record(t, s, j.ID).Progress; p != want {
			t.Fatalf("job %d progress %d, want %d", j.ID, p, want)
		}
	}
	wantProgress(0)
	write(t, c, j.Ref, 40)
	wantProgress(40)
	write(t, c, j.Ref, -1, "line")
	wantProgress(40)
	write(t, c, j.Ref, 0)
	wantProgress(0)
	write(t, c, j.Ref, 75, "done")
	wantProgress(75)
	apply(t, s, outcome(j, driver.Succeeded))
	wantProgress(75)
	requeueIDs(t, s, j.ID)
	wantProgress(75)
}

func testConsolePrune(t *testing.T, s driver.Store) {
	c := console(t, s)
	done := start(t, s, task("a"))
	write(t, c, done.Ref, 50, "a1", "a2")
	apply(t, s, outcome(done, driver.Succeeded))
	failed := start(t, s, task("b"))
	write(t, c, failed.Ref, -1, "b1")
	apply(t, s, outcome(failed, driver.Failed))
	live := start(t, s, task("c"))
	write(t, c, live.Ref, -1, "c1")
	time.Sleep(30 * time.Millisecond)
	pp := keep()
	pp.Succeeded, pp.Failed = 10*time.Millisecond, 10*time.Millisecond
	if n := prune(t, s, pp); n < 2 {
		t.Fatalf("pruned %d rows, want both finished jobs", n)
	}
	for _, id := range []int64{done.ID, failed.ID} {
		if !gone(t, s, id) {
			t.Fatalf("job %d survived the prune", id)
		}
		wantLines(t, c, id)
	}
	wantLines(t, c, live.ID, "c1")
}

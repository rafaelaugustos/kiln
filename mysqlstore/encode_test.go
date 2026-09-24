package mysqlstore

import (
	"bytes"
	"context"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/rafaelaugustos/kiln/driver"
)

func TestRender(t *testing.T) {
	at := time.Date(2026, 9, 24, 11, 14, 21, 112181999, time.FixedZone("x", -3*3600))
	cases := []struct {
		got, want string
	}{
		{render("a = ? AND b IN (?)", "plain", []int64{1, 2}), "a = 'plain' AND b IN (1,2)"},
		{render("?", "it's"), "_utf8mb4 X'69742773'"},
		{render("?", `a\b`), "_utf8mb4 X'615c62'"},
		{render("?", "ç"), "_utf8mb4 X'c3a7'"},
		{render("?", []byte{0, 'a'}), "X'0061'"},
		{render("?", []byte(`{"a":1}`)), `'{"a":1}'`},
		{render("?, ?, ?", nil, []byte(nil), time.Time{}), "NULL, NULL, NULL"},
		{render("?", at), "TIMESTAMP '2026-09-24 14:14:21.112181'"},
		{render("? ?", true, raw("NOW()")), "TRUE NOW()"},
		{render("IN (?)", []int64(nil)), "IN (NULL)"},
		{render("? ?", "?", 1), "'?' 1"},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("got %s, want %s", c.got, c.want)
		}
	}
}

func TestStampParse(t *testing.T) {
	for in, want := range map[string]time.Time{
		"2026-09-24 11:14:21":           time.Date(2026, 9, 24, 11, 14, 21, 0, time.UTC),
		"2026-09-24 11:14:21.5":         time.Date(2026, 9, 24, 11, 14, 21, 500000000, time.UTC),
		"2026-09-24 11:14:21.112181":    time.Date(2026, 9, 24, 11, 14, 21, 112181000, time.UTC),
		"1000-01-01 00:00:00.000000":    time.Date(1000, 1, 1, 0, 0, 0, 0, time.UTC),
		"2026-09-24 11:14:21.112181999": time.Date(2026, 9, 24, 11, 14, 21, 112181999, time.UTC),
	} {
		var s stamp
		if err := s.Scan([]byte(in)); err != nil || !s.Time.Equal(want) {
			t.Errorf("%s: %v %v, want %v", in, s.Time, err, want)
		}
	}
	for _, bad := range []string{"", "2026-09-24", "2026-09-24T11:14:21", "2026-0x-24 11:14:21"} {
		var s stamp
		if err := s.Scan([]byte(bad)); err == nil {
			t.Errorf("%q parsed as %v", bad, s.Time)
		}
	}
}

func TestAwkwardValues(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	weird := "quote ' back \\ slash \" nul-free \n tab \t ünïcødé 🚀 ? %s"
	key := []byte{0, '\'', '\\', 0xff, 'x', '?'}
	p := job(weird, func(p *driver.InsertParams) {
		p.Queue = weird
		p.Args = []byte(`{"s":"a\"b'c\\d","n":12345678901234567890123,"f":0.1000000000000000055511151231257827}`)
		p.Meta = map[string]string{weird: weird, "k": `'`}
		p.Tags = []string{weird, "", `"`}
		p.UniqueKey = key
		p.RecurringID = weird
		p.LimitKey = weird
		p.LimitMax = 2
	})
	res := insert(t, s, p)
	if dup := insert(t, s, p)[0]; !dup.Duplicate || dup.ID != res[0].ID {
		t.Fatalf("binary unique key not matched: %+v", dup)
	}
	js := claim(t, s, 1, weird)
	if len(js) != 1 {
		t.Fatalf("claimed %d from an awkward queue", len(js))
	}
	j := js[0]
	switch {
	case j.Kind != weird || j.Queue != weird || j.RecurringID != weird || j.LimitKey != weird:
		t.Fatalf("strings %q %q %q %q", j.Kind, j.Queue, j.RecurringID, j.LimitKey)
	case !bytes.Equal(j.Args, p.Args):
		t.Fatalf("args %s, want exact bytes %s", j.Args, p.Args)
	case !maps.Equal(j.Meta, p.Meta) || !slices.Equal(j.Tags, p.Tags):
		t.Fatalf("meta %v tags %q", j.Meta, j.Tags)
	}
	if err := s.SetMeta(ctx, j.Ref, map[string]string{"x": weird}); err != nil {
		t.Fatal(err)
	}
	out := driver.Outcome{Ref: j.Ref, State: driver.Failed, Reason: "permanent", Error: weird + "\x00", Trace: strings.Repeat("é", 5000),
		Output: []byte(`{"q":"'\\"}`)}
	finish(t, s, out)
	r := record(t, s, j.ID)
	e := r.History[len(r.History)-1]
	if e.Error != weird || len(e.Trace) > 8<<10 || !strings.HasPrefix(e.Trace, "éé") || r.Meta["x"] != weird {
		t.Fatalf("history %+v meta %v", e, r.Meta)
	}
	if n, err := s.Requeue(ctx, driver.Filter{IDs: []int64{j.ID}, Queue: weird}); err != nil || n != 1 {
		t.Fatalf("requeue by awkward queue: %d %v", n, err)
	}
	j = claim(t, s, 1, weird)[0]
	finish(t, s, driver.Outcome{Ref: j.Ref, State: driver.Succeeded, Output: out.Output})
	if r := record(t, s, j.ID); !bytes.Equal(r.Output, out.Output) {
		t.Fatalf("output %s, want %s", r.Output, out.Output)
	}
	if err := s.PauseQueue(ctx, weird, true); err != nil {
		t.Fatal(err)
	}
	qs, err := s.Queues(ctx)
	if err != nil || len(qs) != 1 || qs[0].Name != weird || !qs[0].Paused {
		t.Fatalf("queues %+v %v", qs, err)
	}
}

func TestParseTimeConnection(t *testing.T) {
	t.Parallel()
	loc, err := time.LoadLocation("America/Sao_Paulo")
	if err != nil {
		t.Skip(err)
	}
	db := database(t, func(c *mysql.Config) {
		c.ParseTime, c.Loc, c.InterpolateParams = true, loc, true
	})
	ctx := context.Background()
	s, err := New(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	n0, err := s.Now(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if d := time.Since(n0); d < -time.Minute || d > time.Minute || n0.Location() != time.UTC {
		t.Fatalf("store clock %v is %v away from the local clock", n0, d)
	}
	at := n0.Add(time.Hour).Truncate(time.Microsecond)
	id := insert(t, s, job("a", func(p *driver.InsertParams) { p.RunAt = at }))[0].ID
	r := record(t, s, id)
	if !r.RunAt.Equal(at) || r.State != driver.Scheduled {
		t.Fatalf("run at %v state %s, want %v scheduled", r.RunAt, r.State, at)
	}
	if d := r.CreatedAt.Sub(n0); d < 0 || d > time.Minute {
		t.Fatalf("created at %v, clock %v", r.CreatedAt, n0)
	}
	plain := open(t)
	if _, err := plain.db.ExecContext(ctx, "SELECT 1"); err != nil {
		t.Fatal(err)
	}
	m0, err := plain.Now(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if d := m0.Sub(n0); d < 0 || d > time.Minute {
		t.Fatalf("clocks differ by %v between parseTime and raw connections", d)
	}
}

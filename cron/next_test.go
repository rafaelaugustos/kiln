package cron

import (
	"testing"
	"time"
	_ "time/tzdata"
)

func load(tb testing.TB, name string) *time.Location {
	tb.Helper()
	loc, err := time.LoadLocation(name)
	if err != nil {
		tb.Fatalf("LoadLocation(%q): %v", name, err)
	}
	return loc
}

func at(tb testing.TB, s string, loc *time.Location) time.Time {
	tb.Helper()
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		tb.Fatal(err)
	}
	return t.In(loc)
}

func checkNext(t *testing.T, spec string, loc *time.Location, from string, want ...string) {
	t.Helper()
	s := mustParse(t, spec)
	cur := at(t, from, loc)
	for i, w := range want {
		got := s.Next(cur)
		if w == "" {
			if !got.IsZero() {
				t.Errorf("%q from %s: step %d = %s, want zero", spec, from, i, got.Format(time.RFC3339))
			}
			return
		}
		if g := got.Format(time.RFC3339); g != w {
			t.Errorf("%q in %s from %s: step %d = %s, want %s", spec, loc, from, i, g, w)
			return
		}
		if got.Location() != loc {
			t.Errorf("%q: location %s, want %s", spec, got.Location(), loc)
		}
		if !got.After(cur) {
			t.Errorf("%q: %s is not after %s", spec, got, cur)
		}
		cur = got
	}
}

func TestNext(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		spec string
		from string
		want []string
	}{
		{"* * * * *", "2026-01-01T00:00:00Z", []string{"2026-01-01T00:01:00Z", "2026-01-01T00:02:00Z"}},
		{"* * * * *", "2026-01-01T00:00:59.999Z", []string{"2026-01-01T00:01:00Z"}},
		{"* * * * * *", "2026-01-01T00:00:00.5Z", []string{"2026-01-01T00:00:01Z", "2026-01-01T00:00:02Z"}},
		{"*/20 * * * * *", "2026-01-01T00:00:45Z", []string{"2026-01-01T00:01:00Z", "2026-01-01T00:01:20Z"}},
		{"*/15 * * * *", "2026-01-01T00:07:00Z", []string{
			"2026-01-01T00:15:00Z", "2026-01-01T00:30:00Z", "2026-01-01T00:45:00Z", "2026-01-01T01:00:00Z",
		}},
		{"5/15 * * * *", "2026-01-01T00:50:00Z", []string{"2026-01-01T01:05:00Z", "2026-01-01T01:20:00Z"}},
		{"0 0 * * *", "2026-01-01T00:00:00Z", []string{"2026-01-02T00:00:00Z"}},
		{"0 9-17/4 * * MON-FRI", "2026-01-02T10:00:00Z", []string{
			"2026-01-02T13:00:00Z", "2026-01-02T17:00:00Z", "2026-01-05T09:00:00Z",
		}},
		{"0 0 12 ? * MON-FRI", "2026-01-02T12:00:00Z", []string{"2026-01-05T12:00:00Z"}},
		{"30 0 0 1 1 *", "2026-01-01T00:00:30Z", []string{"2027-01-01T00:00:30Z"}},
		{"0 0 1 */3 *", "2026-02-15T00:00:00Z", []string{
			"2026-04-01T00:00:00Z", "2026-07-01T00:00:00Z", "2026-10-01T00:00:00Z", "2027-01-01T00:00:00Z",
		}},
		{"59 23 31 12 *", "2026-06-01T00:00:00Z", []string{"2026-12-31T23:59:00Z", "2027-12-31T23:59:00Z"}},
		{"0 0 1 1 *", "2026-12-31T23:59:59Z", []string{"2027-01-01T00:00:00Z"}},
		{"0 0 31 * *", "2026-04-01T00:00:00Z", []string{
			"2026-05-31T00:00:00Z", "2026-07-31T00:00:00Z", "2026-08-31T00:00:00Z", "2026-10-31T00:00:00Z",
		}},
		{"0 0 L * *", "2027-01-31T00:00:00Z", []string{"2027-02-28T00:00:00Z", "2027-03-31T00:00:00Z", "2027-04-30T00:00:00Z"}},
		{"0 0 L * *", "2028-01-31T00:00:00Z", []string{"2028-02-29T00:00:00Z"}},
		{"0 0 L 2 *", "2099-03-01T00:00:00Z", []string{"2100-02-28T00:00:00Z", "2101-02-28T00:00:00Z"}},
		{"0 0 L 2 *", "2399-03-01T00:00:00Z", []string{"2400-02-29T00:00:00Z"}},
		{"0 12 L-1 2 *", "2026-01-01T00:00:00Z", []string{
			"2026-02-27T12:00:00Z", "2027-02-27T12:00:00Z", "2028-02-28T12:00:00Z",
		}},
		{"0 0 L-30 * *", "2026-01-15T00:00:00Z", []string{"2026-03-01T00:00:00Z", "2026-05-01T00:00:00Z"}},
		{"0 0 1,L * *", "2026-01-15T00:00:00Z", []string{
			"2026-01-31T00:00:00Z", "2026-02-01T00:00:00Z", "2026-02-28T00:00:00Z", "2026-03-01T00:00:00Z",
		}},
		{"0 0 LW * *", "2026-01-01T00:00:00Z", []string{
			"2026-01-30T00:00:00Z", "2026-02-27T00:00:00Z", "2026-03-31T00:00:00Z", "2026-04-30T00:00:00Z", "2026-05-29T00:00:00Z",
		}},
		{"0 0 15W * *", "2026-07-20T00:00:00Z", []string{
			"2026-08-14T00:00:00Z", "2026-09-15T00:00:00Z", "2026-10-15T00:00:00Z", "2026-11-16T00:00:00Z",
		}},
		{"0 0 1W * *", "2026-07-15T00:00:00Z", []string{
			"2026-08-03T00:00:00Z", "2026-09-01T00:00:00Z", "2026-10-01T00:00:00Z", "2026-11-02T00:00:00Z",
		}},
		{"0 0 31W * *", "2026-01-01T00:00:00Z", []string{
			"2026-01-30T00:00:00Z", "2026-03-31T00:00:00Z", "2026-05-29T00:00:00Z", "2026-07-31T00:00:00Z",
		}},
		{"0 0 30W 2 *", "2026-01-01T00:00:00Z", []string{""}},
		{"0 0 31 2 *", "2026-01-01T00:00:00Z", []string{""}},
		{"0 0 31 4,6,9,11 *", "2026-01-01T00:00:00Z", []string{""}},
		{"0 0 30 2 ?", "2026-01-01T00:00:00Z", []string{""}},
		{"0 0 * * 5L", "2026-01-01T00:00:00Z", []string{
			"2026-01-30T00:00:00Z", "2026-02-27T00:00:00Z", "2026-03-27T00:00:00Z",
		}},
		{"0 0 * * FRI#5", "2026-01-01T00:00:00Z", []string{
			"2026-01-30T00:00:00Z", "2026-05-29T00:00:00Z", "2026-07-31T00:00:00Z",
		}},
		{"0 0 * * MON#1", "2026-01-01T00:00:00Z", []string{
			"2026-01-05T00:00:00Z", "2026-02-02T00:00:00Z", "2026-03-02T00:00:00Z",
		}},
		{"0 0 * 2 MON#5", "2026-01-01T00:00:00Z", []string{""}},
		{"0 0 * 2 MON#5", "2040-01-01T00:00:00Z", []string{"2044-02-29T00:00:00Z"}},
		{"0 0 29 2 *", "2026-01-01T00:00:00Z", []string{"2028-02-29T00:00:00Z", "2032-02-29T00:00:00Z"}},
		{"0 0 29 2 *", "2097-03-01T00:00:00Z", []string{""}},
		{"0 0 29 2 *", "2099-03-01T00:00:00Z", []string{"2104-02-29T00:00:00Z"}},
		{"0 0 13 * FRI", "2026-04-04T00:00:00Z", []string{
			"2026-04-10T00:00:00Z", "2026-04-13T00:00:00Z", "2026-04-17T00:00:00Z",
		}},
		{"0 0 L * 1", "2026-01-26T00:00:00Z", []string{"2026-01-31T00:00:00Z", "2026-02-02T00:00:00Z"}},
		{"0 0 */2 * FRI", "2026-01-01T00:00:00Z", []string{
			"2026-01-09T00:00:00Z", "2026-01-23T00:00:00Z", "2026-02-13T00:00:00Z", "2026-02-27T00:00:00Z",
		}},
		{"0 0 * * 7", "2026-01-01T00:00:00Z", []string{"2026-01-04T00:00:00Z", "2026-01-11T00:00:00Z"}},
		{"0 0 * * 5-7", "2026-01-01T00:00:00Z", []string{
			"2026-01-02T00:00:00Z", "2026-01-03T00:00:00Z", "2026-01-04T00:00:00Z", "2026-01-09T00:00:00Z",
		}},
		{"@every 90s", "2026-01-01T12:00:00.7Z", []string{"2026-01-01T12:01:30Z", "2026-01-01T12:03:00Z"}},
		{"@hourly", "2026-01-01T12:30:00Z", []string{"2026-01-01T13:00:00Z"}},
		{"@daily", "2026-01-01T12:30:00Z", []string{"2026-01-02T00:00:00Z"}},
		{"@weekly", "2026-01-01T12:30:00Z", []string{"2026-01-04T00:00:00Z"}},
		{"@monthly", "2026-01-01T12:30:00Z", []string{"2026-02-01T00:00:00Z"}},
		{"@yearly", "2026-01-01T12:30:00Z", []string{"2027-01-01T00:00:00Z"}},
	} {
		checkNext(t, tc.spec, time.UTC, tc.from, tc.want...)
	}
}

func TestNextLocation(t *testing.T) {
	t.Parallel()
	tokyo := load(t, "Asia/Tokyo")
	checkNext(t, "0 9 * * MON", tokyo, "2026-01-01T00:00:00Z", "2026-01-05T09:00:00+09:00", "2026-01-12T09:00:00+09:00")
	checkNext(t, "@every 1h", tokyo, "2026-01-01T00:00:00Z", "2026-01-01T10:00:00+09:00")
	kathmandu := load(t, "Asia/Kathmandu")
	checkNext(t, "0 * * * *", kathmandu, "2026-01-01T00:00:00Z", "2026-01-01T06:00:00+05:45", "2026-01-01T07:00:00+05:45")
	fixed := time.FixedZone("x", -(3*3600 + 30*60))
	checkNext(t, "0 0 * * *", fixed, "2026-01-01T00:00:00Z", "2026-01-01T00:00:00-03:30", "2026-01-02T00:00:00-03:30")
}

func TestNextDays(t *testing.T) {
	t.Parallel()
	lastDay := func(d time.Time) bool { return d.AddDate(0, 0, 1).Day() == 1 }
	nearest := func(n int) func(time.Time) bool {
		return func(d time.Time) bool {
			x := time.Date(d.Year(), d.Month(), n, 0, 0, 0, 0, time.UTC)
			if x.Month() != d.Month() {
				return false
			}
			switch x.Weekday() {
			case time.Saturday:
				if n == 1 {
					x = x.AddDate(0, 0, 2)
				} else {
					x = x.AddDate(0, 0, -1)
				}
			case time.Sunday:
				if lastDay(x) {
					x = x.AddDate(0, 0, -2)
				} else {
					x = x.AddDate(0, 0, 1)
				}
			}
			return x.Equal(d)
		}
	}
	lastWeekday := func(d time.Time) bool {
		x := time.Date(d.Year(), d.Month()+1, 0, 0, 0, 0, 0, time.UTC)
		for x.Weekday() == time.Saturday || x.Weekday() == time.Sunday {
			x = x.AddDate(0, 0, -1)
		}
		return x.Equal(d)
	}
	nth := func(wd time.Weekday, n int) func(time.Time) bool {
		return func(d time.Time) bool { return d.Weekday() == wd && (d.Day()-1)/7 == n-1 }
	}
	lastOf := func(wd time.Weekday) func(time.Time) bool {
		return func(d time.Time) bool { return d.Weekday() == wd && d.AddDate(0, 0, 7).Month() != d.Month() }
	}
	is := func(wds ...time.Weekday) func(time.Time) bool {
		return func(d time.Time) bool {
			for _, wd := range wds {
				if d.Weekday() == wd {
					return true
				}
			}
			return false
		}
	}
	for _, tc := range []struct {
		spec string
		ok   func(time.Time) bool
	}{
		{"0 0 * * *", func(time.Time) bool { return true }},
		{"0 0 L * *", lastDay},
		{"0 0 L-3 * *", func(d time.Time) bool { return d.AddDate(0, 0, 4).Day() == 1 }},
		{"0 0 L,15 * *", func(d time.Time) bool { return lastDay(d) || d.Day() == 15 }},
		{"0 0 LW * *", lastWeekday},
		{"0 0 1W * *", nearest(1)},
		{"0 0 15W * *", nearest(15)},
		{"0 0 30W * *", nearest(30)},
		{"0 0 31W * *", nearest(31)},
		{"0 0 1W,15W * *", func(d time.Time) bool { return nearest(1)(d) || nearest(15)(d) }},
		{"0 0 * * 5L", lastOf(time.Friday)},
		{"0 0 * * SUNL,MON#1", func(d time.Time) bool { return lastOf(time.Sunday)(d) || nth(time.Monday, 1)(d) }},
		{"0 0 * * WED#3", nth(time.Wednesday, 3)},
		{"0 0 * * SAT#5", nth(time.Saturday, 5)},
		{"0 0 ? * MON-FRI", is(time.Monday, time.Tuesday, time.Wednesday, time.Thursday, time.Friday)},
		{"0 0 * * 6,7", is(time.Saturday, time.Sunday)},
		{"0 0 * * */2", is(time.Sunday, time.Tuesday, time.Thursday, time.Saturday)},
		{"0 0 13 * FRI", func(d time.Time) bool { return d.Day() == 13 || d.Weekday() == time.Friday }},
		{"0 0 1-7 * MON", func(d time.Time) bool { return d.Day() <= 7 || d.Weekday() == time.Monday }},
		{"0 0 */10 * MON", func(d time.Time) bool { return d.Day()%10 == 1 && d.Weekday() == time.Monday }},
		{"0 0 L * FRI#2", func(d time.Time) bool { return lastDay(d) || nth(time.Friday, 2)(d) }},
		{"0 0 LW * 0L", func(d time.Time) bool { return lastWeekday(d) || lastOf(time.Sunday)(d) }},
		{"0 0 29 2 *", func(d time.Time) bool { return d.Month() == 2 && d.Day() == 29 }},
		{"0 0 * 2,8 SUN#2", func(d time.Time) bool {
			return (d.Month() == 2 || d.Month() == 8) && nth(time.Sunday, 2)(d)
		}},
	} {
		s := mustParse(t, tc.spec)
		start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		end := time.Date(2034, 1, 1, 0, 0, 0, 0, time.UTC)
		cur := s.Next(start.Add(-time.Second))
		for d := start; d.Before(end); d = d.AddDate(0, 0, 1) {
			if !tc.ok(d) {
				continue
			}
			if !cur.Equal(d) {
				t.Errorf("%q: got %s, want %s", tc.spec, cur.Format(time.DateOnly), d.Format(time.DateOnly))
				break
			}
			cur = s.Next(cur)
		}
	}
}

func TestNextAllocs(t *testing.T) {
	ny := load(t, "America/New_York")
	lh := load(t, "Australia/Lord_Howe")
	for _, spec := range []string{"*/5 * * * *", "0 30 2 * * *", "0 0 L * *", "0 0 15W,LW * *", "0 0 * * 5L,MON#2", "0 0 29 2 *", "@every 1m"} {
		s := mustParse(t, spec)
		for _, from := range []time.Time{
			time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2026, 3, 7, 12, 0, 0, 0, ny),
			time.Date(2026, 10, 31, 12, 0, 0, 0, ny),
			time.Date(2026, 4, 4, 12, 0, 0, 0, lh),
		} {
			if n := testing.AllocsPerRun(100, func() { s.Next(from) }); n != 0 {
				t.Errorf("%q from %s: %v allocs per Next", spec, from, n)
			}
		}
	}
}

func TestCalendar(t *testing.T) {
	t.Parallel()
	for z := epochDays(-1000, 1, 1); z < epochDays(3000, 1, 1); z++ {
		y, m, d := civil(z)
		ref := time.Unix(z*day, 0).UTC()
		if y != ref.Year() || m != int(ref.Month()) || d != ref.Day() {
			t.Fatalf("civil(%d) = %d-%d-%d, want %s", z, y, m, d, ref.Format(time.DateOnly))
		}
		if back := epochDays(y, m, d); back != z {
			t.Fatalf("epochDays(%d, %d, %d) = %d, want %d", y, m, d, back, z)
		}
		if n := daysIn(y, m); d == 1 && ref.AddDate(0, 0, n).Day() != 1 {
			t.Fatalf("daysIn(%d, %d) = %d", y, m, n)
		}
	}
}

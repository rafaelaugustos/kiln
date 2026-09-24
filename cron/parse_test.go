package cron

import (
	"errors"
	"strings"
	"testing"

	"github.com/rafaelaugustos/kiln/driver"
)

func mustParse(tb testing.TB, spec string) *Schedule {
	tb.Helper()
	s, err := Parse(spec)
	if err != nil {
		tb.Fatalf("Parse(%q): %v", spec, err)
	}
	return s
}

func TestParseErrors(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		spec string
		want string
	}{
		{"", "empty spec"},
		{"   ", "empty spec"},
		{"* * * *", "expected 5 or 6 fields, found 4"},
		{"* * * * * * *", "expected 5 or 6 fields, found 7"},
		{"60 * * * *", "minute: 60 out of range 0-59"},
		{"60 * * * * *", "second: 60 out of range 0-59"},
		{"* 24 * * *", "hour: 24 out of range 0-23"},
		{"* * 0 * *", "day of month: 0 out of range 1-31"},
		{"* * 32 * *", "day of month: 32 out of range 1-31"},
		{"* * * 0 *", "month: 0 out of range 1-12"},
		{"* * * 13 *", "month: 13 out of range 1-12"},
		{"* * * * 8", "day of week: 8 out of range 0-7"},
		{"*/0 * * * *", `minute: invalid step in "*/0"`},
		{"*/60 * * * *", `minute: invalid step in "*/60"`},
		{"* */24 * * *", `hour: invalid step in "*/24"`},
		{"*/2/3 * * * *", `minute: invalid step in "*/2/3"`},
		{"*/x * * * *", `minute: invalid step in "*/X"`},
		{"5-1 * * * *", "minute: range 5-1 is backwards"},
		{"* * * * FRI-MON", "day of week: range FRI-MON is backwards"},
		{"1-2-3 * * * *", `minute: invalid value "2-3"`},
		{"-1 * * * *", `minute: invalid value ""`},
		{"+1 * * * *", `minute: invalid value "+1"`},
		{"1,,2 * * * *", `minute: invalid value ""`},
		{"1, * * * *", `minute: invalid value ""`},
		{"a * * * *", `minute: invalid value "A"`},
		{"*-5 * * * *", `minute: invalid value "*"`},
		{"1234567890 * * * *", `minute: invalid value "1234567890"`},
		{"? * * * *", `minute: invalid value "?"`},
		{"* ? * * *", `hour: invalid value "?"`},
		{"* * * ? *", `month: invalid value "?"`},
		{"* * ?/2 * *", `day of month: invalid value "?"`},
		{"* * * FOO *", `month: invalid value "FOO"`},
		{"* * * * FOO", `day of week: invalid value "FOO"`},
		{"* * * JAN-FOO *", `month: invalid value "FOO"`},
		{"L * * * *", `minute: invalid value "L"`},
		{"* L * * *", `hour: invalid value "L"`},
		{"* * * L *", `month: invalid value "L"`},
		{"* * L-0 * *", `day of month: invalid offset in "L-0"`},
		{"* * L-31 * *", `day of month: invalid offset in "L-31"`},
		{"* * L-x * *", `day of month: invalid offset in "L-X"`},
		{"* * L-2W * *", `day of month: invalid offset in "L-2W"`},
		{"* * 0W * *", "day of month: 0 out of range 1-31"},
		{"* * 32W * *", "day of month: 32 out of range 1-31"},
		{"* * W * *", `day of month: invalid value ""`},
		{"* * 1-5W * *", `day of month: invalid value "1-5"`},
		{"* * 1#2 * *", `day of month: invalid value "1#2"`},
		{"* * * * 1W", `day of week: invalid value "1W"`},
		{"* * * * L", `day of week: invalid value ""`},
		{"* * * * 8L", "day of week: 8 out of range 0-7"},
		{"* * * * 1-5L", `day of week: invalid value "1-5"`},
		{"* * * * MON#0", `day of week: invalid occurrence in "MON#0"`},
		{"* * * * MON#6", `day of week: invalid occurrence in "MON#6"`},
		{"* * * * MON#", `day of week: invalid occurrence in "MON#"`},
		{"* * * * #2", `day of week: invalid value ""`},
		{"* * * * 1#2#3", `day of week: invalid occurrence in "1#2#3"`},
		{"@", "unknown descriptor @"},
		{"@fortnightly", "unknown descriptor @fortnightly"},
		{"@daily 1", "@daily takes no arguments"},
		{"@every", "@every takes one duration"},
		{"@every 1m 2m", "@every takes one duration"},
		{"@every x", `time: invalid duration "x"`},
		{"@every 0s", "interval 0s is shorter than 1s"},
		{"@every 999ms", "interval 999ms is shorter than 1s"},
		{"@every -5m", "interval -5m is shorter than 1s"},
	} {
		s, err := Parse(tc.spec)
		if err == nil {
			t.Errorf("Parse(%q) = %q, want error", tc.spec, s)
			continue
		}
		if !errors.Is(err, driver.ErrInvalid) {
			t.Errorf("Parse(%q) error %v does not wrap ErrInvalid", tc.spec, err)
		}
		if !strings.HasSuffix(err.Error(), ": "+tc.want) || !strings.Contains(err.Error(), "cron ") {
			t.Errorf("Parse(%q) error %q, want suffix %q", tc.spec, err, tc.want)
		}
	}
}

func TestParseString(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		spec string
		want string
	}{
		{"* * * * *", "* * * * *"},
		{"*/5 * * * *", "*/5 * * * *"},
		{"0 */5 * * * *", "*/5 * * * *"},
		{"00 0 * * * *", "0 * * * *"},
		{"30 * * * * *", "30 * * * * *"},
		{"0,30 * * * * *", "0,30 * * * * *"},
		{"  0\t9  * *   mon-fri  ", "0 9 * * MON-FRI"},
		{"05 09 01 01 *", "5 9 1 1 *"},
		{"0 0 12 ? * mon#2", "0 12 * * MON#2"},
		{"0 0 ? * sun", "0 0 * * SUN"},
		{"0 0 l * *", "0 0 L * *"},
		{"0 0 l-03 * *", "0 0 L-3 * *"},
		{"0 0 lw * *", "0 0 LW * *"},
		{"0 0 015w * *", "0 0 15W * *"},
		{"0 0 1,15,L * *", "0 0 1,15,L * *"},
		{"0 0 * * 5l", "0 0 * * 5L"},
		{"0 0 * * fril,mon#1", "0 0 * * FRIL,MON#1"},
		{"0 0 * * 7", "0 0 * * 7"},
		{"0 0 1 jan-jun/2 *", "0 0 1 JAN-JUN/2 *"},
		{"0 9-17/2 * * 1-5", "0 9-17/2 * * 1-5"},
		{"0 3/4 * * *", "0 3/4 * * *"},
		{"@yearly", "@yearly"},
		{"@ANNUALLY", "@yearly"},
		{"@monthly", "@monthly"},
		{"@Weekly", "@weekly"},
		{"@daily", "@daily"},
		{"@midnight", "@daily"},
		{"@hourly", "@hourly"},
		{"@every 90s", "@every 1m30s"},
		{"@every 1.5s", "@every 1s"},
		{"@EVERY 1h", "@every 1h0m0s"},
	} {
		s := mustParse(t, tc.spec)
		if got := s.String(); got != tc.want {
			t.Errorf("Parse(%q).String() = %q, want %q", tc.spec, got, tc.want)
		}
		again := mustParse(t, s.String())
		if *again != *s {
			t.Errorf("Parse(%q) and Parse(%q) differ", tc.spec, s.String())
		}
	}
}

func TestParseEquivalent(t *testing.T) {
	t.Parallel()
	for _, pair := range [][2]string{
		{"0 0 * * 7", "0 0 * * 0"},
		{"0 0 * * SUN", "0 0 * * 0"},
		{"0 0 * * 0-7", "0 0 * * *"},
		{"0 0 * * MON-FRI", "0 0 * * 1-5"},
		{"0 0 * * 5-7", "0 0 * * 0,5,6"},
		{"0 0 * JAN,MAR *", "0 0 * 1,3 *"},
		{"0 0 * * 7#2", "0 0 * * 0#2"},
		{"0 0 * * 7L", "0 0 * * SUNL"},
		{"0 0 ? * *", "0 0 * * ?"},
		{"@daily", "0 0 * * *"},
		{"@hourly", "0 * * * *"},
		{"@weekly", "0 0 * * 0"},
		{"@monthly", "0 0 1 * *"},
		{"@yearly", "0 0 1 1 *"},
		{"0 */20 * * * *", "0 0,20,40 * * * *"},
		{"*/10 * * * *", "0-59/10 * * * *"},
		{"5/15 * * * *", "5-59/15 * * * *"},
	} {
		a, b := *mustParse(t, pair[0]), *mustParse(t, pair[1])
		a.spec, b.spec = "", ""
		a.interval, b.interval = false, false
		a.domStar, b.domStar = false, false
		a.dowStar, b.dowStar = false, false
		if a != b {
			t.Errorf("Parse(%q) and Parse(%q) select different times", pair[0], pair[1])
		}
	}
}

func TestParseFlags(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		spec             string
		interval         bool
		domStar, dowStar bool
	}{
		{"0 2 * * *", false, true, true},
		{"0 * * * *", true, true, true},
		{"0 */2 * * *", true, true, true},
		{"0 1-23/2 * * *", true, true, true},
		{"0 0-23 * * *", false, true, true},
		{"*/5 1 * * *", false, true, true},
		{"0 9,17 * * *", false, true, true},
		{"0 0 1 * MON", false, false, false},
		{"0 0 */2 * MON", false, true, false},
		{"0 0 ? * MON", false, true, false},
		{"0 0 1 * ?", false, false, true},
		{"0 0 L * *", false, false, true},
		{"@hourly", true, true, true},
		{"@daily", false, true, true},
	} {
		s := mustParse(t, tc.spec)
		if s.interval != tc.interval || s.domStar != tc.domStar || s.dowStar != tc.dowStar {
			t.Errorf("Parse(%q): interval=%v domStar=%v dowStar=%v, want %v %v %v",
				tc.spec, s.interval, s.domStar, s.dowStar, tc.interval, tc.domStar, tc.dowStar)
		}
	}
}

func TestParseHangfire(t *testing.T) {
	t.Parallel()
	for _, spec := range []string{
		"* * * * *", "0 * * * *", "15 * * * *", "0 0 * * *", "30 3 * * *", "0 0 * * 1", "0 9 * * 5",
		"0 0 1 * *", "0 9 15 * *", "0 0 1 1 *", "0 9 25 12 *", "0 0 31 2 *", "*/5 * * * *", "0 */2 * * *",
		"0 0 */3 * *", "0 0 1 */2 *", "*/30 * * * * *", "0 0 12 ? * MON-FRI", "0 15 10 L * ?", "0 15 10 ? * 6#3",
	} {
		mustParse(t, spec)
	}
}

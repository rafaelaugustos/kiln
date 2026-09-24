package cron

import (
	"errors"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

func FuzzParse(f *testing.F) {
	for _, spec := range []string{
		"* * * * *", "*/5 * * * *", "0 0 * * * *", "0 9-17/2 * * MON-FRI", "0 0 12 ? * MON#2",
		"0 0 L * *", "0 0 L-3 * *", "0 0 LW * *", "0 0 15W * *", "0 0 * * 5L", "0 0 1,15,L * FRI#5",
		"0 0 29 2 *", "0 0 30 2 *", "0 0 * * 7", "5/15 1-3 * JAN-JUN/2 0-7", "@daily", "@every 90s",
		"@every 1h30m", "@annually", "a b c d e", "1-2-3 * * * *", "*/0 * * * *", "L-W * * * *",
	} {
		f.Add(spec)
	}
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		f.Fatal(err)
	}
	lh, err := time.LoadLocation("Australia/Lord_Howe")
	if err != nil {
		f.Fatal(err)
	}
	starts := []time.Time{
		time.Date(2026, 3, 8, 1, 30, 0, 0, ny),
		time.Date(2026, 11, 1, 1, 30, 0, 0, ny),
		time.Date(2026, 10, 4, 1, 45, 0, 0, lh),
		time.Date(2028, 2, 28, 23, 59, 59, 999, time.UTC),
	}
	f.Fuzz(func(t *testing.T, spec string) {
		s, err := Parse(spec)
		if err != nil {
			if !errors.Is(err, driver.ErrInvalid) {
				t.Fatalf("Parse(%q): %v does not wrap ErrInvalid", spec, err)
			}
			return
		}
		again, err := Parse(s.String())
		if err != nil {
			t.Fatalf("Parse(%q) normalized to %q which fails: %v", spec, s, err)
		}
		if *again != *s {
			t.Fatalf("Parse(%q) normalized to %q which parses differently", spec, s)
		}
		for _, from := range starts {
			cur := from
			for range 3 {
				next := s.Next(cur)
				if next.IsZero() {
					break
				}
				if !next.After(cur) {
					t.Fatalf("%q: Next(%s) = %s", spec, cur, next)
				}
				if next.Location() != from.Location() {
					t.Fatalf("%q: Next returned location %s", spec, next.Location())
				}
				cur = next
			}
		}
	})
}

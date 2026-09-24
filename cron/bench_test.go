package cron

import (
	"testing"
	"time"
)

func BenchmarkParse(b *testing.B) {
	for _, bc := range []struct{ name, spec string }{
		{"simple", "*/5 * * * *"},
		{"complex", "0 30 9-17/2 ? JAN-JUN MON-FRI"},
		{"special", "0 0 L-2,15W * FRI#3,5L"},
		{"descriptor", "@daily"},
		{"every", "@every 1h30m"},
	} {
		b.Run(bc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := Parse(bc.spec); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkNext(b *testing.B) {
	ny := load(b, "America/New_York")
	for _, bc := range []struct {
		name string
		spec string
		from time.Time
	}{
		{"minutely/UTC", "* * * * *", time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"every5m/NY", "*/5 * * * *", time.Date(2026, 1, 1, 0, 0, 0, 0, ny)},
		{"weekdays/UTC", "0 9 * * MON-FRI", time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"weekdays/NY", "0 9 * * MON-FRI", time.Date(2026, 1, 1, 0, 0, 0, 0, ny)},
		{"lastFriday/UTC", "0 0 * * 5L", time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"lastWeekday/NY", "0 18 LW * *", time.Date(2026, 1, 1, 0, 0, 0, 0, ny)},
		{"gap/NY", "30 2 * * *", time.Date(2026, 3, 7, 12, 0, 0, 0, ny)},
		{"leapDay/UTC", "0 0 29 2 *", time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"never/NY", "0 0 * 2 MON#5", time.Date(2026, 1, 1, 0, 0, 0, 0, ny)},
		{"every/UTC", "@every 90s", time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)},
	} {
		b.Run(bc.name, func(b *testing.B) {
			s := mustParse(b, bc.spec)
			end := bc.from.AddDate(10, 0, 0)
			cur := bc.from
			b.ReportAllocs()
			for b.Loop() {
				cur = s.Next(cur)
				if cur.IsZero() || cur.After(end) {
					cur = bc.from
				}
			}
		})
	}
}

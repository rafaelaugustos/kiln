package cron

import (
	"math/bits"
	"time"
)

const (
	day     = 24 * 60 * 60
	horizon = (5*365 + 2) * day
)

func (s *Schedule) Next(t time.Time) time.Time {
	if s.every > 0 {
		return t.Truncate(time.Second).Add(s.every)
	}
	_, off := t.Zone()
	from := t.Unix() + int64(off) + 1
	limit := from + horizon
	for seg := t; ; {
		start, end := seg.ZoneBounds()
		lo, hi := from, limit
		if !s.interval && !start.IsZero() {
			if _, prev := start.Add(-time.Second).Zone(); prev > off {
				lo = max(lo, start.Unix()+int64(prev))
			}
		}
		if !end.IsZero() {
			hi = min(hi, end.Unix()+int64(off))
		}
		if w, ok := s.find(lo, hi); ok {
			return time.Unix(w-int64(off), 0).In(t.Location())
		}
		if hi == limit {
			return time.Time{}
		}
		_, next := end.Zone()
		if !s.interval && next > off {
			if _, ok := s.find(hi, end.Unix()+int64(next)); ok {
				return end
			}
		}
		seg, off, from = end, next, end.Unix()+int64(next)
	}
}

func (s *Schedule) find(lo, hi int64) (int64, bool) {
	if lo >= hi {
		return 0, false
	}
	z := floorDiv(lo, day)
	y, m, d := civil(z)
	r := int(lo - z*day)
	h, mi, sec := r/3600, r/60%60, r%60
	var (
		cy, cm int
		base   int64
		mask   uint64
	)
	for {
		if b := s.month >> m << m; b == 0 {
			y, m, d, h, mi, sec = y+1, bits.TrailingZeros64(s.month), 1, 0, 0, 0
		} else if n := bits.TrailingZeros64(b); n != m {
			m, d, h, mi, sec = n, 1, 0, 0, 0
		}
		if y != cy || m != cm {
			if base = epochDays(y, m, 1); base*day >= hi {
				return 0, false
			}
			cy, cm, mask = y, m, s.dayMask(y, m, base)
		}
		b := mask >> d << d
		if b == 0 {
			m, d, h, mi, sec = m+1, 1, 0, 0, 0
			if m > 12 {
				y, m = y+1, 1
			}
			continue
		}
		if n := bits.TrailingZeros64(b); n != d {
			d, h, mi, sec = n, 0, 0, 0
		}
		if b = s.hour >> h << h; b == 0 {
			d, h, mi, sec = d+1, 0, 0, 0
			continue
		}
		if n := bits.TrailingZeros64(b); n != h {
			h, mi, sec = n, 0, 0
		}
		if b = s.minute >> mi << mi; b == 0 {
			h, mi, sec = h+1, 0, 0
			continue
		}
		if n := bits.TrailingZeros64(b); n != mi {
			mi, sec = n, 0
		}
		if b = s.second >> sec << sec; b == 0 {
			mi, sec = mi+1, 0
			continue
		}
		sec = bits.TrailingZeros64(b)
		w := (base+int64(d-1))*day + int64(h*3600+mi*60+sec)
		if w >= hi {
			return 0, false
		}
		return w, true
	}
}

func (s *Schedule) dayMask(y, m int, first int64) uint64 {
	n := daysIn(y, m)
	wd := int((first%7 + 11) % 7)
	dom := s.dom | bits.Reverse64(s.last)>>(63-n)
	for b := s.near; b != 0; b &= b - 1 {
		if d := bits.TrailingZeros64(b); d <= n {
			dom |= 1 << nearest(d, n, wd)
		}
	}
	if s.lw {
		dom |= 1 << nearest(n, n, wd)
	}
	dow := s.week[wd] | s.lastWeek[wd]>>(n-6)<<(n-6)
	all := uint64(1)<<(n+1) - 2
	if s.domStar || s.dowStar {
		return dom & dow & all
	}
	return (dom | dow) & all
}

func nearest(d, n, first int) int {
	switch (first + d - 1) % 7 {
	case 6:
		if d == 1 {
			return 3
		}
		return d - 1
	case 0:
		if d == n {
			return d - 2
		}
		return d + 1
	}
	return d
}

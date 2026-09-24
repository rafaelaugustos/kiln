package cron

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
)

type field struct {
	name   string
	lo, hi int
	names  []string
}

var (
	seconds   = field{name: "second", hi: 59}
	minutes   = field{name: "minute", hi: 59}
	hours     = field{name: "hour", hi: 23}
	monthDays = field{name: "day of month", lo: 1, hi: 31}
	months    = field{name: "month", lo: 1, hi: 12, names: []string{
		"JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC",
	}}
	weekdays = field{name: "day of week", hi: 7, names: []string{
		"SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT",
	}}
)

type parser struct {
	s     *Schedule
	text  []byte
	nth   [7]uint64
	lastW uint64
}

func parseFields(f []string) (*Schedule, error) {
	p := parser{s: &Schedule{
		domStar:  f[3][0] == '*' || f[3][0] == '?',
		dowStar:  f[5][0] == '*' || f[5][0] == '?',
		interval: strings.ContainsAny(f[2], "*/"),
	}}
	var sets [6]uint64
	for i, fd := range [...]*field{&seconds, &minutes, &hours, &monthDays, &months, &weekdays} {
		if i > 0 {
			p.text = append(p.text, ' ')
		}
		set, err := p.field(fd, strings.ToUpper(f[i]))
		if err != nil {
			return nil, err
		}
		sets[i] = set
	}
	s := p.s
	s.second, s.minute, s.hour, s.dom, s.month = sets[0], sets[1], sets[2], sets[3], sets[4]
	week := (sets[5] | sets[5]>>7) & 0x7f
	for first := range 7 {
		for d := 1; d <= 31; d++ {
			wd := (first + d - 1) % 7
			if week>>wd&1 != 0 || p.nth[wd]>>((d+6)/7)&1 != 0 {
				s.week[first] |= 1 << d
			}
			if p.lastW>>wd&1 != 0 {
				s.lastWeek[first] |= 1 << d
			}
		}
	}
	s.spec = strings.TrimPrefix(string(p.text), "0 ")
	return s, nil
}

func (p *parser) field(f *field, spec string) (uint64, error) {
	var set uint64
	for i, item := range strings.Split(spec, ",") {
		if i > 0 {
			p.text = append(p.text, ',')
		}
		v, err := p.item(f, item)
		if err != nil {
			return 0, fmt.Errorf("%s: %v", f.name, err)
		}
		set |= v
	}
	return set, nil
}

func (p *parser) item(f *field, item string) (uint64, error) {
	switch f {
	case &monthDays:
		return p.day(item)
	case &weekdays:
		return p.weekday(item)
	}
	return p.span(f, item)
}

func (p *parser) day(item string) (uint64, error) {
	switch {
	case item == "?":
		return p.span(&monthDays, "*")
	case item == "L":
		p.s.last |= 1
	case item == "LW":
		p.s.lw = true
	case strings.HasPrefix(item, "L-"):
		n, ok := atoi(item[2:])
		if !ok || n < 1 || n > 30 {
			return 0, fmt.Errorf("invalid offset in %q", item)
		}
		p.s.last |= 1 << n
		item = "L-" + strconv.Itoa(n)
	case strings.HasSuffix(item, "W"):
		n, tok, err := monthDays.value(item[:len(item)-1])
		if err != nil {
			return 0, err
		}
		p.s.near |= 1 << n
		item = tok + "W"
	default:
		return p.span(&monthDays, item)
	}
	p.text = append(p.text, item...)
	return 0, nil
}

func (p *parser) weekday(item string) (uint64, error) {
	switch {
	case item == "?":
		return p.span(&weekdays, "*")
	case strings.HasSuffix(item, "L"):
		v, tok, err := weekdays.value(item[:len(item)-1])
		if err != nil {
			return 0, err
		}
		p.lastW |= 1 << (v % 7)
		item = tok + "L"
	case strings.Contains(item, "#"):
		d, k, _ := strings.Cut(item, "#")
		v, tok, err := weekdays.value(d)
		if err != nil {
			return 0, err
		}
		n, ok := atoi(k)
		if !ok || n < 1 || n > 5 {
			return 0, fmt.Errorf("invalid occurrence in %q", item)
		}
		p.nth[v%7] |= 1 << n
		item = tok + "#" + strconv.Itoa(n)
	default:
		return p.span(&weekdays, item)
	}
	p.text = append(p.text, item...)
	return 0, nil
}

func (p *parser) span(f *field, item string) (uint64, error) {
	rng, step, stepped := strings.Cut(item, "/")
	lo, hi := f.lo, f.hi
	if rng == "*" {
		p.text = append(p.text, '*')
	} else {
		a, b, ranged := strings.Cut(rng, "-")
		v, tok, err := f.value(a)
		if err != nil {
			return 0, err
		}
		lo = v
		p.text = append(p.text, tok...)
		if ranged {
			if hi, tok, err = f.value(b); err != nil {
				return 0, err
			}
			if hi < lo {
				return 0, fmt.Errorf("range %s is backwards", rng)
			}
			p.text = append(append(p.text, '-'), tok...)
		} else if !stepped {
			hi = lo
		}
	}
	n := 1
	if stepped {
		var ok bool
		if n, ok = atoi(step); !ok || n < 1 || n > f.hi {
			return 0, fmt.Errorf("invalid step in %q", item)
		}
		p.text = strconv.AppendInt(append(p.text, '/'), int64(n), 10)
	}
	var set uint64
	for v := lo; v <= hi; v += n {
		set |= 1 << v
	}
	return set, nil
}

func (f *field) value(s string) (int, string, error) {
	if i := slices.Index(f.names, s); i >= 0 {
		return f.lo + i, s, nil
	}
	v, ok := atoi(s)
	if !ok {
		return 0, "", fmt.Errorf("invalid value %q", s)
	}
	if v < f.lo || v > f.hi {
		return 0, "", fmt.Errorf("%d out of range %d-%d", v, f.lo, f.hi)
	}
	return v, strconv.Itoa(v), nil
}

func atoi(s string) (int, bool) {
	if s == "" || len(s) > 9 {
		return 0, false
	}
	n := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int(c-'0')
	}
	return n, true
}

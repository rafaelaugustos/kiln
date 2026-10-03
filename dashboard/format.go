package dashboard

import (
	"bytes"
	"encoding/json"
	"html/template"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/rafaelaugustos/kiln/driver"
)

func integer(v any) int64 {
	switch v := v.(type) {
	case int:
		return int64(v)
	case int64:
		return v
	}
	return 0
}

func (lc *locale) num(v any) string {
	n := integer(v)
	s := strconv.FormatInt(n, 10)
	neg := n < 0
	if neg {
		s = s[1:]
	}
	var b strings.Builder
	if neg {
		b.WriteByte('-')
	}
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			b.WriteString(lc.group)
		}
		b.WriteRune(c)
	}
	return b.String()
}

func (lc *locale) short(n int64) string {
	switch {
	case n >= 1e6:
		return lc.text("count.million", "n", lc.tenths(float64(n)/1e6))
	case n >= 1e5:
		return lc.text("count.thousand", "n", lc.tenths(float64(n)/1e3))
	}
	return lc.num(n)
}

func (lc *locale) tenths(x float64) string {
	return lc.decimal(math.Round(x*10)/10, -1)
}

func (lc *locale) decimal(x float64, prec int) string {
	return strings.Replace(strconv.FormatFloat(x, 'f', prec, 64), ".", lc.point, 1)
}

func (lc *locale) ago(t time.Time) string {
	d := time.Since(t)
	switch {
	case d > -time.Second && d < time.Second:
		return lc.text("time.now")
	case d < 0:
		return lc.text("time.in", "t", lc.span(-d))
	}
	return lc.text("time.ago", "t", lc.span(d))
}

func (lc *locale) span(d time.Duration) string {
	switch {
	case d < time.Minute:
		return lc.unit("unit.s", d/time.Second)
	case d < time.Hour:
		return lc.unit("unit.m", d/time.Minute)
	case d < 48*time.Hour:
		return lc.unit("unit.h", d/time.Hour)
	}
	return lc.unit("unit.d", d/(24*time.Hour))
}

func (lc *locale) unit(key string, n time.Duration) string {
	return lc.text(key, "n", strconv.Itoa(int(n)))
}

func (lc *locale) dur(d time.Duration) string {
	switch {
	case d <= 0:
		return lc.unit("unit.s", 0)
	case d < time.Second:
		return lc.unit("unit.ms", d/time.Millisecond)
	case d < 10*time.Second:
		return lc.text("unit.s", "n", lc.decimal(d.Seconds(), 1))
	case d < time.Minute:
		return lc.unit("unit.s", d/time.Second)
	case d < time.Hour:
		return lc.pair(d, time.Minute, "unit.m", time.Second, "unit.s")
	case d < 24*time.Hour:
		return lc.pair(d, time.Hour, "unit.h", time.Minute, "unit.m")
	}
	return lc.pair(d, 24*time.Hour, "unit.d", time.Hour, "unit.h")
}

func (lc *locale) pair(d, big time.Duration, bk string, small time.Duration, sk string) string {
	s := lc.unit(bk, d/big)
	if r := d % big / small; r > 0 {
		s += " " + lc.unit(sk, r)
	}
	return s
}

func (lc *locale) abs(t time.Time) string {
	s := t.UTC().Format("2006-01-02 15:04:05")
	return lc.text("date.full", "y", s[:4], "m", s[5:7], "d", s[8:10], "time", s[11:])
}

func (lc *locale) stamp(t time.Time) string {
	u := t.UTC()
	return lc.text("date.stamp", "month", lc.months[u.Month()-1], "d", strconv.Itoa(u.Day()), "time", u.Format("15:04"))
}

func iso(t time.Time) string {
	return t.UTC().Format(time.RFC3339)
}

func clock(t time.Time) string {
	return t.UTC().Format("15:04:05")
}

func (lc *locale) label(s driver.State, form ...string) string {
	k := "state." + string(s)
	for _, f := range form {
		k += "." + f
	}
	return lc.text(k)
}

func initial(s string) string {
	r, _ := utf8.DecodeRuneInString(s)
	return strings.ToUpper(string(r))
}

func preview(v json.RawMessage) string {
	const limit = 160
	var b bytes.Buffer
	if json.Compact(&b, v) != nil {
		b.Reset()
		b.Write(v)
	}
	s := b.String()
	if s == "{}" || s == "null" {
		return ""
	}
	if len(s) <= limit {
		return s
	}
	n := limit
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n] + "…"
}

func pretty(v json.RawMessage) template.HTML {
	var b bytes.Buffer
	if json.Indent(&b, v, "", "  ") != nil {
		return template.HTML(template.HTMLEscapeString(string(v)))
	}
	s := b.Bytes()
	if len(s) > 256<<10 {
		return template.HTML(template.HTMLEscapeString(string(s)))
	}
	var out strings.Builder
	out.Grow(len(s) * 2)
	tok := func(class string, t []byte) {
		out.WriteString(`<span class="`)
		out.WriteString(class)
		out.WriteString(`">`)
		template.HTMLEscape(&out, t)
		out.WriteString("</span>")
	}
	for i := 0; i < len(s); {
		switch c := s[i]; {
		case c == '"':
			j := i + 1
			for s[j] != '"' {
				if s[j] == '\\' {
					j++
				}
				j++
			}
			j++
			if j < len(s) && s[j] == ':' {
				tok("k", s[i:j])
			} else {
				tok("s", s[i:j])
			}
			i = j
		case c == '-' || '0' <= c && c <= '9':
			j := i + 1
			for j < len(s) && strings.IndexByte("+-.0123456789eE", s[j]) >= 0 {
				j++
			}
			tok("n", s[i:j])
			i = j
		case c == 't' || c == 'f' || c == 'n':
			j := i + 1
			for j < len(s) && 'a' <= s[j] && s[j] <= 'z' {
				j++
			}
			tok("b", s[i:j])
			i = j
		default:
			out.WriteByte(c)
			i++
		}
	}
	return template.HTML(out.String())
}

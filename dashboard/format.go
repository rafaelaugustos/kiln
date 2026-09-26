package dashboard

import (
	"bytes"
	"encoding/json"
	"html/template"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/rafaelaugustos/kiln/driver"
)

func num(v any) string {
	var n int64
	switch v := v.(type) {
	case int:
		n = int64(v)
	case int64:
		n = v
	}
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
			b.WriteByte(',')
		}
		b.WriteRune(c)
	}
	return b.String()
}

func ago(t time.Time) string {
	d := time.Since(t)
	switch {
	case d > -time.Second && d < time.Second:
		return "now"
	case d < 0:
		return "in " + span(-d)
	}
	return span(d) + " ago"
}

func span(d time.Duration) string {
	switch {
	case d < time.Minute:
		return strconv.Itoa(int(d/time.Second)) + "s"
	case d < time.Hour:
		return strconv.Itoa(int(d/time.Minute)) + "m"
	case d < 48*time.Hour:
		return strconv.Itoa(int(d/time.Hour)) + "h"
	}
	return strconv.Itoa(int(d/(24*time.Hour))) + "d"
}

func dur(d time.Duration) string {
	switch {
	case d <= 0:
		return "0s"
	case d < time.Second:
		return strconv.Itoa(int(d/time.Millisecond)) + "ms"
	case d < 10*time.Second:
		return strconv.FormatFloat(d.Seconds(), 'f', 1, 64) + "s"
	case d < time.Minute:
		return strconv.Itoa(int(d/time.Second)) + "s"
	case d < time.Hour:
		return pair(d, time.Minute, "m", time.Second, "s")
	case d < 24*time.Hour:
		return pair(d, time.Hour, "h", time.Minute, "m")
	}
	return pair(d, 24*time.Hour, "d", time.Hour, "h")
}

func pair(d, big time.Duration, bs string, small time.Duration, ss string) string {
	s := strconv.Itoa(int(d/big)) + bs
	if r := d % big / small; r > 0 {
		s += " " + strconv.Itoa(int(r)) + ss
	}
	return s
}

func abs(t time.Time) string {
	return t.UTC().Format("2006-01-02 15:04:05 UTC")
}

func stamp(t time.Time) string {
	return t.UTC().Format("Jan 2 15:04 UTC")
}

func iso(t time.Time) string {
	return t.UTC().Format(time.RFC3339)
}

func label(s driver.State) string {
	if s == "" {
		return ""
	}
	return strings.ToUpper(string(s[:1])) + string(s[1:])
}

func plural(n any, one, other string) string {
	switch n {
	case 1, int64(1):
		return one
	}
	return other
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

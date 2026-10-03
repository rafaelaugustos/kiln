package mssqlstore

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/golang-sql/civil"
	"github.com/rafaelaugustos/kiln/driver"
)

const (
	hexDigits = "0123456789abcdef"
	isoLayout = "2006-01-02T15:04:05.0000000"
)

func appendJSONString(b []byte, s string) []byte {
	b = append(b, '"')
	start := 0
	for i := 0; i < len(s); {
		c := s[i]
		if c < utf8.RuneSelf {
			if c >= 0x20 && c != '"' && c != '\\' {
				i++
				continue
			}
			b = append(b, s[start:i]...)
			switch c {
			case '"', '\\':
				b = append(b, '\\', c)
			case '\n':
				b = append(b, '\\', 'n')
			case '\r':
				b = append(b, '\\', 'r')
			case '\t':
				b = append(b, '\\', 't')
			default:
				b = append(b, '\\', 'u', '0', '0', hexDigits[c>>4], hexDigits[c&0xf])
			}
			i++
			start = i
			continue
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 {
			b = append(b, s[start:i]...)
			b = append(b, `�`...)
			i += size
			start = i
			continue
		}
		i += size
	}
	b = append(b, s[start:]...)
	return append(b, '"')
}

func encodeMeta(m map[string]string) []byte {
	if len(m) == 0 {
		return nil
	}
	b := make([]byte, 0, 64)
	b = append(b, '{')
	first := true
	for k, v := range m {
		if !first {
			b = append(b, ',')
		}
		first = false
		b = appendJSONString(b, k)
		b = append(b, ':')
		b = appendJSONString(b, v)
	}
	return append(b, '}')
}

func encodeStrings(ss []string) []byte {
	if ss == nil {
		return nil
	}
	b := make([]byte, 0, 16*len(ss)+2)
	b = append(b, '[')
	for i, s := range ss {
		if i > 0 {
			b = append(b, ',')
		}
		b = appendJSONString(b, s)
	}
	return append(b, ']')
}

func encodeIDs(ids []int64) []byte {
	if ids == nil {
		return nil
	}
	b := make([]byte, 0, 8*len(ids)+2)
	b = append(b, '[')
	for i, id := range ids {
		if i > 0 {
			b = append(b, ',')
		}
		b = strconv.AppendInt(b, id, 10)
	}
	return append(b, ']')
}

func decodeMeta(b []byte) map[string]string {
	if len(b) == 0 {
		return nil
	}
	var m map[string]string
	if json.Unmarshal(b, &m) != nil {
		return nil
	}
	return m
}

func decodeStrings(b []byte) []string {
	if len(b) == 0 {
		return nil
	}
	var ss []string
	if json.Unmarshal(b, &ss) != nil {
		return nil
	}
	return ss
}

func decodeIDs(b []byte) []int64 {
	if len(b) == 0 {
		return nil
	}
	var ids []int64
	if json.Unmarshal(b, &ids) != nil {
		return nil
	}
	return ids
}

type entry struct {
	state   driver.State
	attempt int
	reason  string
	err     string
	trace   string
	server  string
}

func (e entry) encode(at time.Time) []byte {
	if e.reason == "" {
		return nil
	}
	b := make([]byte, 0, 96+len(e.err)+len(e.trace))
	b = append(b, `{"at":"`...)
	b = at.UTC().AppendFormat(b, time.RFC3339Nano)
	b = append(b, `","state":`...)
	b = appendJSONString(b, string(e.state))
	b = append(b, `,"attempt":`...)
	b = strconv.AppendInt(b, int64(e.attempt), 10)
	b = append(b, `,"reason":`...)
	b = appendJSONString(b, e.reason)
	if e.err != "" {
		b = append(b, `,"error":`...)
		b = appendJSONString(b, e.err)
	}
	if e.trace != "" {
		b = append(b, `,"trace":`...)
		b = appendJSONString(b, e.trace)
	}
	if e.server != "" {
		b = append(b, `,"server":`...)
		b = appendJSONString(b, e.server)
	}
	return append(b, '}')
}

func decodeHistory(b []byte) []driver.Entry {
	var raw []struct {
		At      time.Time    `json:"at"`
		State   driver.State `json:"state"`
		Attempt int          `json:"attempt"`
		Reason  string       `json:"reason"`
		Error   string       `json:"error"`
		Trace   string       `json:"trace"`
		Server  string       `json:"server"`
	}
	if json.Unmarshal(b, &raw) != nil {
		return nil
	}
	out := make([]driver.Entry, len(raw))
	for i, e := range raw {
		out[i] = driver.Entry(e)
	}
	return out
}

func clean(s string, n int) string {
	if strings.IndexByte(s, 0) >= 0 {
		s = strings.ReplaceAll(s, "\x00", "")
	}
	if len(s) > n {
		for n > 0 && !utf8.RuneStart(s[n]) {
			n--
		}
		s = s[:n]
	}
	if !utf8.ValidString(s) {
		s = strings.ToValidUTF8(s, "�")
	}
	return s
}

func millis(d time.Duration) int64 {
	if d < 0 {
		return -1
	}
	return int64((d + time.Millisecond - 1) / time.Millisecond)
}

func timeout(ms int64) time.Duration {
	if ms < 0 {
		return -1
	}
	return time.Duration(ms) * time.Millisecond
}

func micros(d time.Duration) int64 {
	return int64(d / time.Microsecond)
}

func stamp(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return civil.DateTimeOf(t.UTC())
}

type moment struct{ time.Time }

func (m *moment) Scan(v any) error {
	switch v := v.(type) {
	case nil:
		m.Time = time.Time{}
		return nil
	case time.Time:
		m.Time = time.Date(v.Year(), v.Month(), v.Day(), v.Hour(), v.Minute(), v.Second(), v.Nanosecond(), time.UTC)
		return nil
	}
	return fmt.Errorf("kiln: cannot scan %T into a timestamp", v)
}

type table struct {
	b    []byte
	n    int
	cols int
}

func (r *table) row() {
	if r.n == 0 {
		r.b = append(r.b, '[')
	} else {
		r.b = append(r.b, "},"...)
	}
	r.b = append(r.b, '{')
	r.n++
	r.cols = 0
}

func (r *table) key(name string) {
	if r.cols > 0 {
		r.b = append(r.b, ',')
	}
	r.cols++
	r.b = append(r.b, '"')
	r.b = append(r.b, name...)
	r.b = append(r.b, '"', ':')
}

func (r *table) int(name string, v int64) {
	r.key(name)
	r.b = strconv.AppendInt(r.b, v, 10)
}

func (r *table) flag(name string, v bool) {
	if v {
		r.int(name, 1)
	}
}

func (r *table) str(name, v string) {
	r.key(name)
	r.b = appendJSONString(r.b, v)
}

func (r *table) opt(name, v string) {
	if v != "" {
		r.str(name, v)
	}
}

func (r *table) text(name string, v []byte) {
	if v != nil {
		r.key(name)
		r.b = appendJSONString(r.b, string(v))
	}
}

func (r *table) json(name string, v []byte) {
	if v != nil {
		r.key(name)
		r.b = append(r.b, v...)
	}
}

func (r *table) bin(name string, v []byte) {
	if v != nil {
		r.key(name)
		r.b = append(r.b, '"')
		r.b = hex.AppendEncode(r.b, v)
		r.b = append(r.b, '"')
	}
}

func (r *table) time(name string, t time.Time) {
	if !t.IsZero() {
		r.key(name)
		r.b = append(r.b, '"')
		r.b = t.UTC().AppendFormat(r.b, isoLayout)
		r.b = append(r.b, '"')
	}
}

func (r *table) String() string {
	if r.n == 0 {
		return "[]"
	}
	return string(append(r.b, "}]"...))
}

func idList(ids []int64) string {
	if len(ids) == 0 {
		return "[]"
	}
	return string(encodeIDs(ids))
}

func stringList(ss []string) string {
	if len(ss) == 0 {
		return "[]"
	}
	return string(encodeStrings(ss))
}

func keyList(keys [][]byte) string {
	b := make([]byte, 0, 2+36*len(keys))
	b = append(b, '[')
	for i, k := range keys {
		if i > 0 {
			b = append(b, ',')
		}
		b = append(b, '"')
		b = hex.AppendEncode(b, k)
		b = append(b, '"')
	}
	return string(append(b, ']'))
}

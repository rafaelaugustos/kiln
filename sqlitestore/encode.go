package sqlitestore

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/rafaelaugustos/kiln/driver"
)

const hexDigits = "0123456789abcdef"

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

func idList(ids []int64) string {
	if len(ids) == 0 {
		return "[]"
	}
	return string(encodeIDs(ids))
}

func nameList(ss []string) string {
	if len(ss) == 0 {
		return "[]"
	}
	return string(encodeStrings(ss))
}

func text(b []byte) any {
	if b == nil {
		return nil
	}
	return string(b)
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
	if json.Unmarshal(b, &ids) != nil || len(ids) == 0 {
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

func (e entry) encode(at int64) any {
	if e.reason == "" {
		return nil
	}
	b := make([]byte, 0, 96+len(e.err)+len(e.trace))
	b = append(b, `{"at":"`...)
	b = time.UnixMicro(at).UTC().AppendFormat(b, time.RFC3339Nano)
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
	return string(append(b, '}'))
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

func instant(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t.UnixMicro()
}

type stamp struct{ time.Time }

func (s *stamp) Scan(v any) error {
	switch v := v.(type) {
	case nil:
		s.Time = time.Time{}
	case int64:
		s.Time = time.UnixMicro(v).UTC()
	default:
		return fmt.Errorf("kiln: cannot scan %T into a timestamp", v)
	}
	return nil
}

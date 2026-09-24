package pgstore

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const hex = "0123456789abcdef"

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
				b = append(b, '\\', 'u', '0', '0', hex[c>>4], hex[c&0xf])
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

func encodeMeta(m map[string]string) string {
	if len(m) == 0 {
		return ""
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
	return string(append(b, '}'))
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

func textArray(ss []string) string {
	if ss == nil {
		return ""
	}
	b := make([]byte, 0, 16*len(ss)+2)
	b = append(b, '{')
	for i, s := range ss {
		if i > 0 {
			b = append(b, ',')
		}
		b = append(b, '"')
		for j := 0; j < len(s); j++ {
			if s[j] == '"' || s[j] == '\\' {
				b = append(b, '\\')
			}
			b = append(b, s[j])
		}
		b = append(b, '"')
	}
	return string(append(b, '}'))
}

func intArray(ids []int64) string {
	if ids == nil {
		return ""
	}
	b := make([]byte, 0, 8*len(ids)+2)
	b = append(b, '{')
	for i, id := range ids {
		if i > 0 {
			b = append(b, ',')
		}
		b = strconv.AppendInt(b, id, 10)
	}
	return string(append(b, '}'))
}

func clean(s string, n int) string {
	if strings.IndexByte(s, 0) >= 0 {
		s = strings.ReplaceAll(s, "\x00", "")
	}
	s = truncate(s, n)
	if !utf8.ValidString(s) {
		s = strings.ToValidUTF8(s, "\uFFFD")
	}
	return s
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}

func micros(d time.Duration) int64 {
	return int64(d / time.Microsecond)
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

package mysqlstore

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

type raw string

type jsonText []byte

const stampLayout = "2006-01-02 15:04:05.000000"

func render(q string, args ...any) string {
	return string(appendSQL(make([]byte, 0, len(q)+24*len(args)), q, args...))
}

func appendSQL(b []byte, q string, args ...any) []byte {
	n := 0
	for {
		i := strings.IndexByte(q, '?')
		if i < 0 {
			break
		}
		if n == len(args) {
			panic(fmt.Sprintf("mysqlstore: too few arguments for %q", q))
		}
		b = append(b, q[:i]...)
		b = appendValue(b, args[n])
		n++
		q = q[i+1:]
	}
	if n != len(args) {
		panic(fmt.Sprintf("mysqlstore: %d arguments for %d placeholders", len(args), n))
	}
	return append(b, q...)
}

func appendValue(b []byte, v any) []byte {
	switch v := v.(type) {
	case nil:
		return append(b, "NULL"...)
	case raw:
		return append(b, v...)
	case int:
		return strconv.AppendInt(b, int64(v), 10)
	case int16:
		return strconv.AppendInt(b, int64(v), 10)
	case int32:
		return strconv.AppendInt(b, int64(v), 10)
	case int64:
		return strconv.AppendInt(b, v, 10)
	case bool:
		if v {
			return append(b, "TRUE"...)
		}
		return append(b, "FALSE"...)
	case string:
		return appendString(b, v)
	case driver.State:
		return appendString(b, string(v))
	case []byte:
		return appendBytes(b, v)
	case jsonText:
		if v == nil {
			return append(b, "NULL"...)
		}
		return appendString(b, string(v))
	case time.Time:
		return appendTime(b, v)
	case []int64:
		if len(v) == 0 {
			return append(b, "NULL"...)
		}
		for i, n := range v {
			if i > 0 {
				b = append(b, ',')
			}
			b = strconv.AppendInt(b, n, 10)
		}
		return b
	case []string:
		if len(v) == 0 {
			return append(b, "NULL"...)
		}
		for i, s := range v {
			if i > 0 {
				b = append(b, ',')
			}
			b = appendString(b, s)
		}
		return b
	case [][]byte:
		if len(v) == 0 {
			return append(b, "NULL"...)
		}
		for i, k := range v {
			if i > 0 {
				b = append(b, ',')
			}
			b = appendBytes(b, k)
		}
		return b
	}
	panic(fmt.Sprintf("mysqlstore: cannot render %T", v))
}

func plain[T string | []byte](s T) bool {
	for i := 0; i < len(s); i++ {
		if c := s[i]; c < 0x20 || c > 0x7e || c == '\'' || c == '\\' {
			return false
		}
	}
	return true
}

func appendString(b []byte, s string) []byte {
	if plain(s) {
		b = append(b, '\'')
		b = append(b, s...)
		return append(b, '\'')
	}
	b = append(b, "_utf8mb4 X'"...)
	b = hex.AppendEncode(b, []byte(s))
	return append(b, '\'')
}

func appendBytes(b []byte, v []byte) []byte {
	switch {
	case v == nil:
		return append(b, "NULL"...)
	case plain(v):
		b = append(b, '\'')
		b = append(b, v...)
		return append(b, '\'')
	}
	b = append(b, "X'"...)
	b = hex.AppendEncode(b, v)
	return append(b, '\'')
}

func appendTime(b []byte, t time.Time) []byte {
	if t.IsZero() {
		return append(b, "NULL"...)
	}
	b = append(b, "TIMESTAMP '"...)
	b = t.UTC().AppendFormat(b, stampLayout)
	return append(b, '\'')
}

func micros(d time.Duration) int64 {
	return int64(d / time.Microsecond)
}

package dashboard

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestNum(t *testing.T) {
	t.Parallel()
	for in, want := range map[int64]string{0: "0", 7: "7", 999: "999", 1000: "1,000", 1234567: "1,234,567", -4200: "-4,200"} {
		if got := num(in); got != want {
			t.Errorf("num(%d) = %q, want %q", in, got, want)
		}
	}
	if got := num(12345); got != "12,345" {
		t.Errorf("num(int) = %q", got)
	}
}

func TestAgo(t *testing.T) {
	t.Parallel()
	now := time.Now()
	tests := []struct {
		at   time.Time
		want string
	}{
		{now, "now"},
		{now.Add(-3 * time.Second), "3s ago"},
		{now.Add(-3*time.Minute - 10*time.Second), "3m ago"},
		{now.Add(-5 * time.Hour), "5h ago"},
		{now.Add(-72 * time.Hour), "3d ago"},
		{now.Add(12*time.Second + 500*time.Millisecond), "in 12s"},
		{now.Add(90 * time.Minute), "in 1h"},
	}
	for _, tt := range tests {
		if got := ago(tt.at); got != tt.want {
			t.Errorf("ago(%v) = %q, want %q", now.Sub(tt.at), got, tt.want)
		}
	}
}

func TestDur(t *testing.T) {
	t.Parallel()
	for in, want := range map[time.Duration]string{
		0:                                 "0s",
		340 * time.Millisecond:            "340ms",
		2500 * time.Millisecond:           "2.5s",
		42 * time.Second:                  "42s",
		3*time.Minute + 20*time.Second:    "3m 20s",
		3 * time.Minute:                   "3m",
		2*time.Hour + 5*time.Minute:       "2h 5m",
		50 * time.Hour:                    "2d 2h",
		-time.Second:                      "0s",
		time.Hour + 59*time.Second:        "1h",
		59*time.Minute + 59*time.Second:   "59m 59s",
		24*time.Hour + 30*time.Minute:     "1d",
		10*time.Second + time.Millisecond: "10s",
	} {
		if got := dur(in); got != want {
			t.Errorf("dur(%v) = %q, want %q", in, got, want)
		}
	}
}

func TestPretty(t *testing.T) {
	t.Parallel()
	got := string(pretty(json.RawMessage(`{"a":"<b>\"x\"</b>","n":-1.5e3,"ok":true,"z":null,"l":[1,"s"]}`)))
	for _, w := range []string{
		`<span class="k">&#34;a&#34;</span>: <span class="s">&#34;&lt;b&gt;\&#34;x\&#34;&lt;/b&gt;&#34;</span>`,
		`<span class="n">-1.5e3</span>`,
		`<span class="b">true</span>`,
		`<span class="b">null</span>`,
		`<span class="s">&#34;s&#34;</span>`,
	} {
		if !strings.Contains(got, w) {
			t.Errorf("missing %s in\n%s", w, got)
		}
	}
	if strings.Contains(got, "<b>") {
		t.Error("unescaped markup")
	}
	if got := string(pretty(json.RawMessage(`<not json>`))); got != "&lt;not json&gt;" {
		t.Errorf("invalid JSON: %q", got)
	}
}

func TestPreview(t *testing.T) {
	t.Parallel()
	if got := preview(json.RawMessage("{\n  \"a\": 1\n}")); got != `{"a":1}` {
		t.Errorf("compact: %q", got)
	}
	if got := preview(json.RawMessage(`{}`)); got != "" {
		t.Errorf("empty object: %q", got)
	}
	long := json.RawMessage(`"` + strings.Repeat("é", 200) + `"`)
	got := preview(long)
	if !strings.HasSuffix(got, "…") || len(got) > 170 || !strings.HasPrefix(got, `"é`) {
		t.Errorf("truncate: %q", got)
	}
}

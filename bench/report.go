package main

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"
)

type results map[string]map[string][]result

func (r results) add(scenario, variant string, res result) {
	if r[scenario] == nil {
		r[scenario] = make(map[string][]result)
	}
	r[scenario][variant] = append(r[scenario][variant], res)
}

func (m metric) header() string {
	switch m {
	case rate:
		return "jobs/s"
	case p50:
		return "p50 ms"
	default:
		return "p99 ms"
	}
}

func (m metric) of(r result) float64 {
	switch m {
	case rate:
		return r.rate
	case p50:
		return ms(r.p50)
	default:
		return ms(r.p99)
	}
}

func (m metric) format(v float64) string {
	if m == rate {
		return thousands(int64(math.Round(v)))
	}
	return strconv.FormatFloat(v, 'f', 2, 64)
}

func (r result) format(metrics []metric) string {
	parts := make([]string, len(metrics))
	for i, m := range metrics {
		parts[i] = m.header() + " " + m.format(m.of(r))
	}
	return strings.Join(parts, ", ")
}

func report(w io.Writer, scs []scenario, res results) {
	fmt.Fprintln(w, "| Scenario | Library | jobs/s | p50 ms | p99 ms |")
	fmt.Fprintln(w, "|---|---|--:|--:|--:|")
	for _, sc := range scs {
		for _, v := range sc.variants {
			rs := res[sc.id][v.name]
			if len(rs) == 0 {
				continue
			}
			cells := make([]string, 3)
			for _, m := range sc.metrics {
				cells[m] = summarize(m, rs)
			}
			fmt.Fprintf(w, "| %s. %s | %s | %s |\n", sc.id, sc.title, v.name, strings.Join(cells, " | "))
		}
	}
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Raw values per round:")
	for _, sc := range scs {
		for _, v := range sc.variants {
			rs := res[sc.id][v.name]
			if len(rs) == 0 {
				continue
			}
			for _, m := range sc.metrics {
				vals := make([]string, len(rs))
				for i, r := range rs {
					vals[i] = m.format(m.of(r))
				}
				fmt.Fprintf(w, "  %s %-32s %-7s %s\n", sc.id, v.name, m.header(), strings.Join(vals, "  "))
			}
		}
	}
}

func summarize(m metric, rs []result) string {
	vals := make([]float64, len(rs))
	for i, r := range rs {
		vals[i] = m.of(r)
	}
	slices.Sort(vals)
	return fmt.Sprintf("%s (%s–%s)", m.format(median(vals)), m.format(vals[0]), m.format(vals[len(vals)-1]))
}

func (r results) save(path string, scs []scenario) error {
	type row struct {
		Scenario string    `json:"scenario"`
		Title    string    `json:"title"`
		Library  string    `json:"library"`
		Rate     []float64 `json:"jobs_per_sec,omitempty"`
		P50      []float64 `json:"p50_ms,omitempty"`
		P99      []float64 `json:"p99_ms,omitempty"`
	}
	var rows []row
	for _, sc := range scs {
		for _, v := range sc.variants {
			rs := r[sc.id][v.name]
			if len(rs) == 0 {
				continue
			}
			out := row{Scenario: sc.id, Title: sc.title, Library: v.name}
			for _, m := range sc.metrics {
				vals := make([]float64, len(rs))
				for i, x := range rs {
					vals[i] = math.Round(m.of(x)*100) / 100
				}
				switch m {
				case rate:
					out.Rate = vals
				case p50:
					out.P50 = vals
				case p99:
					out.P99 = vals
				}
			}
			rows = append(rows, out)
		}
	}
	b, err := json.MarshalIndent(rows, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}

func quantile(sorted []time.Duration, q float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := int(math.Ceil(q*float64(len(sorted)))) - 1
	return sorted[min(max(i, 0), len(sorted)-1)]
}

func median(sorted []float64) float64 {
	n := len(sorted)
	if n%2 == 1 {
		return sorted[n/2]
	}
	return (sorted[n/2-1] + sorted[n/2]) / 2
}

func ms(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}

func thousands(n int64) string {
	s := strconv.FormatInt(n, 10)
	var b strings.Builder
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteRune(c)
	}
	return b.String()
}

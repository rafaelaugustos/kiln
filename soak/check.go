package main

import (
	"bufio"
	"cmp"
	"context"
	"database/sql"
	"fmt"
	"maps"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/rafaelaugustos/kiln"
	"github.com/rafaelaugustos/kiln/driver"
)

const (
	rateSlack  = 500 * time.Millisecond
	stallGap   = time.Second
	stallGrace = 5 * time.Second
)

var windows = []time.Duration{time.Second, 10 * time.Second, time.Minute}

type report struct {
	jobs     int
	kinds    map[string]int
	states   map[kiln.State]int
	missing  int
	runs     int
	results  map[string]int
	checks   []verdict
	drained  time.Duration
	drainErr error
}

type verdict struct {
	ok   bool
	warn bool
	text string
}

type final struct {
	state   kiln.State
	attempt int
}

type run struct {
	job      int64
	attempt  int
	inst     string
	started  int64
	finished sql.NullInt64
	result   sql.NullString
}

type tally struct {
	runs, cut, ok int
}

type span struct {
	start, end int64
}

type stall struct {
	at  int64
	why string
}

func (r *report) add(ok bool, format string, args ...any) {
	r.checks = append(r.checks, verdict{ok: ok, text: fmt.Sprintf(format, args...)})
}

func (r *report) passed() bool {
	return r.drainErr == nil && !slices.ContainsFunc(r.checks, func(v verdict) bool { return !v.ok })
}

func (h *harness) verify(ctx context.Context) (*report, error) {
	enq, err := h.enqueued(ctx)
	if err != nil {
		return nil, fmt.Errorf("read enqueued jobs: %w", err)
	}
	recs, err := h.records(ctx)
	if err != nil {
		return nil, fmt.Errorf("read kiln jobs: %w", err)
	}
	counters, err := h.counters(ctx)
	if err != nil {
		return nil, fmt.Errorf("read limits: %w", err)
	}
	swept, err := h.sweep(ctx)
	if err != nil {
		return nil, fmt.Errorf("sweep: %w", err)
	}
	runs, err := h.runs(ctx)
	if err != nil {
		return nil, fmt.Errorf("read runs: %w", err)
	}
	r := &report{jobs: len(enq), runs: len(runs), kinds: map[string]int{}, states: map[kiln.State]int{}, results: map[string]int{}}

	var lost, unexpected []int64
	for id, e := range enq {
		r.kinds[e.kind]++
		rec, ok := recs[id]
		if !ok {
			r.missing++
			lost = append(lost, id)
			continue
		}
		r.states[rec.state]++
		switch rec.state {
		case kiln.Succeeded, kiln.Failed, kiln.Deleted:
		default:
			lost = append(lost, id)
			continue
		}
		want := kiln.Succeeded
		if e.kind == "doomed" {
			want = kiln.Failed
		}
		if rec.state != want {
			unexpected = append(unexpected, id)
		}
	}
	r.add(len(lost) == 0, "every job reached a final state: %d lost%s", len(lost), sample(lost))
	r.add(len(unexpected) == 0, "doomed jobs failed and all others succeeded: %d unexpected%s", len(unexpected), sample(unexpected))

	h.fleet.mu.Lock()
	exits := make(map[string]int64, len(h.fleet.procs))
	for _, p := range h.fleet.procs {
		exits[p.inst] = p.exited
	}
	h.fleet.mu.Unlock()
	tallies := make(map[int64]*tally)
	spans := make(map[string][]span)
	for _, ru := range runs {
		t := tallies[ru.job]
		if t == nil {
			t = &tally{}
			tallies[ru.job] = t
		}
		t.runs++
		res := "unfinished"
		if ru.result.Valid {
			res = ru.result.String
		}
		r.results[res]++
		switch res {
		case "ok":
			t.ok++
		case "unfinished", "canceled":
			t.cut++
		}
		if e, ok := enq[ru.job]; ok && e.key != "" {
			end := exits[ru.inst]
			if ru.finished.Valid {
				end = ru.finished.Int64
			}
			spans[e.key] = append(spans[e.key], span{ru.started, max(end, ru.started)})
		}
	}
	var extra, unrun []int64
	for id := range enq {
		rec, ok := recs[id]
		if !ok {
			continue
		}
		t := tallies[id]
		if t == nil {
			t = &tally{}
		}
		if t.runs > rec.attempt+t.cut {
			extra = append(extra, id)
		}
		if rec.state == kiln.Succeeded && t.ok == 0 {
			unrun = append(unrun, id)
		}
	}
	r.add(len(extra) == 0, "no job ran more often than its attempts plus its runs cut short: %d over%s", len(extra), sample(extra))
	r.add(len(unrun) == 0, "every succeeded job has a successful run: %d without%s", len(unrun), sample(unrun))

	stalls := h.stalls(runs)
	for _, l := range h.limits {
		s := spans[l.Key]
		ok := true
		var (
			parts  []string
			excuse string
		)
		if l.Max > 0 {
			peak := overlap(s)
			ok = peak <= l.Max
			parts = append(parts, fmt.Sprintf("peak %d running of max %d", peak, l.Max))
		}
		if l.Rate > 0 {
			starts := make([]int64, len(s))
			for i, sp := range s {
				starts[i] = sp.start
			}
			slices.Sort(starts)
			for _, w := range windows {
				bound := int(math.Floor(float64(l.Rate)*(w+rateSlack).Seconds()/l.Per.Seconds() + float64(l.Burst) + 0.5))
				n, why, strict := busiest(starts, w.Microseconds(), bound, stalls)
				ok = ok && !strict
				excuse = cmp.Or(why, excuse)
				parts = append(parts, fmt.Sprintf("%d starts of %d allowed in %s", n, bound, w))
			}
		}
		text := fmt.Sprintf("limit %s over %d runs: %s", l.Key, len(s), strings.Join(parts, ", "))
		if excuse != "" {
			text += "; a window over the bound starts right after " + excuse
		}
		r.checks = append(r.checks, verdict{ok: ok, warn: excuse != "", text: text})
	}

	var held []string
	for _, c := range counters {
		if c.Active != 0 || c.Throttled != 0 || c.Reserved != 0 {
			held = append(held, fmt.Sprintf("%s active %d throttled %d reserved %d", c.Key, c.Active, c.Throttled, c.Reserved))
		}
	}
	r.add(len(held) == 0, "limit counters at 0 on %d keys%s", len(counters), list(held))
	r.add(swept == 0, "sweep found %d rows to repair", swept)

	h.fleet.mu.Lock()
	var crashed, unclean []string
	for _, p := range h.fleet.procs {
		switch {
		case p.signal == 0 && !p.stopped:
			crashed = append(crashed, fmt.Sprintf("%s: %v", p.inst, p.err))
		case (p.signal == syscall.SIGTERM || p.stopped) && p.err != nil:
			unclean = append(unclean, fmt.Sprintf("%s: %v", p.inst, p.err))
		}
	}
	hung, failures := slices.Clone(h.fleet.hung), len(h.fleet.failures)
	h.fleet.mu.Unlock()
	r.add(len(crashed) == 0 && failures == 0, "no worker exited on its own or failed to start: %d exited, %d failed to start%s", len(crashed), failures, list(crashed))
	r.add(len(unclean) == 0 && len(hung) == 0, "terminated workers shut down cleanly: %d unclean, %d hung%s", len(unclean), len(hung), list(append(unclean, hung...)))
	return r, nil
}

func (h *harness) enqueued(ctx context.Context) (map[int64]entry, error) {
	rows, err := h.b.db.QueryContext(ctx, "SELECT id, kind, lkey FROM "+h.b.enqueued)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[int64]entry)
	for rows.Next() {
		var (
			id int64
			e  entry
		)
		if err := rows.Scan(&id, &e.kind, &e.key); err != nil {
			return nil, err
		}
		out[id] = e
	}
	return out, rows.Err()
}

func (h *harness) runs(ctx context.Context) ([]run, error) {
	rows, err := h.b.db.QueryContext(ctx, "SELECT job, attempt, inst, started, finished, result FROM "+h.b.runs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []run
	for rows.Next() {
		var ru run
		if err := rows.Scan(&ru.job, &ru.attempt, &ru.inst, &ru.started, &ru.finished, &ru.result); err != nil {
			return nil, err
		}
		out = append(out, ru)
	}
	return out, rows.Err()
}

func (h *harness) records(ctx context.Context) (map[int64]final, error) {
	out := make(map[int64]final)
	for _, st := range []kiln.State{kiln.Awaiting, kiln.Scheduled, kiln.Throttled, kiln.Enqueued, kiln.Processing, kiln.Failed, kiln.Succeeded, kiln.Deleted} {
		q := kiln.JobQuery{State: st, Limit: 500}
		for {
			p, err := h.b.store.Jobs(ctx, q)
			if err != nil {
				return nil, err
			}
			for _, rec := range p.Records {
				out[rec.ID] = final{rec.State, rec.Attempt}
			}
			if p.Next == "" {
				break
			}
			q.Cursor = p.Next
		}
	}
	return out, nil
}

func (h *harness) counters(ctx context.Context) ([]driver.LimitInfo, error) {
	lr, ok := h.b.store.(driver.LimitReader)
	if !ok {
		return nil, nil
	}
	var out []driver.LimitInfo
	after := ""
	for {
		page, err := lr.Limits(ctx, after, 100)
		if err != nil {
			return nil, err
		}
		out = append(out, page...)
		if len(page) < 100 {
			return out, nil
		}
		after = page[len(page)-1].Key
	}
}

func (h *harness) sweep(ctx context.Context) (int, error) {
	total := 0
	for range 3 {
		n, err := h.b.store.Sweep(ctx, 1000)
		if err != nil {
			return total, err
		}
		total += n
	}
	return total, nil
}

func overlap(spans []span) int {
	type edge struct {
		at int64
		d  int
	}
	edges := make([]edge, 0, 2*len(spans))
	for _, s := range spans {
		edges = append(edges, edge{s.start, 1}, edge{s.end, -1})
	}
	slices.SortFunc(edges, func(a, b edge) int { return cmp.Or(cmp.Compare(a.at, b.at), cmp.Compare(a.d, b.d)) })
	n, peak := 0, 0
	for _, e := range edges {
		n += e.d
		peak = max(peak, n)
	}
	return peak
}

func (h *harness) stalls(runs []run) []stall {
	var out []stall
	for _, at := range h.restarted {
		out = append(out, stall{at, fmt.Sprintf("a database restart at %s", time.UnixMicro(at).Sub(h.start).Round(time.Second))})
	}
	starts := make([]int64, len(runs))
	for i, ru := range runs {
		starts[i] = ru.started
	}
	slices.Sort(starts)
	for i := 1; i < len(starts); i++ {
		if gap := time.Duration(starts[i]-starts[i-1]) * time.Microsecond; gap >= stallGap {
			at := time.UnixMicro(starts[i]).Sub(h.start).Round(time.Second)
			out = append(out, stall{starts[i], fmt.Sprintf("a %s gap in job starts at %s", gap.Round(100*time.Millisecond), at)})
		}
	}
	return out
}

func busiest(starts []int64, w int64, bound int, stalls []stall) (best int, why string, strict bool) {
	j := 0
	for i := range starts {
		for starts[i]-starts[j] >= w {
			j++
		}
		n := i - j + 1
		best = max(best, n)
		if n <= bound {
			continue
		}
		k := slices.IndexFunc(stalls, func(s stall) bool {
			return starts[j] >= s.at && starts[j]-s.at <= stallGrace.Microseconds()
		})
		if k < 0 {
			strict = true
		} else {
			why = stalls[k].why
		}
	}
	return best, why, strict
}

func sample(ids []int64) string {
	if len(ids) == 0 {
		return ""
	}
	slices.Sort(ids)
	return fmt.Sprintf(" (ids %v)", ids[:min(len(ids), 10)])
}

func list(items []string) string {
	if len(items) == 0 {
		return ""
	}
	return " (" + strings.Join(items[:min(len(items), 10)], "; ") + ")"
}

func (h *harness) print(r *report) {
	h.out.Lock()
	defer h.out.Unlock()
	other := 0
	for st, n := range r.states {
		if st != kiln.Succeeded && st != kiln.Failed && st != kiln.Deleted {
			other += n
		}
	}
	fmt.Printf("\nsummary: %s, %s, %d workers, %g enqueues/s\n", h.o.backend, h.o.duration, h.o.workers, h.o.rate)
	fmt.Printf("  jobs     %d enqueued: %s\n", r.jobs, tallyList(r.kinds))
	fmt.Printf("  states   succeeded %d, failed %d, deleted %d, live %d, missing %d\n", r.states[kiln.Succeeded], r.states[kiln.Failed], r.states[kiln.Deleted], other, r.missing)
	fmt.Printf("  runs     %d: %s\n", r.runs, tallyList(r.results))
	fmt.Printf("  chaos    %d sigkill, %d sigterm, %d database restarts\n", h.kills.Load(), h.terms.Load(), h.restarts.Load())
	fmt.Printf("  enqueue  %d batches rolled back", h.enq.failed.Load())
	if err := h.enq.last.Load(); err != nil {
		msg := strings.Join(strings.Fields((*err).Error()), " ")
		fmt.Printf(", the last with: %s", msg[:min(len(msg), 120)])
	}
	fmt.Println()
	logs := tallyList(h.logs())
	if logs == "" {
		logs = "no warnings or errors"
	}
	fmt.Printf("  logs     %s\n", logs)
	fmt.Printf("  drain    %s\n\n", r.drained.Round(time.Second))
	for _, c := range r.checks {
		mark := "ok  "
		switch {
		case !c.ok:
			mark = "FAIL"
		case c.warn:
			mark = "WARN"
		}
		fmt.Printf("  %s  %s\n", mark, c.text)
	}
	if r.drainErr != nil {
		fmt.Printf("  FAIL  %v\n", r.drainErr)
	}
	if r.passed() {
		fmt.Println("\nPASS")
	} else {
		fmt.Println("\nFAIL")
	}
}

func (h *harness) logs() map[string]int {
	out := make(map[string]int)
	paths, _ := filepath.Glob(filepath.Join(h.dir, "*.log"))
	for _, p := range paths {
		f, err := os.Open(p)
		if err != nil {
			continue
		}
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 64<<10), 1<<20)
		for sc.Scan() {
			line := sc.Text()
			if !strings.Contains(line, " level=WARN ") && !strings.Contains(line, " level=ERROR ") {
				continue
			}
			key := strings.TrimPrefix(field(line, "msg"), "kiln: ")
			if op := field(line, "op"); op != "" {
				key += " " + op
			}
			out[key]++
		}
		f.Close()
	}
	return out
}

func field(line, name string) string {
	i := strings.Index(line, " "+name+"=")
	if i < 0 {
		return ""
	}
	v := line[i+len(name)+2:]
	if q, err := strconv.QuotedPrefix(v); err == nil {
		u, _ := strconv.Unquote(q)
		return u
	}
	if j := strings.IndexByte(v, ' '); j >= 0 {
		v = v[:j]
	}
	return v
}

func tallyList(m map[string]int) string {
	parts := make([]string, 0, len(m))
	for _, k := range slices.Sorted(maps.Keys(m)) {
		parts = append(parts, fmt.Sprintf("%s %d", k, m[k]))
	}
	return strings.Join(parts, ", ")
}

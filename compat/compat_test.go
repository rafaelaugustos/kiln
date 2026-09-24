package compat

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln"
	"github.com/rafaelaugustos/kiln/driver"
)

const (
	oldVersion = "v0.2.0"
	oldDir     = "v020"
	oldName    = "compat-v020"
	newName    = "compat-current"
	poll       = 20 * time.Millisecond
)

var dataTables = []string{"jobs", "archive", "deps", "batches", "uniques", "recurring", "limits"}

func TestRollingUpgrade(t *testing.T) {
	bin := build(t)
	t.Run("shipped", func(t *testing.T) {
		frozen(t, bin.mods)
	})
	for _, b := range []struct {
		name string
		open func(*testing.T) backend
	}{
		{"postgres", newPostgres},
		{"mysql", newMySQL},
		{"sqlite", newSQLite},
	} {
		t.Run(b.name, func(t *testing.T) {
			t.Parallel()
			upgrade(t, bin.path, b.open(t))
		})
	}
}

func upgrade(t *testing.T, bin string, db backend) {
	began := time.Now()
	ctx := t.Context()

	old := launch(t, bin, db, "-echo=10", "-fail=5", "-limited=4", "-flows=1", "-batches=1")
	alone := old.result()
	if !eventually(30*time.Second, func() bool {
		n, err := db.count(ctx, "TRUE")
		return err == nil && n == 0
	}) {
		t.Fatalf("%s did not finish the jobs it enqueued while running alone", oldVersion)
	}

	before := inspect(t, db, nil)
	unmigrated(t, db)
	st, closeStore, err := db.open(ctx, true)
	if err != nil {
		t.Fatalf("the current code cannot open a database created by %s: %v", oldVersion, err)
	}
	t.Cleanup(closeStore)
	additive(t, db, before, inspect(t, db, before))

	client := kiln.NewClient(st)
	w := newWorker()
	errs := &errlog{}
	srv, stop := serve(t, client, w.mux(), errs)
	peak := watch(t, db)

	delay := 5 * time.Second
	r1 := old.send(plan{Echo: 60, Fail: 10, Handoff: 6, Limited: 6, Flows: 2, Batches: 2, Delayed: 1, Delay: delay,
		Unique: []string{"old-live"}, Window: []string{"old-window"}})
	n1 := enqueue(ctx, client, plan{Echo: 60, Fail: 10, Handoff: 6, Limited: 6, Flows: 2, Batches: 2, Delayed: 1, Delay: delay,
		After:  []int64{head(t, alone, "echo"), head(t, r1, "delayed")},
		Unique: []string{"old-live", "new-live"}, Window: []string{"old-window", "new-window"}}, errs)
	if n1.Err != "" {
		t.Fatalf("current client: %s", n1.Err)
	}
	ran := head(t, n1, "echo")
	if !eventually(30*time.Second, func() bool {
		r, err := client.Get(ctx, ran)
		return err == nil && r.State == kiln.Succeeded
	}) {
		t.Fatalf("job %d of the current client did not succeed", ran)
	}
	r2 := old.send(plan{After: []int64{ran, head(t, n1, "delayed")}, Unique: []string{"new-live"}, Window: []string{"new-window"}})

	settle(t, st)
	t.Logf("version column of the servers table: %v", reported(t, st))
	oldSum, exit := old.stop()
	if exit != nil {
		t.Errorf("%s exited with %v", oldVersion, exit)
	}
	if !eventually(10*time.Second, func() bool { return srv.Stats().Leader }) {
		t.Errorf("the current server did not take over leadership after %s stopped", oldVersion)
	}

	again := launch(t, bin, db, "-echo=20", "-fail=4", "-handoff=4", "-limited=6", "-flows=1", "-batches=1",
		"-after="+strconv.FormatInt(ran, 10))
	r3 := again.result()
	n3 := enqueue(ctx, client, plan{Echo: 20, Fail: 4, Handoff: 4, Limited: 6, Flows: 1, Batches: 1,
		After: []int64{head(t, r3, "echo")}}, errs)
	settle(t, st)
	limit := peak()

	paced := enqueue(ctx, client, plan{Rated: 20}, errs)
	settle(t, st)
	againSum, exit := again.stop()
	if exit != nil {
		t.Errorf("%s exited with %v after the restart", oldVersion, exit)
	}
	stop()
	newSum := w.report()
	stats := srv.Stats()
	newSum.Server, newSum.Stats, newSum.Errors = srv.ID(), &stats, errs.all()

	final := finished(t, client)
	counted(t, st, final, alone, r1, n1, r2, r3, n3, paced)
	exactlyOnce(t, final, oldSum, againSum, newSum)
	retried(t, client, slices.Concat(alone.Jobs["fail"], r1.Jobs["fail"], n1.Jobs["fail"], r3.Jobs["fail"], n3.Jobs["fail"]))
	handedOff(t, client, oldVersion, slices.Concat(r1.Jobs["handoff"], r3.Jobs["handoff"]))
	handedOff(t, client, version, slices.Concat(n1.Jobs["handoff"], n3.Jobs["handoff"]))
	fromOld, fromNew := spread(final, r1.Jobs["echo"]), spread(final, n1.Jobs["echo"])
	if fromOld[oldVersion] == 0 || fromOld[version] == 0 {
		t.Errorf("compat.echo jobs of the %s client were not completed by both servers: %v", oldVersion, fromOld)
	}
	t.Logf("compat.echo jobs of the %s client ran on %v, of the current client on %v", oldVersion, fromOld, fromNew)
	if len(againSum.Runs) == 0 {
		t.Errorf("%s restarted against the upgraded database but completed no jobs", oldVersion)
	}
	t.Logf("after the restart, compat.echo jobs of the %s client ran on %v, of the current client on %v",
		oldVersion, spread(final, r3.Jobs["echo"]), spread(final, n3.Jobs["echo"]))
	batches(t, st, alone, r1, n1, r3, n3)
	duplicates(t, r1, n1, r2)
	if limit != 2 {
		t.Errorf("compat.limited jobs peaked at %d enqueued or processing at once, want their limit of 2", limit)
	}
	rateHeld(t, final, paced.Jobs["rated"])
	clean(t, oldSum, againSum, newSum)

	refuse(t, bin, db)
	t.Logf("%d jobs, limit peak %d, done in %s", len(final), limit, time.Since(began).Round(time.Millisecond))
}

func launch(t *testing.T, bin string, db backend, args ...string) *proc {
	t.Helper()
	return start(t, bin, slices.Concat(db.flags(), []string{"-role=both", "-commands", "-name=" + oldName, "-poll=" + poll.String()}, args)...)
}

func serve(t *testing.T, c *kiln.Client, m *kiln.Mux, errs *errlog) (*kiln.Server, func()) {
	t.Helper()
	srv, err := kiln.NewServer(c, m, kiln.ServerConfig{
		Queues:            map[string]int{kiln.DefaultQueue: 8},
		Name:              newName,
		PollInterval:      poll,
		HeartbeatInterval: 250 * time.Millisecond,
		KillGrace:         250 * time.Millisecond,
		DeadAfter:         6 * time.Second,
		LeaderTTL:         time.Second,
		ShutdownTimeout:   5 * time.Second,
		Backoff:           kiln.Constant(150 * time.Millisecond),
		Logger:            slog.New(slog.NewTextHandler(errs, &slog.HandlerOptions{Level: slog.LevelWarn})),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()
	stop := sync.OnceFunc(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("current server: %v", err)
		}
	})
	t.Cleanup(stop)
	if !eventually(10*time.Second, func() bool { return srv.Healthy() == nil }) {
		t.Fatal("the current server did not start")
	}
	return srv, stop
}

func watch(t *testing.T, db backend) func() int {
	ctx, cancel := context.WithCancel(context.Background())
	peak := make(chan int, 1)
	go func() {
		n := 0
		for ctx.Err() == nil {
			if c, err := db.count(ctx, "limit_key = '"+limitKey+"' AND state IN ('enqueued', 'processing')"); err == nil {
				n = max(n, c)
			}
			time.Sleep(5 * time.Millisecond)
		}
		peak <- n
	}()
	stop := sync.OnceValue(func() int {
		cancel()
		return <-peak
	})
	t.Cleanup(func() { stop() })
	return stop
}

func settle(t *testing.T, st driver.Store) {
	t.Helper()
	var c driver.Counts
	if !eventually(time.Minute, func() bool {
		var err error
		c, err = st.Counts(t.Context())
		return err == nil && c.Awaiting+c.Scheduled+c.Throttled+c.Enqueued+c.Processing == 0
	}) {
		t.Fatalf("jobs still active after a minute: %+v", c)
	}
}

func eventually(d time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(20 * time.Millisecond)
	}
	return true
}

func head(t *testing.T, r *result, group string) int64 {
	t.Helper()
	if len(r.Jobs[group]) == 0 {
		t.Fatalf("no %s job in %v", group, r.Jobs)
	}
	return r.Jobs[group][0]
}

func side(server string) string {
	name, _, _ := strings.Cut(server, ":")
	switch name {
	case oldName:
		return oldVersion
	case newName:
		return version
	}
	return server
}

func get(t *testing.T, c *kiln.Client, id int64) kiln.Record {
	t.Helper()
	r, err := c.Get(t.Context(), id)
	if err != nil {
		t.Fatalf("job %d: %v", id, err)
	}
	return r
}

func last(r kiln.Record) string {
	if len(r.History) == 0 {
		return "no history"
	}
	e := r.History[len(r.History)-1]
	return fmt.Sprintf("%s by %s at attempt %d: %s", e.Reason, e.Server, e.Attempt, e.Error)
}

func list(t *testing.T, c *kiln.Client, s kiln.State) []kiln.Record {
	t.Helper()
	var out []kiln.Record
	q := kiln.JobQuery{State: s, Limit: 500}
	for {
		p, err := c.List(t.Context(), q)
		if err != nil {
			t.Fatalf("list %s jobs: %v", s, err)
		}
		out = append(out, p.Records...)
		if p.Next == "" {
			return out
		}
		q.Cursor = p.Next
	}
}

func finished(t *testing.T, c *kiln.Client) map[int64]kiln.Record {
	t.Helper()
	done := make(map[int64]kiln.Record)
	for _, s := range driver.States {
		for _, r := range list(t, c, s) {
			if s == kiln.Succeeded {
				done[r.ID] = r
				continue
			}
			t.Errorf("job %d (%s) ended %s, last event %s", r.ID, r.Kind, s, last(get(t, c, r.ID)))
		}
	}
	return done
}

func counted(t *testing.T, st driver.Store, done map[int64]kiln.Record, rs ...*result) {
	t.Helper()
	want := 0
	for _, r := range rs {
		for group, ids := range r.Jobs {
			for _, id := range ids {
				if _, ok := done[id]; !ok {
					t.Errorf("%s job %d did not succeed", group, id)
				}
			}
			want += len(ids)
		}
		want += 4 * len(r.Batches)
	}
	if len(done) != want {
		t.Errorf("%d jobs succeeded, the clients inserted %d", len(done), want)
	}
	c, err := st.Counts(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if c.Succeeded != int64(len(done)) || c.Deleted != 0 {
		t.Errorf("stats written by both versions count %d succeeded and %d deleted, want %d and 0", c.Succeeded, c.Deleted, len(done))
	}
}

func exactlyOnce(t *testing.T, done map[int64]kiln.Record, sums ...*summary) {
	t.Helper()
	for id, r := range done {
		n := 0
		var where []string
		for _, s := range sums {
			if c := s.Runs[id]; c > 0 {
				n += c
				where = append(where, fmt.Sprintf("%d on %s", c, s.Server))
			}
		}
		if n != 1 {
			t.Errorf("job %d (%s) succeeded %d times: %v", id, r.Kind, n, where)
		}
	}
	for _, s := range sums {
		for id := range s.Runs {
			if _, ok := done[id]; !ok {
				t.Errorf("a handler on %s succeeded for job %d, which did not end succeeded", s.Server, id)
			}
		}
	}
}

func retried(t *testing.T, c *kiln.Client, ids []int64) {
	t.Helper()
	moves := make(map[string]int)
	for _, id := range ids {
		r := get(t, c, id)
		if r.Attempt != 2 {
			t.Errorf("compat.fail_once job %d took %d attempts, want 2", id, r.Attempt)
		}
		for _, e := range r.History {
			if e.Reason == "retry" {
				moves[side(e.Server)+" -> "+side(r.Server)]++
			}
		}
	}
	t.Logf("compat.fail_once retries, scheduled by -> run by: %v", moves)
}

func handedOff(t *testing.T, c *kiln.Client, from string, ids []int64) {
	t.Helper()
	var attempts []int
	for _, id := range ids {
		r := get(t, c, id)
		attempts = append(attempts, r.Attempt)
		var out struct{ From, To string }
		json.Unmarshal(r.Output, &out)
		switch h := r.History; {
		case len(h) == 0 || h[len(h)-1].Reason != "retry" || side(h[len(h)-1].Server) != from:
			t.Errorf("compat.handoff job %d did not end with a retry scheduled by %s: %s", id, from, last(r))
		case side(r.Server) == from || out.From != from || out.To != side(r.Server):
			t.Errorf("compat.handoff job %d from %s finished on %s with output %s", id, from, r.Server, r.Output)
		}
	}
	t.Logf("compat.handoff jobs started on %s took %v attempts", from, attempts)
}

func reported(t *testing.T, st driver.Store) map[string]string {
	t.Helper()
	infos, err := st.Servers(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	vs := make(map[string]string)
	for _, s := range infos {
		vs[side(s.ID)] = s.Version
	}
	return vs
}

func spread(done map[int64]kiln.Record, ids []int64) map[string]int {
	by := make(map[string]int)
	for _, id := range ids {
		by[side(done[id].Server)]++
	}
	return by
}

func batches(t *testing.T, st driver.Store, rs ...*result) {
	t.Helper()
	for _, r := range rs {
		for _, id := range r.Batches {
			b, err := st.Batch(t.Context(), id)
			switch {
			case err != nil:
				t.Errorf("batch %d: %v", id, err)
			case b.FinishedAt.IsZero() || b.Total != 3 || b.Counts[kiln.Succeeded] != 3:
				t.Errorf("batch %d: finished at %v, total %d, counts %v", id, b.FinishedAt, b.Total, b.Counts)
			}
		}
	}
}

func duplicates(t *testing.T, r1, n1, r2 *result) {
	t.Helper()
	for _, d := range []struct {
		client string
		got    *result
		key    string
		holder []int64
	}{
		{"current", n1, "old-live", r1.Jobs["unique"]},
		{"current", n1, "old-window", r1.Jobs["window"]},
		{oldVersion, r2, "new-live", n1.Jobs["unique"]},
		{oldVersion, r2, "new-window", n1.Jobs["window"]},
	} {
		if len(d.holder) != 1 || d.got.Duplicates[d.key] != d.holder[0] {
			t.Errorf("the %s client enqueued unique key %q held by %v and got duplicate of %d", d.client, d.key, d.holder, d.got.Duplicates[d.key])
		}
	}
}

func rateHeld(t *testing.T, done map[int64]kiln.Record, ids []int64) {
	t.Helper()
	const (
		window = 200 * time.Millisecond
		stall  = 50 * time.Millisecond
		slack  = 5 * time.Millisecond
	)
	gap := time.Second / rate
	allowed := int(rate*window/time.Second) + 1
	var rs []kiln.Record
	for _, id := range ids {
		if r, ok := done[id]; ok {
			rs = append(rs, r)
		}
	}
	if len(rs) == 0 {
		t.Error("no compat.rated job succeeded")
		return
	}
	slices.SortFunc(rs, func(a, b kiln.Record) int { return a.RunAt.Compare(b.RunAt) })
	for i, r := range rs {
		if r.AttemptedAt.Before(r.RunAt.Add(-slack)) {
			t.Errorf("compat.rated job %d started %v before its slot", r.ID, r.RunAt.Sub(r.AttemptedAt))
		}
		if i > 0 && r.RunAt.Sub(rs[i-1].RunAt) < gap-slack {
			t.Errorf("slots of compat.rated jobs %d and %d are %v apart, want at least %v", rs[i-1].ID, r.ID, r.RunAt.Sub(rs[i-1].RunAt), gap)
		}
	}
	slices.SortFunc(rs, func(a, b kiln.Record) int { return a.AttemptedAt.Compare(b.AttemptedAt) })
	most, from := 0, 0
	for i, j := 0, 0; i < len(rs); i++ {
		for j < len(rs) && rs[j].AttemptedAt.Sub(rs[i].AttemptedAt) <= window {
			j++
		}
		if j-i > most {
			most, from = j-i, i
		}
	}
	first := rs[0].AttemptedAt
	if most > allowed {
		var late time.Duration
		for _, r := range rs[from : from+most] {
			late = max(late, r.AttemptedAt.Sub(r.RunAt))
		}
		if late >= stall {
			t.Logf("%d compat.rated jobs started within %s after a %v stall; every slot was kept", most, window, late.Round(time.Millisecond))
			return
		}
		var b strings.Builder
		for _, r := range rs {
			fmt.Fprintf(&b, "\n\tjob %d: slot %+dms, started %+dms on %s",
				r.ID, r.RunAt.Sub(first).Milliseconds(), r.AttemptedAt.Sub(first).Milliseconds(), side(r.Server))
		}
		t.Errorf("%d compat.rated jobs started within %s; %d per second allows %d:%s", most, window, rate, allowed, b.String())
	}
	t.Logf("%d compat.rated jobs started over %s, at most %d within %s",
		len(rs), rs[len(rs)-1].AttemptedAt.Sub(first).Round(time.Millisecond), most, window)
}

func clean(t *testing.T, sums ...*summary) {
	t.Helper()
	for _, s := range sums {
		if len(s.Errors) > 0 {
			t.Errorf("%s reported errors:\n%s", s.Server, strings.Join(s.Errors, "\n"))
		}
		if st := s.Stats; st == nil || st.Stale > 0 || st.Abandoned > 0 || st.Failed > 0 {
			t.Errorf("%s server stats: %+v", s.Server, st)
		}
		t.Logf("%s processed %v with %v handler calls", s.Server, s.Processed, s.Calls)
	}
}

type snapshot struct {
	versions map[int]string
	objects  map[string]string
	data     map[string]string
}

func inspect(t *testing.T, db backend, like *snapshot) *snapshot {
	t.Helper()
	ctx := t.Context()
	s := &snapshot{data: make(map[string]string)}
	var err error
	if s.versions, err = db.versions(ctx); err != nil {
		t.Fatalf("read migrations: %v", err)
	}
	if s.objects, err = db.objects(ctx); err != nil {
		t.Fatalf("read schema: %v", err)
	}
	if like == nil {
		like = s
	}
	for _, table := range dataTables {
		cols := columns(like.objects, table)
		if len(cols) == 0 {
			t.Fatalf("%s created no %s table", oldVersion, table)
		}
		if s.data[table], err = db.digest(ctx, table, cols); err != nil {
			t.Fatalf("digest %s: %v", table, err)
		}
	}
	return s
}

func columns(objects map[string]string, table string) []string {
	var cols []string
	for k := range objects {
		if c, ok := strings.CutPrefix(k, "column "+table+"."); ok {
			cols = append(cols, c)
		}
	}
	slices.Sort(cols)
	return cols
}

func additive(t *testing.T, db backend, before, after *snapshot) {
	t.Helper()
	if !maps.Equal(before.versions, after.versions) {
		t.Errorf("the versioned migrations went from %v to %v; %s refuses to start above version %d, so additive changes go in changes/",
			before.versions, after.versions, oldVersion, highest(before.versions))
	}
	for k, v := range before.objects {
		switch got, ok := after.objects[k]; {
		case !ok:
			t.Errorf("the current code dropped %s", k)
		case got != v:
			t.Errorf("the current code changed %s\n%s: %s\ncurrent: %s", k, oldVersion, v, got)
		}
	}
	for table, d := range before.data {
		if after.data[table] != d {
			t.Errorf("the current code rewrote rows of %s", table)
		}
	}
	var added []string
	for k, def := range after.objects {
		if _, ok := before.objects[k]; ok {
			continue
		}
		added = append(added, k)
		col, ok := strings.CutPrefix(k, "column ")
		table, _, _ := strings.Cut(col, ".")
		if ok && len(columns(before.objects, table)) > 0 && !strings.HasPrefix(def, "optional") {
			t.Errorf("the current code added %s with no default, and %s inserts rows without it: %s", k, oldVersion, def)
		}
	}
	slices.Sort(added)
	applied, err := db.changes(t.Context())
	if err != nil {
		t.Fatalf("read schema_changes: %v", err)
	}
	slices.Sort(applied)
	if want := changeNames(t, db.module()); !slices.Equal(applied, want) {
		t.Errorf("schema_changes lists %v, %s/changes has %v", applied, db.module(), want)
	}
	t.Logf("%s wrote schema version %d, the current code left it at %d, recorded changes %v and added %s",
		oldVersion, highest(before.versions), highest(after.versions), applied, strings.Join(added, ", "))
}

func highest(versions map[int]string) int {
	return slices.Max(slices.Collect(maps.Keys(versions)))
}

func unmigrated(t *testing.T, db backend) {
	t.Helper()
	pending := changeNames(t, db.module())
	if len(pending) == 0 {
		return
	}
	if err := opened(db.open(t.Context(), false)); err == nil || !strings.Contains(err.Error(), pending[0]) {
		t.Errorf("New with NoMigrate on the schema of %s returned %v, want an error naming change %s", oldVersion, err, pending[0])
	}
}

func changeNames(t *testing.T, store string) []string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join("..", store, "changes", "*.sql"))
	if err != nil {
		t.Fatal(err)
	}
	out := make([]string, len(files))
	for i, f := range files {
		out[i] = strings.TrimSuffix(filepath.Base(f), ".sql")
	}
	return out
}

func refuse(t *testing.T, bin string, db backend) {
	t.Helper()
	ctx := t.Context()
	vs, err := db.versions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	next := highest(vs) + 1
	if err := db.addVersion(ctx, next); err != nil {
		t.Fatalf("insert migration %d: %v", next, err)
	}
	for _, c := range []struct {
		name string
		call func() error
	}{
		{"New", func() error { return opened(db.open(ctx, true)) }},
		{"New with NoMigrate", func() error { return opened(db.open(ctx, false)) }},
		{"Migrate", func() error { return db.migrate(ctx) }},
	} {
		if err := c.call(); err == nil || !mentions(err.Error(), next) {
			t.Errorf("%s on a schema at version %d returned %v, want an error naming that version", c.name, next, err)
		}
	}
	sum, err := once(bin, append(db.flags(), "-role=enqueue")...)
	if err == nil || sum == nil || !mentions(strings.Join(sum.Errors, "\n"), next) {
		t.Errorf("%s against a schema at version %d: %v %+v, want it to refuse with an error naming that version", oldVersion, next, err, sum)
		return
	}
	t.Logf("%s refuses a schema at version %d: %s", oldVersion, next, strings.Join(sum.Errors, "; "))
}

func opened(_ driver.Store, closeStore func(), err error) error {
	if err == nil {
		closeStore()
	}
	return err
}

func mentions(msg string, v int) bool {
	digits := strings.FieldsFunc(msg, func(r rune) bool { return r < '0' || r > '9' })
	return strings.Contains(msg, "version") && slices.Contains(digits, strconv.Itoa(v))
}

func frozen(t *testing.T, mods map[string]string) {
	for _, store := range stores {
		released := mods[module+"/"+store]
		shipped := unchanged(t, store, released, "migrations")
		if len(shipped) == 0 {
			t.Fatalf("no migrations shipped with %s %s in %s", store, oldVersion, released)
		}
		unchanged(t, store, released, "changes")
		top := 0
		for _, name := range shipped {
			top = max(top, number(t, name))
		}
		current, _ := filepath.Glob(filepath.Join("..", store, "migrations", "*.sql"))
		for _, f := range current {
			name := filepath.Base(f)
			if slices.Contains(shipped, name) {
				continue
			}
			if n := number(t, name); n <= top {
				t.Errorf("%s/migrations/%s is numbered %d, not above %d, so databases created by %s would never run it", store, name, n, top, oldVersion)
			}
		}
	}
}

func unchanged(t *testing.T, store, released, dir string) []string {
	t.Helper()
	files, _ := filepath.Glob(filepath.Join(released, dir, "*.sql"))
	var shipped []string
	for _, f := range files {
		name := filepath.Base(f)
		shipped = append(shipped, name)
		want, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		switch got, err := os.ReadFile(filepath.Join("..", store, dir, name)); {
		case err != nil:
			t.Errorf("%s/%s/%s shipped with %s and is gone: %v", store, dir, name, oldVersion, err)
		case !bytes.Equal(got, want):
			t.Errorf("%s/%s/%s differs from the file shipped with %s; schema changes go in a new file", store, dir, name, oldVersion)
		}
	}
	return shipped
}

func number(t *testing.T, name string) int {
	t.Helper()
	prefix, _, _ := strings.Cut(name, "_")
	n, err := strconv.Atoi(prefix)
	if err != nil {
		t.Fatalf("migration %s has no number: %v", name, err)
	}
	return n
}

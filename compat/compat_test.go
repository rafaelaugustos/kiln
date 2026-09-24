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
	oldVersion = "v0.1.0"
	oldName    = "compat-v010"
	newName    = "compat-current"
)

var dataTables = []string{"jobs", "archive", "deps", "batches", "uniques", "recurring"}

func TestRollingUpgrade(t *testing.T) {
	bin := build(t)
	t.Run("migrations", func(t *testing.T) {
		frozen(t, bin.mods)
	})
	for _, b := range []struct {
		name string
		open func(*testing.T) backend
	}{
		{"postgres", newPostgres},
		{"mysql", newMySQL},
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

	old := start(t, bin, append(db.flags(), "-role=both", "-commands", "-name="+oldName,
		"-echo=10", "-fail=5", "-limited=4", "-flows=1", "-batches=1")...)
	alone := old.result()
	if !eventually(30*time.Second, func() bool {
		n, err := db.count(ctx, "TRUE")
		return err == nil && n == 0
	}) {
		t.Fatal("v0.1.0 did not finish the jobs it enqueued while running alone")
	}

	before := inspect(t, db, nil)
	st, closeStore, err := db.open(ctx, true)
	if err != nil {
		t.Fatalf("the current code cannot open a database created by v0.1.0: %v", err)
	}
	t.Cleanup(closeStore)
	additive(t, before, inspect(t, db, before))

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
	limit := peak()
	t.Logf("version column of the servers table: %v", reported(t, st))
	oldSum, exit := old.stop()
	if exit != nil {
		t.Errorf("v0.1.0 exited with %v", exit)
	}
	if !eventually(10*time.Second, func() bool { return srv.Stats().Leader }) {
		t.Error("the current server did not take over leadership after v0.1.0 stopped")
	}
	stop()
	newSum := w.report()
	stats := srv.Stats()
	newSum.Server, newSum.Stats, newSum.Errors = srv.ID(), &stats, errs.all()

	final := finished(t, client)
	counted(t, st, final, alone, r1, n1, r2)
	exactlyOnce(t, final, oldSum, newSum)
	retried(t, client, slices.Concat(alone.Jobs["fail"], r1.Jobs["fail"], n1.Jobs["fail"]))
	handedOff(t, client, oldVersion, r1.Jobs["handoff"])
	handedOff(t, client, version, n1.Jobs["handoff"])
	fromOld, fromNew := spread(final, r1.Jobs["echo"]), spread(final, n1.Jobs["echo"])
	if fromOld[oldVersion] == 0 || fromOld[version] == 0 {
		t.Errorf("compat.echo jobs of the v0.1.0 client were not completed by both servers: %v", fromOld)
	}
	t.Logf("compat.echo jobs of the v0.1.0 client ran on %v, of the current client on %v", fromOld, fromNew)
	batches(t, st, alone, r1, n1)
	duplicates(t, r1, n1, r2)
	if limit > 2 {
		t.Errorf("%d compat.limited jobs were enqueued or processing at once, the limit is 2", limit)
	}
	clean(t, oldSum, newSum)

	refuse(t, bin, db)
	t.Logf("%d jobs, limit peak %d, done in %s", len(final), limit, time.Since(began).Round(time.Millisecond))
}

func serve(t *testing.T, c *kiln.Client, m *kiln.Mux, errs *errlog) (*kiln.Server, func()) {
	t.Helper()
	srv, err := kiln.NewServer(c, m, kiln.ServerConfig{
		Queues:            map[string]int{kiln.DefaultQueue: 8},
		Name:              newName,
		PollInterval:      100 * time.Millisecond,
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

func exactlyOnce(t *testing.T, done map[int64]kiln.Record, oldSum, newSum *summary) {
	t.Helper()
	for id, r := range done {
		if o, n := oldSum.Runs[id], newSum.Runs[id]; o+n != 1 {
			t.Errorf("job %d (%s) succeeded %d times on v0.1.0 and %d times on the current code", id, r.Kind, o, n)
		}
	}
	for _, s := range []*summary{oldSum, newSum} {
		for id := range s.Runs {
			if _, ok := done[id]; !ok {
				t.Errorf("a %s handler succeeded for job %d, which did not end succeeded", s.Version, id)
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

func clean(t *testing.T, sums ...*summary) {
	t.Helper()
	for _, s := range sums {
		if len(s.Errors) > 0 {
			t.Errorf("%s reported errors:\n%s", s.Version, strings.Join(s.Errors, "\n"))
		}
		if st := s.Stats; st == nil || st.Stale > 0 || st.Abandoned > 0 || st.Failed > 0 {
			t.Errorf("%s server stats: %+v", s.Version, st)
		}
		t.Logf("%s processed %v with %v handler calls", s.Version, s.Processed, s.Calls)
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
			t.Fatalf("v0.1.0 created no %s table", table)
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

func additive(t *testing.T, before, after *snapshot) {
	t.Helper()
	top := slices.Max(slices.Collect(maps.Keys(before.versions)))
	for v, at := range before.versions {
		if after.versions[v] != at {
			t.Errorf("migration %d, applied by v0.1.0 at %s, now reads %q", v, at, after.versions[v])
		}
	}
	var added []int
	for v := range after.versions {
		if _, ok := before.versions[v]; ok {
			continue
		}
		if v < top {
			t.Errorf("the current code applied migration %d below the %d written by v0.1.0", v, top)
		}
		added = append(added, v)
	}
	for k, v := range before.objects {
		switch got, ok := after.objects[k]; {
		case !ok:
			t.Errorf("the current migrations dropped %s", k)
		case got != v:
			t.Errorf("the current migrations changed %s\nv0.1.0:  %s\ncurrent: %s", k, v, got)
		}
	}
	for table, d := range before.data {
		if after.data[table] != d {
			t.Errorf("the current migrations rewrote rows of %s", table)
		}
	}
	slices.Sort(added)
	t.Logf("v0.1.0 wrote schema version %d; the current code applied %v and added %d objects", top, added, len(after.objects)-len(before.objects))
}

func refuse(t *testing.T, bin string, db backend) {
	t.Helper()
	ctx := t.Context()
	vs, err := db.versions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	next := slices.Max(slices.Collect(maps.Keys(vs))) + 1
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
		if err := c.call(); err == nil || !names(err.Error(), next) {
			t.Errorf("%s on a schema at version %d returned %v, want an error naming that version", c.name, next, err)
		}
	}
	sum, err := once(bin, append(db.flags(), "-role=enqueue")...)
	switch {
	case err == nil:
		t.Logf("v0.1.0 has no newer-schema check: it started against version %d", next)
	case sum != nil && names(strings.Join(sum.Errors, "\n"), next):
		t.Logf("v0.1.0 refuses a schema at version %d: %s", next, strings.Join(sum.Errors, "; "))
	default:
		t.Errorf("v0.1.0 against a schema at version %d: %v %+v", next, err, sum)
	}
}

func opened(_ driver.Store, closeStore func(), err error) error {
	if err == nil {
		closeStore()
	}
	return err
}

func names(msg string, v int) bool {
	digits := strings.FieldsFunc(msg, func(r rune) bool { return r < '0' || r > '9' })
	return strings.Contains(msg, "version") && slices.Contains(digits, strconv.Itoa(v))
}

func frozen(t *testing.T, mods map[string]string) {
	for _, store := range []string{"pgstore", "mysqlstore"} {
		dir := filepath.Join(mods["github.com/rafaelaugustos/kiln/"+store], "migrations")
		shipped, _ := filepath.Glob(filepath.Join(dir, "*.sql"))
		if len(shipped) == 0 {
			t.Fatalf("no migrations shipped with %s v0.1.0 in %s", store, dir)
		}
		top := 0
		for _, f := range shipped {
			name := filepath.Base(f)
			top = max(top, number(t, name))
			want, err := os.ReadFile(f)
			if err != nil {
				t.Fatal(err)
			}
			switch got, err := os.ReadFile(filepath.Join("..", store, "migrations", name)); {
			case err != nil:
				t.Errorf("%s/migrations/%s shipped with v0.1.0 and is gone: %v", store, name, err)
			case !bytes.Equal(got, want):
				t.Errorf("%s/migrations/%s differs from the file shipped with v0.1.0; schema changes go in a new file", store, name)
			}
		}
		current, _ := filepath.Glob(filepath.Join("..", store, "migrations", "*.sql"))
		for _, f := range current {
			name := filepath.Base(f)
			if _, err := os.Stat(filepath.Join(dir, name)); err == nil {
				continue
			}
			if n := number(t, name); n <= top {
				t.Errorf("%s/migrations/%s is numbered %d, not above %d, so databases created by v0.1.0 would never run it", store, name, n, top)
			}
		}
	}
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

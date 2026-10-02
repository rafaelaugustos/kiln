# compat

A rolling-upgrade test for kiln. It runs the previous published release and the code in this
repository against the same database at the same time, the way they meet during a deploy.

## The rule it protects

Release vX.Y must interoperate with the previous release on the same database. During a rolling
deploy both versions enqueue, claim, retry and finish each other's jobs, and a server of the
previous release can restart after the new one has upgraded the schema. So:

- Schema files that shipped in a release never change. `migrations/001_init.sql` and every file
  in `changes/` are compared byte for byte with the published module.
- Additive changes go in a new file in the store's `changes/` directory (`NNN_name.sql`): new
  tables, new indexes, and new columns that are nullable or have a default (a metadata-only
  `ADD COLUMN` on PostgreSQL, `ALGORITHM = INSTANT` on MySQL, a plain `ADD COLUMN` on SQLite).
  Each file is applied once, in name order, under the migration lock, and recorded by name in the
  `schema_changes` table. Older releases never read that table, so they keep starting and working
  after the upgrade. `New` with `NoMigrate` refuses to open a database that lacks a known change.
- Only a breaking change bumps the versioned `migrations` table: dropping, renaming or retyping
  something the previous release still uses, or rewriting its rows. Every release refuses to start
  when the table is above the newest version it knows, so a bump locks the previous release out,
  including its processes that restart in the middle of the deploy. A breaking change therefore
  waits until no supported release reads the old shape (expand now, contract later), and its
  release notes must say that the previous release cannot run next to it.

## Layout

- `v031/` is its own module, built only from the published `kiln v0.3.1`, `pgstore/v0.3.1`,
  `mysqlstore/v0.3.1` and `sqlitestore/v0.3.1` as served by the module proxy, with no replace
  directives. Its program opens a store (PostgreSQL, MySQL, or a SQLite file through
  modernc.org/sqlite with `busy_timeout`, WAL and `_txlock=immediate`), optionally runs a worker,
  enqueues jobs from flags and from JSON lines on stdin, and prints a JSON summary when it exits:
  jobs processed per kind, handler runs per job, server stats and errors.
- This module replaces `kiln`, `pgstore`, `mysqlstore` and `sqlitestore` with the directories in
  this repository, so the test runs the current code. Both sides register the same kinds with the
  same behaviour and the same insert options, and both clients enqueue every kind:
  `compat.echo`, `compat.fail_once`, `compat.child`, `compat.handoff`, `compat.limited`, which
  shares `Limit{Key: "compat", Max: 2}`, and `compat.rated`, which shares
  `Limit{Key: "compat.rate", Rate: 20, Per: time.Second, Burst: 1}`, a key with a rate and no
  concurrency limit.

## What the test does

For PostgreSQL, MySQL and SQLite. v0.3.1 always runs as a separate process; for SQLite the
current code opens the same database file in the test process.

1. Builds the v0.3.1 binary once with `GOWORK=off go build`.
2. Starts it against a fresh schema, database or file. It creates the schema, applies the changes
   it ships, enqueues jobs and works through them alone. Among them are `compat.rated` jobs, so
   the rate key already holds the slot state v0.3.1 wrote when the current code arrives.
3. Snapshots the versioned migrations, the rows of `schema_changes`, the schema, the storage
   identity of every table and the rows of the data tables (jobs, archive, deps, batches, uniques,
   recurring and limits). `New` with `NoMigrate` must refuse the database with an error naming the
   first file in the store's `changes/` that v0.3.1 did not apply (`002_admit_gate`), and `New`
   then applies the pending changes while v0.3.1 keeps running. They must be additive: the test
   fails if the versioned migrations changed (they must still end at version 1), if
   `schema_changes` lost or rewrote a row or does not list exactly the files in `changes/`, if an
   existing object was dropped, retyped or otherwise changed, if a table was rebuilt or its rows
   rewritten, or if a column added to an existing table is required, since v0.3.1 inserts rows
   without it.
4. Starts a server and a client of the current code in the test process next to v0.3.1. Both
   clients enqueue plain jobs, retries, flows with `Needs`, batches with `Then`, continuations on
   the other version's jobs, unique jobs on the other version's keys, and jobs that share one
   concurrency `Limit` of 2.
5. Stops v0.3.1, checks that the current server takes over leadership, and restarts v0.3.1
   against the database the current code has been running on. It must start and keep processing:
   both clients enqueue another round, and `compat.handoff` jobs of the current client can only
   finish on the restarted v0.3.1.
6. With both versions running, both clients enqueue `compat.rated` jobs on the shared rate key at
   the same time, three rounds of four jobs each, so the slots they reserve interleave. All of
   them must complete, and the rate must hold across both versions: no job starts before the slot
   reserved for it, reserved slots are at least 50ms apart whichever client reserved them, and no
   200ms window holds more than 5 starts. That last check is waived only when the densest window
   contains a start that was 50ms or more late, the sign of an environment stall that held jobs
   past their slots and let them start together afterwards; the first two checks still apply.
7. Checks that every job succeeded exactly once across the three processes, that plain jobs from
   the v0.3.1 client ran on both servers, that `compat.handoff` retries scheduled by one version
   were picked up by the other, that duplicates resolved to the other version's job, that the
   concurrency limit peaked at exactly 2 across both versions, and that no process logged a store
   error.
8. Inserts a migration row above the latest one and checks that the current code (`New`, `New`
   with `NoMigrate` and `Migrate`) and v0.3.1 all refuse the database with an error naming that
   version.
9. Drops the schema or database; the SQLite file lives in a temporary directory.

The `shipped` subtest compares every schema file that each of the three stores shipped with
v0.3.1, `migrations/001_init.sql` and the rate-limit change in `changes/`, with the file of the
same name in this repository, byte for byte, and checks that new migration files are numbered
above them.

## Running

    cd compat
    GOWORK=off go test -v -count=1 ./...

`GOWORK=off` is needed locally because the repository's `go.work` does not include this module.
The databases come from `KILN_DATABASE_URL` (default
`postgres://kiln:kiln@localhost:55432/kiln?sslmode=disable`) and `KILN_MYSQL_DSN` (default
`kiln:kiln@tcp(localhost:53306)/kiln`); the MySQL user needs to create databases and read
`information_schema.INNODB_TABLES`. A backend that cannot be reached is skipped, SQLite always
runs, and the whole test is skipped when the v0.3.1 modules cannot be downloaded. The backends run
in parallel and each takes about ten seconds.

Both versions poll every 20ms in the test, below the 50ms between rate-limited starts. The test
configures no bus, so MySQL has no notifications and both versions find a job admitted at insert
by polling. With a longer poll interval such a job waits for the next poll and can start together
with the next reserved slot; that is polling latency, not a compatibility problem, and the short
interval keeps it out of the rate check.

Both versions also run with a 250ms heartbeat, and their claim, heartbeat and promote calls time
out after that long. A stall of the database or of the machine longer than that makes a server
log a store error ending in `context deadline exceeded`, often on both versions at the same
moment, and a claim that timed out after the database committed it comes back as a lost claim. CI
runners stall like that now and then, so step 7 logs these timeouts instead of failing on them,
and skips the stale-outcome check for a server that had them. Any other error a server logs still
fails the test, and so does a job that runs twice or not at all.

The old program can be run by hand too:

    cd compat/v031
    GOWORK=off go build -o /tmp/kiln-v031 .
    /tmp/kiln-v031 -backend=postgres -dsn="$KILN_DATABASE_URL" -schema=kiln_try -role=both -echo=20 -fail=5 -rated=10 -duration=5s
    /tmp/kiln-v031 -backend=sqlite -dsn=/tmp/kiln-try.db -role=both -echo=20 -fail=5 -rated=10 -duration=5s

## After the next release

When v0.4.0 is tagged, replace `v031/` with `v040/`: copy it, require the v0.4.0 modules, run
`GOWORK=off go mod tidy` and `GOWORK=off go mod verify` there, and point `oldVersion`, `oldDir`
and `oldName` in `compat_test.go` at it.

Step 3 expects no schema change at all because none was made after v0.3.1. When a store gains a
file in `changes/`, relax that check for what the file adds: new tables and indexes, and new
columns that are nullable or have a default, since the previous release inserts rows without them.
`schema_changes` must then gain exactly the new file names, and `New` with `NoMigrate` must refuse
the old database with an error naming the first missing change. The v0.2.0 version of this test
checked exactly that and is in the history of this directory.

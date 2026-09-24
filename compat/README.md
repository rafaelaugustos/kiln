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

- `v020/` is its own module, built only from the published `kiln v0.2.0`, `pgstore/v0.2.0`,
  `mysqlstore/v0.2.0` and `sqlitestore/v0.2.0`, with no replace directives. Its program opens a
  store (PostgreSQL, MySQL, or a SQLite file through modernc.org/sqlite with `busy_timeout`, WAL
  and `_txlock=immediate`), optionally runs a worker, enqueues jobs from flags and from JSON lines
  on stdin, and prints a JSON summary when it exits: jobs processed per kind, handler runs per
  job, server stats and errors.
- This module replaces `kiln`, `pgstore`, `mysqlstore` and `sqlitestore` with the directories in
  this repository, so the test runs the current code. Both sides register the same kinds with the
  same behaviour: `compat.echo`, `compat.fail_once`, `compat.child`, `compat.handoff`,
  `compat.limited` and `compat.rated`. Only the current client enqueues `compat.rated`, with
  `Limit{Key: "compat.rate", Rate: 20, Per: time.Second, Burst: 1}`, which v0.2.0 cannot express.

## What the test does

For PostgreSQL, MySQL and SQLite. v0.2.0 always runs as a separate process; for SQLite the
current code opens the same database file in the test process.

1. Builds the v0.2.0 binary once with `GOWORK=off go build`.
2. Starts it against a fresh schema, database or file. It creates the schema, enqueues jobs and
   works through them alone.
3. Snapshots the schema, the storage identity of every table and the rows, checks that the
   current code with `NoMigrate` refuses the database with an error naming the first missing
   change, then opens it with the current code, which applies its changes while v0.2.0 keeps
   running. It fails
   if anything that existed before was dropped, retyped or otherwise changed, if a table was
   rebuilt or its rows rewritten, if a column was added to an existing table without a default, if
   the versioned migrations changed (they must still end at version 1), or if `schema_changes`
   does not list exactly the files in the store's `changes/` directory.
4. Starts a server and a client of the current code in the test process next to v0.2.0. Both
   clients enqueue plain jobs, retries, flows with `Needs`, batches with `Then`, continuations on
   the other version's jobs, unique jobs on the other version's keys, and jobs that share one
   concurrency `Limit` of 2.
5. Stops v0.2.0, checks that the current server takes over leadership, and restarts v0.2.0
   against the upgraded database. It must start and keep processing: both clients enqueue another
   round, and `compat.handoff` jobs of the current client can only finish on the restarted v0.2.0.
6. Enqueues 20 `compat.rated` jobs from the current client on a key with a rate and no
   concurrency limit. All of them must complete, v0.2.0 must start none of them, and their starts
   must respect the rate: at most 5 in any 200ms.
7. Checks that every job succeeded exactly once across the three processes, that plain jobs from
   the v0.2.0 client ran on both servers, that `compat.handoff` retries scheduled by one version
   were picked up by the other, that duplicates resolved to the other version's job, that the
   concurrency limit peaked at exactly 2 across both versions, and that no process logged a store
   error.
8. Inserts a migration row above the latest one and checks that the current code (`New`, `New`
   with `NoMigrate` and `Migrate`) and v0.2.0 all refuse the database with an error naming that
   version.
9. Drops the schema or database; the SQLite file lives in a temporary directory.

The `shipped` subtest compares every file in `migrations/` and `changes/` shipped with v0.2.0
with the file of the same name in this repository, byte for byte, and checks that new migration
files are numbered above them.

## Running

    cd compat
    GOWORK=off go test -v -count=1 ./...

`GOWORK=off` is needed locally because the repository's `go.work` does not include this module.
The databases come from `KILN_DATABASE_URL` (default
`postgres://kiln:kiln@localhost:55432/kiln?sslmode=disable`) and `KILN_MYSQL_DSN` (default
`kiln:kiln@tcp(localhost:53306)/kiln`); the MySQL user needs to create databases and read
`information_schema.INNODB_TABLES`. A backend that cannot be reached is skipped, SQLite always
runs, and the whole test is skipped when the v0.2.0 modules cannot be downloaded. The backends run
in parallel and each takes about ten seconds.

Both versions poll every 20ms in the test, below the 50ms between rate-limited starts. MySQL has
no notifications, so with a longer poll interval a job admitted at insert waits for the next poll
and can start together with the next reserved slot; that is polling latency, not a compatibility
problem, and the test keeps it out of the rate check.

The old program can be run by hand too:

    cd compat/v020
    GOWORK=off go build -o /tmp/kiln-v020 .
    /tmp/kiln-v020 -backend=postgres -dsn="$KILN_DATABASE_URL" -schema=kiln_try -role=both -echo=20 -fail=5 -duration=5s
    /tmp/kiln-v020 -backend=sqlite -dsn=/tmp/kiln-try.db -role=both -echo=20 -fail=5 -duration=5s

## After the next release

When v0.3.0 is tagged, replace `v020/` with `v030/`: copy it, require the v0.3.0 modules, run
`GOWORK=off go mod tidy` there, and point `oldVersion`, `oldDir` and `oldName` in
`compat_test.go` at it. v0.3.0 servers admit rate-limited jobs themselves, so from then on the
old binary registers the rate limit on `compat.rated` too and the check that it starts none of
them goes away; the rate check stays.

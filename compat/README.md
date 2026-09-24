# compat

A rolling-upgrade test for kiln. It runs the last published release and the code in this
repository against the same database at the same time, the way they meet during a deploy.

## The rule it protects

A v0.X server must work side by side with v0.(X-1) servers on the same database. During a
deploy both versions enqueue, claim, retry and finish each other's jobs, so:

- Migration files that shipped in a release never change. A schema change is a new file with a
  higher number (`002_...sql`).
- New files only add things: tables, indexes, functions, nullable columns or columns with a
  default. Dropping, renaming or retyping anything an older version still uses, or rewriting its
  rows, waits until no supported version reads it (expand now, contract in a later release).
- A binary refuses to start when the database is at a newer schema version than it knows, and
  v0.1.0 already does this. Once a newer release has applied a new migration, v0.1.0 processes
  that are already running keep working, but one that restarts exits. Plan rollbacks with that
  in mind.

## Layout

- `v010/` is its own module, pinned to the published `kiln v0.1.0`, `pgstore/v0.1.0` and
  `mysqlstore/v0.1.0` with no replace directives. Its program opens a store, optionally runs a
  worker, enqueues jobs from flags and from JSON lines on stdin, and prints a JSON summary when
  it exits: jobs processed per kind, handler runs per job, server stats and errors.
- This module replaces `kiln`, `pgstore` and `mysqlstore` with `../`, `../pgstore` and
  `../mysqlstore`, so the test runs the current code. Both sides register the same kinds with
  the same behaviour: `compat.echo`, `compat.fail_once`, `compat.child`, `compat.handoff` and
  `compat.limited`.

## What the test does

For PostgreSQL and for MySQL:

1. Builds the v0.1.0 binary once with `GOWORK=off go build`.
2. Starts it against a fresh schema (PostgreSQL) or database (MySQL). It runs the v0.1.0
   migrations, enqueues jobs and works through them alone.
3. Snapshots the schema and the rows, opens the database with the current code, which runs its
   migrations, and fails if anything that existed before was dropped, changed or rewritten, or if
   the version rows v0.1.0 wrote changed.
4. Starts a server and a client of the current code in the test process while v0.1.0 keeps
   running. Both clients enqueue plain jobs, retries, flows with `Needs`, batches with `Then`,
   continuations on the other version's jobs, unique jobs on the other version's keys, and jobs
   that share one `Limit` key.
5. Checks that every job succeeded exactly once, that plain jobs from the v0.1.0 client ran on
   both servers, that `compat.handoff` retries scheduled by one version were picked up by the
   other, that duplicates resolved to the other version's job, that the limit held across both
   versions, that the current server takes over leadership when v0.1.0 stops, and that neither
   side logged a store error.
6. Inserts a migration row above the latest one and checks that the current code refuses to open
   the database with an error naming that version. It records whether v0.1.0 refuses as well.
7. Drops the schema or database.

The `migrations` subtest compares every migration file shipped with v0.1.0 with the file of the
same name in this repository, byte for byte, and checks that new files are numbered above them.

## Running

    cd compat
    GOWORK=off go test -v -count=1 ./...

`GOWORK=off` is needed locally because the repository's `go.work` does not include this module.
The databases come from `KILN_DATABASE_URL` (default
`postgres://kiln:kiln@localhost:55432/kiln?sslmode=disable`) and `KILN_MYSQL_DSN` (default
`kiln:kiln@tcp(localhost:53306)/kiln`). A backend that cannot be reached is skipped, and the whole
test is skipped when the v0.1.0 modules cannot be downloaded. Each backend takes under ten
seconds.

The old program can be run by hand too:

    cd compat/v010
    GOWORK=off go build -o /tmp/kiln-v010 .
    /tmp/kiln-v010 -backend=postgres -dsn="$KILN_DATABASE_URL" -schema=kiln_try -role=both -echo=20 -fail=5 -duration=5s

## After the next release

When v0.2.0 is tagged, add `v020/` pinned to it (a copy of `v010/` with the versions changed),
point the test at it, and keep `v010/` only while v0.1 servers are still supported.

# Changelog

Every module in this repository (`kiln`, `pgstore`, `mysqlstore`, `sqlitestore`, `kilnotel`, `redisbus`) is
released together under the same version. The GitHub releases have the full notes.

## v0.4.0 (2026-10-02)

### Added
- `kilnotel` module: OpenTelemetry spans from the enqueue to the handler, job metrics, and server and queue
  gauges.
- `driver.Bus` and the `redisbus` module, which carries wakeups over Redis Pub/Sub so MySQL and SQLite
  servers in other processes start new jobs within milliseconds instead of on their next poll.
- `Job.RunAt`, the time the job became due.
- `driver.TxWriter`, the interface of every store's transactional writer, `Notify` included.
- `pgstore.Store.SQLTx` for applications on `database/sql` with pgx's stdlib driver.
- Doc comments on the exported API of `kiln`, `driver`, `cron`, `dashboard`, `kilntest`, `kilnotel` and
  `redisbus`.

### Changed
- Requeueing a failed, succeeded or deleted job starts it over with all of its attempts, instead of giving
  it one more (#5).
- The first time a job is rescued from a dead server it goes straight back to its queue; only a job rescued
  twice in a row waits for its backoff (#6). `driver.Orphan` gains `LastReason`.
- A lost claim is requeued without using up an attempt.
- Large counts on the dashboard overview are shortened (167.4k, 1.2M) so they fit their cards.

### Fixed
- Rate-limited jobs no longer start together after admission stalls: a second GCRA state per key,
  `admit_tat`, lets `Burst` of them start and gives the rest new slots (#1). On PostgreSQL, admission no
  longer waits on limits rows, which removes the deadlocks between admit statements and `Finish`.
- Limited jobs enqueued through a `memstore.Tx` are admitted when it commits (#2).
- A server no longer requeues a job as lost while the claim that took it is still on its way back, which
  could run the job twice (#4).
- `kilntest` copies `RunAt`, and `kilnotel` no longer counts a bare `ErrSnoozed` as a snooze.

### Upgrading
- Additive schema change `002_admit_gate`: a nullable `admit_tat` column on the limits table. v0.3 servers
  keep running next to this version during a rolling deploy.

## v0.3.1 (2026-09-25)

- A server that gets no notifications from its store looks for due jobs every 100ms instead of every
  `PollInterval`, so reserved rate-limit slots, delayed jobs and retries start on time on MySQL and on
  PostgreSQL without a working `LISTEN` connection.

## v0.3.0 (2026-09-24)

- Rate limits: `kiln.Limit` takes `Rate`, `Per` and `Burst` next to `Max`. Start times are reserved when a
  job is admitted (GCRA), so a backlog is released at the configured pace and each job is written once.
- Additive schema changes are recorded in a new `schema_changes` table; v0.2 servers keep running during a
  rolling deploy.
- `driver.InsertParams` gains `LimitRate`, `LimitPer` and `LimitBurst`, and the conformance suite a `Rate`
  group.
- READMEs for `pgstore`, `mysqlstore` and `sqlitestore`.

## v0.2.0 (2026-09-24)

- `sqlitestore`, for any `database/sql` SQLite driver.
- `mysqlstore` joins the regular release.
- Go 1.27 is the minimum version; pgx v5.11.0.
- The `compat` module runs the previous release next to the new one on the same database.
- `driver.CheckInsert` and `driver.CheckFilter` validate inputs the same way for every store.
- Servers report the kiln version they run.

## v0.1.0 (2026-09-24)

First public release: fire-and-forget, delayed and recurring jobs, continuations, flows and batches,
retries, snoozes and cancellation, unique jobs and concurrency limits, transactional enqueue, the
dashboard, `pgstore`, and the `drivertest` conformance suite.

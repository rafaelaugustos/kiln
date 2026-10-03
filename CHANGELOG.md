# Changelog

Every module in this repository (`kiln`, `pgstore`, `mysqlstore`, `sqlitestore`, `kilnotel`, `redisbus`) is
released together under the same version. The GitHub releases have the full notes.

## v0.6.0 (2026-10-02)

### Added
- Job console: `Job.Logf` and `Job.SetProgress` write log lines and a progress bar that the dashboard
  shows live on the job's page, grouped by attempt. Stores implement the optional `driver.Console`;
  `kilntest.Result` carries the lines and the progress.
- `Client.SyncRecurring` keeps a group of recurring jobs in step with the code: it sets the ones it's
  given and removes the rest of the group. `driver.Recurring` gains `Group`.
- A Limits page in the dashboard, and `GET /api/limits`: per key, the rule and how many jobs are active,
  throttled and holding a reserved start. Stores implement the optional `driver.LimitReader`.

### Upgrading
- Additive schema changes `003_console` and `004_recurring_group`; v0.5 servers keep running next to v0.6
  during a rolling deploy.

## v0.5.0 (2026-10-02)

### Added
- `mssqlstore`: SQL Server 2019+ and Azure SQL, on any `*sql.DB` from `go-mssqldb`, with an optional
  `driver.Bus` for wakeups. It passes the same conformance suite as the other stores; the database needs
  `READ_COMMITTED_SNAPSHOT`.

### Changed
- The stores agree on a few edge cases, each pinned by a conformance case: `Series` leaves out the bucket
  at `to`, a `Finish` outcome whose State isn't one of the five is Rejected, `Output` is kept only for
  succeeded and deleted jobs, and a limit of 0 or less counts as 1.
- Jobs that a transactional writer's `SealBatch` releases are admitted by `Notify`, like inserted ones,
  instead of waiting for the sweep (pgstore, mysqlstore).
- `Close` can be called more than once on every store.

### Fixed
- sqlitestore reads SQLite's error codes from any driver, so an outcome SQLite refuses is Rejected with
  mattn and ncruces too.
- memstore's `Sweep` respects its limit, and its `Tx` keeps a `UniqueFor` window from the insert.
- mysqlstore's `Delete`, `Requeue` and `Prune` count only work that committed.

## v0.4.1 (2026-10-02)

### Fixed
- mysqlstore: an insert no longer waits behind a limits row that another transaction holds. Only an insert
  that changes that key's rule does. A held row used to stall every insert in the process, with or without
  a limit.
- Delete and `Finish` no longer deadlock over a limits row when a job is deleted while its parent finishes
  (pgstore and mysqlstore).

### Documentation
- Doc comments on the store packages: `memstore`, `drivertest`, `pgstore`, `mysqlstore` and `sqlitestore`.
- `sqlitestore` needs SQLite 3.38 or later; it was documented as 3.35.

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

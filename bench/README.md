# kiln vs River

This module compares kiln (`pgstore`) with [River](https://github.com/riverqueue/river), a PostgreSQL
job queue for Go, on the same machine, database and workloads. It is a separate module so that River,
and the Go 1.26 toolchain it requires, never become dependencies of kiln; `replace` directives point it
at the kiln code in this repository.

## Running

```
cd bench
go run . -rounds 5
```

River v0.47 requires Go 1.26. With the default `GOTOOLCHAIN=auto` the go command downloads it. The
module is intentionally not part of the repository's local `go.work`, so if you have one, run with
`GOWORK=off`.

The database defaults to `postgres://kiln:kiln@localhost:55432/kiln?sslmode=disable`; override it with
`-db` or `KILN_DATABASE_URL`. The program creates and drops schemas named `bench_*` and runs
`VACUUM ANALYZE` and `CHECKPOINT`, so point it at a scratch database with a superuser role. `-only 3,4`
and `-libs kiln,river` narrow the run, `-json file` writes the raw per-round numbers, and `-h` lists the
workload sizes. The run below (warm-up plus 5 rounds) took 11m38s; most of that is River draining at its
default fetch rate.

## Scenarios

1. **Bulk insert.** 100,000 jobs in 10 calls of 10,000: kiln `EnqueueMany`, River `InsertMany` and
   River `InsertManyFast`.
2. **Concurrent single inserts.** 8 goroutines, each inserting 2,000 jobs one call at a time, after 5
   unmeasured inserts each to open connections. Throughput over wall time; p50/p99 over the 16,000
   calls.
3. **Drain, no-op handler.** 50,000 pre-inserted jobs, one worker process with 100 workers, time until
   all are completed.
4. **Enqueue to handler start.** 520 single enqueues on a fixed 10ms schedule (open loop) against an
   idle, running worker process with 100 workers; the first 20 are discarded. Latency runs from just
   before the insert call to the first line of the handler. LISTEN/NOTIFY wakeups are on, as they are by
   default in both libraries.
5. **Drain, 1ms handler.** 20,000 pre-inserted jobs whose handler sleeps 1ms, 100 workers.

## Method

- Both libraries run the same job: kind `bench`, queue `default`, args `{}` (`{"seq":n}` in scenario
  4), and the same handler function, called after each library decodes the args from JSON.
- Every run gets a fresh schema: kiln migrates it through `pgstore.Schema`, River through `rivermigrate`
  with `Config.Schema`. The schema is dropped afterwards.
- Before every run the harness runs `VACUUM ANALYZE` on the database and `CHECKPOINT`. In the drain
  scenarios the jobs are inserted with each library's bulk insert, then the job table gets its own
  `VACUUM ANALYZE` and another `CHECKPOINT` before the clock starts.
- Drains are timed from just before starting the worker process (`Server.Run`, `Client.Start`).
  Completion is detected the same way for both: a separate connection counts completed rows every 50ms
  (kiln: `archive` rows in state `succeeded`; River: `river_job` rows in state `completed`) and the
  clock stops at the first poll that sees all of them. That overstates every drain by 25ms on average
  and quantizes it to 50ms steps, which is visible in kiln's scenario 5 drains (about 1s each): its
  five values sit at 1.00s, 1.00s, 1.00s, 1.15s and 1.30s.
- One warm-up round is discarded, then 5 rounds are measured. The order of the libraries alternates
  every round (A B, B A, A B, ...). Tables show the median and the min–max over the 5 rounds.
- Both libraries run with default configuration except for the worker count (100) and a warn-level
  logger. Each gets its own `pgxpool.New(url)` with default settings: River uses that pool as is (16
  connections on this machine); pgstore copies its configuration into its own pool capped at 8
  connections, its default.
- River's default `FetchCooldown` of 100ms decides scenarios 3 to 5 by itself, so those scenarios also
  run River with `FetchCooldown: 1ms`, the lowest value it accepts. That row is not River's default; it
  is there to separate configuration from implementation.

## Defaults that matter

|                   | kiln | River |
|-------------------|------|-------|
| Fetch cooldown    | 5ms, skipped when the previous claim was full and at least half of the workers are free | 100ms (`FetchCooldown`) |
| Poll interval     | 1s | 1s (`FetchPollInterval`) |
| Jobs per fetch    | free workers, up to min(workers, 200); the next claim is issued as soon as half of the workers are free | free workers (`MaxWorkers` minus running); after a full fetch, the next one waits for a job to finish |
| Insert wakeup     | `pg_notify` after every insert that enqueued jobs, coalesced and sent after the insert returns | `NOTIFY` inside the insert transaction, at most once per `FetchCooldown` per queue per client |
| Single insert     | one autocommit statement | `BEGIN`, `INSERT`, `COMMIT` |
| Completion        | one goroutine flushes outcomes back to back, up to 256 per statement; finished jobs move from `jobs` to `archive` | batch completer ticks every 50ms and flushes at 100 pending jobs or every fifth tick (250ms), up to 5,000 per statement; rows are updated in place |
| Connection pool   | own pool, 8 connections | the pool passed in, default max(4, NumCPU) |

## Environment

- Apple M4 Max (12 performance and 4 efficiency cores), 64 GB, macOS 26.6.2.
- PostgreSQL 17.10 (`postgres:17-alpine`) in Docker on the same machine (OrbStack VM, 16 vCPUs, 16 GB),
  `shared_buffers=256MB`, `max_connections=300`, everything else default, including `fsync` and
  `synchronous_commit`. Commits are comparatively slow here: a claim transaction takes about 2.3ms with
  `synchronous_commit` on and about 0.9ms with it off.
- Go 1.26.0, River v0.47.0 with riverpgxv5 v0.47.0, pgx v5.10.0, kiln from this repository's working
  tree (v0.1.0 plus local changes).
- The host was not idle: other containers and development processes were running throughout. Scenario
  2, which is bound by commit latency, was the most affected (see below).

## Results

Median of 5 rounds, min–max in parentheses. Raw per-round values are in `results.json`.

| # | Scenario | Library | jobs/s | p50 ms | p99 ms |
|---|---|---|--:|--:|--:|
| 1 | bulk insert, 100k in calls of 10k | kiln `EnqueueMany` | 251,527 (231,787–258,516) | | |
| 1 | | River `InsertMany` | 134,124 (129,727–135,466) | | |
| 1 | | River `InsertManyFast` | 222,387 (209,693–231,567) | | |
| 2 | concurrent single inserts, 8 x 2,000 | kiln | 5,125 (1,898–5,580) | 1.49 (1.37–3.04) | 2.51 (2.19–11.12) |
| 2 | | River | 4,433 (2,358–4,712) | 1.65 (1.56–2.59) | 4.67 (2.79–9.55) |
| 3 | drain 50k no-op jobs, 100 workers | kiln | 21,235 (16,370–22,679) | | |
| 3 | | River | 997 (997–997) | | |
| 3 | | River, `FetchCooldown` 1ms | 20,288 (19,123–22,083) | | |
| 4 | enqueue to handler start, 10ms apart | kiln | | 3.34 (2.91–3.42) | 6.56 (5.94–21.56) |
| 4 | | River | | 55.48 (54.41–56.01) | 106.04 (105.22–166.51) |
| 4 | | River, `FetchCooldown` 1ms | | 4.67 (4.22–4.90) | 10.73 (7.84–15.20) |
| 5 | drain 20k jobs, 1ms handler, 100 workers | kiln | 19,941 (15,356–19,960) | | |
| 5 | | River | 997 (995–1,000) | | |
| 5 | | River, `FetchCooldown` 1ms | 13,700 (12,817–14,717) | | |

## What the numbers say

**Bulk insert.** kiln inserts 1.9 times as fast as River's `InsertMany` and 13% faster than
`InsertManyFast`. `InsertMany` returns the full job row for every job and goes through `ON CONFLICT` on
River's unique index; `InsertManyFast` uses `COPY` and returns only a count, which closes most of the
gap. On insert, `river_job` maintains five indexes, including GIN indexes on `args` and `metadata`;
kiln's `jobs` table maintains two (the primary key and the partial fetch index), and kiln returns only
the id and state of each job.

**Concurrent single inserts.** kiln is about 15% ahead in throughput, with a lower p99. Both are bound
by commit latency; kiln's `Enqueue` is one round trip, River's `Insert` is three. This is the noisiest
scenario on this machine: in rounds 2 and 3 something else on the host slowed commits down and both
libraries fell to 2,000–3,000 jobs/s. A separate 9-round run of this scenario alone put kiln ahead in 8
of the 9 rounds (medians 4,883 and 4,074 jobs/s; the ninth was within 4%).

**Drains with River's defaults.** River completes 997 jobs/s in both drains regardless of the handler.
That is its fetch cooldown: at most one fetch every 100ms, each for at most `MaxWorkers` (100) jobs,
so the ceiling is 1,000 jobs/s until either setting is raised.

**No-op drain with River at 1ms.** This is a tie: 21,235 against 20,288 jobs/s with overlapping ranges,
and River's spread is narrower (19.1k–22.1k against 16.4k–22.7k). A separately instrumented run of both
drains (outside this program: timing pgstore's `Claim` and `Finish` in a wrapper, and River's
`JobGetAvailable` metrics through a hook) showed both doing the same thing: about 50 jobs per fetch, one
fetch in flight, 2.2–3.3ms per fetch depending on the run. That time is mostly commit latency. With
`synchronous_commit=off` on the connections both roughly doubled, kiln to 39–41k and River to 36k
jobs/s. kiln commits about
twice as often per job as River in this scenario (a claim plus a finish statement for every ~56 jobs,
where River completes hundreds of jobs per statement), which makes it more exposed to commit latency
spikes on a noisy host; that is the most likely reason for its wider spread, but it was not isolated.

**Drain with a 1ms handler.** kiln is 1.45 times as fast as River at 1ms. Both still fetch about 50
jobs per call at about 2.3ms, but River starts the next fetch only after a job from the previous full
batch has finished, so each cycle costs the fetch plus the handler time. kiln issues its next claim as
soon as half of its workers are free, so the claim overlaps the handlers that are still running. In the
instrumented run River needed 1.42s for 395 fetches of 20,000 jobs, kiln 0.8–1.0s for 400 claims.

**Latency.** With defaults, River's p50 is 55ms and its p99 106ms: an insert sends `NOTIFY` only if its
client has not notified that queue within the last 100ms, and the producer fetches at most every 100ms,
so a job that arrives every 10ms waits half the cooldown on average. At 1ms River is at 4.7ms p50 and
10.7ms p99 against kiln's 3.3ms and 6.6ms. Part of the p50 difference is the insert itself, which is
one round trip in kiln and three in River (scenario 2). The p99 is not a stable comparison on this
host: a repeat of scenario 4 alone (5 rounds) kept the p50 order (kiln 3.43ms, River at 1ms 4.81ms)
but reversed the p99 order (kiln 17.97ms with a 6.82–35.91 range, River 11.17ms with 10.98–19.26). In an
instrumented kiln run the tail came from single inserts and finish statements taking 20–50ms, roughly
once a second. A plain `INSERT` loop on the same database with neither library involved also stalled
(p99 6.7ms, one 282ms stall in 6 seconds), so at least part of this is the host, but a cause inside
kiln was not ruled out.

**Summary.** Against River's default configuration kiln is ahead in every scenario, and most of that
comes from River's 100ms fetch cooldown rather than from how either library uses PostgreSQL. With the
cooldown at 1ms, kiln is still ahead on bulk inserts, single inserts, median wake-up latency and
handlers that take real time; the two are even on a pure no-op drain, where both are limited by one
claim transaction in flight and the cost of a commit; and neither has a reliable edge on p99 wake-up
latency on this machine.

This benchmark does not cover several worker processes competing for the same queue, unique jobs,
retries, large payloads or many queues.

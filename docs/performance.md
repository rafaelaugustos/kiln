# Performance

Apple M4 Max. PostgreSQL and MySQL run in Docker on the same machine; SQLite writes to the local SSD
with `synchronous=NORMAL`. Medians; see the benchmark code for ranges.

| | PostgreSQL | MySQL 8.4 | SQLite (modernc) |
|---|---|---|---|
| Bulk insert, 10k jobs per call | ~250k jobs/s | ~77k jobs/s | ~300k jobs/s |
| Claim + finish, 50 per fetch | ~45k jobs/s | ~15k jobs/s | ~80k jobs/s |
| One server, 100 workers, no-op handler | ~21k jobs/s | ~6k jobs/s | ~70k jobs/s |
| Enqueue to handler start | p50 3.3ms, p99 6.6ms | ~5–20ms with `redisbus`, `PollInterval` without | p50 0.16ms in the same process |

The MySQL numbers are dominated by commit latency on this setup (binlog with `sync_binlog=1`,
4-6ms per commit); a server with a faster disk will do noticeably better.

Compared with [River](https://github.com/riverqueue/river) v0.47 on the same PostgreSQL, alternating
rounds between the two libraries ([bench/](bench/README.md)):

| Scenario | kiln | River (defaults) | River (1ms fetch cooldown) |
|---|---|---|---|
| Bulk insert | 251k jobs/s | 134k (`InsertMany`), 222k (`InsertManyFast`) | |
| 8 goroutines inserting one job at a time | 5.1k jobs/s | 4.4k jobs/s | |
| Drain 50k no-op jobs, 100 workers | 21.2k jobs/s | 1.0k jobs/s | 20.3k jobs/s |
| Drain 20k jobs with a 1ms handler | 19.9k jobs/s | 1.0k jobs/s | 13.7k jobs/s |
| Enqueue to handler start, p50 | 3.3ms | 55ms | 4.7ms |

River's defaults fetch at most once every 100ms, which is what caps it around 1k jobs/s; with that
cooldown lowered the no-op drain is a tie, and the p99 latency of the two was too noisy on this
machine to call either way.

## Under a production-like load

The numbers above are single-purpose benchmarks. To see kiln in something closer to production, a small
shop backend runs the same code on all four stores: PostgreSQL and MySQL with an API process and two
workers, SQLite and memstore in one process. Placing an order writes it and, in the same transaction,
enqueues a flow: a payment job under `Limit{Max: 4, Rate: 20, Per: time.Second, Burst: 5}`, then a
confirmation e-mail and an invoice that wait for it. The payment gateway is a fake that rejects more than 20
requests per second or 4 at a time, fails 15% of charges and leaves PIX payments pending. Servers run with
a 2s heartbeat, `DeadAfter` 15s and `LeaderTTL` 6s; MySQL polls every 200ms and has no bus.

| | v0.3.1 | v0.4.0 |
|---|---|---|
| Busiest second at the gateway, 150 orders from 15 clients | up to 40 requests (429s) | 24–25 on every store, no 429 |
| Same, with the limits row held for 1s mid-run (PostgreSQL) | 40 | 25 |
| Payment job, enqueue to start, MySQL p50 | 684ms | 72ms |
| Continuation, parent done to child start, MySQL p50 | 61ms | 46ms |
| Same two on PostgreSQL | 6ms / 5ms | 7ms / 6ms |
| Export resumed after its worker got `kill -9`, MySQL | 41s | 14s |
| Same with SQLite, where the one process restarts | 38s | 24s |

Rescued jobs now restart within 100ms of being rescued; the rest of those times is noticing that the
worker is gone (`DeadAfter`, plus `LeaderTTL` when the dead server was the leader). Across the runs, 25
buyers racing for 5 units of stock always ended with 5 orders, 20 rejections and no job left behind for a
rolled-back order, and every charge reached the gateway exactly once.

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

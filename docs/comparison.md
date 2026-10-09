# How kiln compares

kiln takes its model from Hangfire. In Go, the libraries people usually weigh it against are
[River](https://github.com/riverqueue/river) and [Asynq](https://github.com/hibiken/asynq), both mature and
with larger communities. This table only compares features, as each project documented them in
October 2026:

| | kiln | Hangfire | River | Asynq |
|---|---|---|---|---|
| Storage | PostgreSQL, MySQL, SQL Server, SQLite | SQL Server; Redis with Pro; others from the community | PostgreSQL; SQLite (experimental) | Redis |
| License | MIT | LGPL-3.0, paid Pro and Ace | MPL-2.0, paid Pro | MIT |
| Retries with backoff | yes | yes | yes | yes |
| Job waits for other jobs | yes | yes, several parents with Pro | Pro (workflows) | no |
| Batches with a continuation | yes | Pro | Pro, as a workflow | no |
| Concurrency limit per key | yes | a mutex; Ace for more | Pro | in the handler, with `x/rate` |
| Rate limit per key | yes | Ace | no | no |
| Unique jobs | yes | third party | yes | yes |
| Cron with time zones | yes, with a misfire policy | yes | yes; durable schedule with Pro | yes |
| Transactional enqueue | yes | yes | yes | no |
| Cancel a running job | yes | yes | yes | yes, best effort |
| Pause a queue | yes | third party | yes | yes |
| Dashboard | in the module | in the package | separate app (River UI) | separate app (Asynqmon) |
| OpenTelemetry | `kilnotel` | contrib package | `rivercontrib` | Prometheus metrics |

Moving a Hangfire application over? [Coming from Hangfire](hangfire.md) maps its calls and attributes
to kiln's, one by one.

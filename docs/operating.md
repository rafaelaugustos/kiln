# Operating kiln

| `ServerConfig` | Default | What it controls |
|---|---|---|
| `PollInterval` | 1s | How often an idle pool looks for work when no notification arrives |
| `HeartbeatInterval` | 5s | How often a server reports that it is alive |
| `DeadAfter` | 60s | Silence after which a server's running jobs are retried elsewhere |
| `LeaderTTL` | 15s | Lease of the leader that runs recurring jobs, rescue, sweep and pruning |
| `ShutdownTimeout` | 30s | How long `Run` waits for running jobs after its context is cancelled |
| `KillGrace` | 5s | Extra wait after cancelling jobs that ignored the shutdown |
| `Timeout` | 30m | Default per-job timeout |

**Recovering from a dead worker.** A server that exits cleanly finishes its running jobs, or hands them
back, before `Run` returns. A server that dies is noticed after `DeadAfter`, and within one more
`HeartbeatInterval` its jobs are rescued: they go straight back to their queues, and the rescued attempt
counts toward `MaxAttempts`. With the defaults, a job whose worker was killed starts again after roughly
60 to 70 seconds. A job rescued twice in a row waits for its backoff before the next attempt, so a job that
takes down the process running it is not handed from worker to worker without a pause. If the dead server
was also the leader, a new leader takes over after `LeaderTTL` and waits `DeadAfter + HeartbeatInterval`
before rescuing anything, which adds about 20 seconds. Lower `DeadAfter` to notice dead workers sooner (it
must stay above `3*HeartbeatInterval + KillGrace + 5s`), and keep long jobs resumable with `SetParam`
checkpoints.

**Connections.** `pgstore` opens its own pool (`MaxConns`, 8 by default) plus one connection for
`LISTEN`, on top of your application's pool: budget up to 9 connections per process that opens a store.
`mysqlstore` and `sqlitestore` use the `*sql.DB` you pass, keeping one of its connections for their own
writes, so leave `SetMaxOpenConns` at 2 or more.

**Polling.** An idle server runs one indexed query per pool every `PollInterval`, and, when it gets no
notifications (MySQL without a `Bus`), one more every 100ms to release delayed jobs on time, besides its
heartbeat and the leader's periodic work. Each query is cheap, but they add up: an API process and two
idle workers on MySQL ran about 70 queries per second with the default `PollInterval` and 200 with
200ms. A `Bus` removes the 100ms check and lets `PollInterval` stay long.

**Health.** `Server.Healthy()` fails when the server has fenced itself off, its heartbeat is stale or its
results are piling up; use it for readiness and liveness probes. `Server.Stats()` has the counters.

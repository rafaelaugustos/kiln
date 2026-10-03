# soak

A soak and chaos test for kiln. It starts worker processes against a real database, enqueues jobs
without pause, kills workers and restarts the database at random, then drains and checks that no
job was lost or ran more often than it should have.

This is a separate module, outside `go.work` and CI, like `bench/`.

## Running

It expects the local test containers: `kiln-postgres` on :55432 for PostgreSQL (the default) and
`kiln-mysql` on :53306 for MySQL.

```
cd soak
GOWORK=off go run . -duration 15m
GOWORK=off go run . -backend mysql -duration 24h -rate 20
```

| flag | default | |
|---|---|---|
| `-duration` | 15m | how long to enqueue jobs and inject faults; the drain and the checks come after |
| `-workers` | 4 | worker processes, each a kiln server |
| `-rate` | 50 | enqueues per second; a flow enqueue inserts three jobs |
| `-backend` | postgres | `postgres` or `mysql` |
| `-dsn` | the test container | database to use |
| `-container` | the test container | container that `docker restart` hits; `none` turns restarts off. Without `-dsn` it defaults to `kiln-postgres` or `kiln-mysql`; with `-dsn` there are no restarts unless it is set |

Each run drops and recreates the kiln schema `kiln_soak` (PostgreSQL) or the `kiln_soak_` tables
(MySQL), and the harness's own tables `soak_enqueued` and `soak_runs`. Worker logs go to a
temporary directory printed on the first line. Ctrl-C ends the run early and still drains and
checks; a second Ctrl-C quits.

## What it does

The harness enqueues, in one transaction with a row in `soak_enqueued`, a mix of plain jobs, jobs
that fail a few times before succeeding, jobs that always fail, fan-in flows whose child checks its
parents' outputs, jobs that sleep up to 5s, and jobs under four limit keys per 50 enqueues/s: a
mutex, Max 3, Rate 3/s with Burst 3, and Max 2 with Rate 3/s and Burst 2. Every handler run is recorded in
`soak_runs` with the job id, attempt, worker and start and end times.

Faults: a SIGKILL of a random worker every 15-45s (restarted 2-8s later), a SIGTERM every
20-60s (restarted 1-4s after it exits), and a `docker restart` of the database every 3-5
minutes. Latency injection through toxiproxy is not included.

## Checks

After the drain (all jobs final, at most 10 minutes) the workers are stopped and it checks that:

- every enqueued job is succeeded, failed or deleted, and the always-failing jobs failed while all
  others succeeded;
- no job ran more times than its attempts plus its runs that were cut short (killed, or canceled
  by a shutdown);
- every succeeded job has a run that returned nil;
- a limit key never had more than Max runs at once, and no window of 1s, 10s or 1m held more
  starts than Rate*(W+0.5s)/Per + Burst, the half second covering claim latency;
- the limits' active, throttled and reserved counts are 0 and a Sweep finds nothing to repair;
- no worker exited on its own, and every SIGTERM ended in a clean exit.

It prints a summary, with the warnings and errors of the worker logs counted by message, and exits
with status 1 when a check fails. Store errors and requeued lost claims are expected around
database restarts and stalls; they only point at a problem when a check fails.

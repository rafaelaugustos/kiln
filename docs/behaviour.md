# Behaviour worth knowing

- **Failed is not final.** A job that exhausts its attempts or returns `Permanent` stays `failed` until
  someone requeues or deletes it, as in Hangfire. Continuations that wait for its success (`After`,
  `Needs`) and batches that contain it wait too. Deleting the failed job deletes those continuations;
  `AfterFinished` continuations run either way.
- **Requeue starts over.** Requeueing a `failed`, `succeeded` or `deleted` job resets its attempt count,
  so it gets all of `MaxAttempts` again and its retries back off from the first delay, as if it had just
  been enqueued. Requeueing a `scheduled` job only runs it now; it keeps the attempts it has used.
- **At least once.** A job can run more than once: a worker can crash after the handler's side effect
  and before its result is stored, and a stalled worker's jobs are retried elsewhere. Make handlers
  idempotent; `SetParam` keeps checkpoints and idempotency keys across attempts.
- **History.** A job's history records transitions that have a reason (retries, snoozes, cancellations,
  requeues, rescues). A plain success adds no entry; its timestamps are on the job.
- **Dead workers.** Jobs of a server that stops heartbeating are retried once it has been silent for
  `DeadAfter`. See [Operating kiln](operating.md) for the timings.

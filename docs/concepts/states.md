# States

```mermaid
stateDiagram-v2
    direction LR
    [*] --> enqueued
    [*] --> scheduled: runs later
    [*] --> awaiting: waits for other jobs
    [*] --> throttled: held by a limit
    awaiting --> enqueued
    scheduled --> enqueued
    throttled --> enqueued
    enqueued --> processing
    processing --> succeeded
    processing --> scheduled: retry
    processing --> failed: out of attempts
    processing --> deleted: canceled
    failed --> enqueued: requeue
```

A job starts in `awaiting` if it has unresolved dependencies, `scheduled` if it runs in the future,
`throttled` if it's waiting on a `Limit` slot, otherwise `enqueued`. As in Hangfire, `failed` is not a
final state: the job sits there until an operator requeues or deletes it.

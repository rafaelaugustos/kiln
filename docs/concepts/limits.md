# Limits and rate limits

`Limit{Key, Max}` caps how many jobs sharing a key can be `processing` at once, across the whole
cluster, independent of queue or worker pool. A job over the cap sits in `throttled` until a slot
frees up. This is kiln's equivalent of Hangfire's `DisableConcurrentExecution` and Hangfire Ace's
semaphores, without needing a separate package.

`Rate` and `Per` cap how many jobs of a key may start per period, and `Burst` how many may start back
to back (it defaults to `Rate`). When a job is admitted kiln reserves its start time, so a backlog of
10,000 jobs against a 100/s limit is released at that pace, each job written once, instead of being
retried until it fits:

```go
client.Enqueue(ctx, ChargeCard{OrderID: id}, kiln.Limit{Key: "stripe", Rate: 100, Per: time.Second})
```

`Max` and `Rate` can be combined on one key: `Max` bounds how many run at once, `Rate` how often
they start.

The rate also holds when kiln falls behind. If admission stalls for a while, because the database was
slow or a lock was held, the jobs whose start times passed in the meantime don't all start when it
resumes: `Burst` of them start and the rest get new start times at the back of the line. No window of
length `Per` sees more than `Rate + Burst` starts of a key.

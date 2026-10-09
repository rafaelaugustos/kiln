# Recurring jobs

`SetRecurring` schedules a job on a cron spec (standard 5/6-field syntax plus `L`, `W`, `#`,
`@every`, ...), evaluated in a given `TZ`, UTC when omitted: `0 3 * * *` without
`kiln.TZ("America/Sao_Paulo")` fires at midnight in São Paulo. `Misfire` controls what happens to occurrences missed
while no server was running: `MisfireOnce` (default, catch up once), `MisfireAll` (run every
missed occurrence, capped), or `MisfireSkip` (drop stale ones). Occurrences overlap by default: if a
run is still going when the next one is due, the next one is enqueued anyway. Pass `kiln.Overlap(false)`
for jobs that must not run twice at the same time, and the occurrence is skipped instead.

`SyncRecurring` declares a group of recurring jobs at once: it sets the ones it's given and removes the
ones of that group that are no longer there, so deleting a schedule from your code deletes it from the
store on the next deploy. Jobs set with `SetRecurring` belong to no group and are left alone.

```go
client.SyncRecurring(ctx, "reports",
	kiln.RecurringSpec{ID: "daily-sales", Spec: "0 7 * * *", Args: SalesReport{}, Options: []kiln.RecurringOption{kiln.TZ("America/Sao_Paulo")}},
	kiln.RecurringSpec{ID: "weekly-stock", Spec: "@weekly", Args: StockReport{}},
)
```

# Coming from Hangfire

Hangfire serializes a method call; kiln enqueues a typed value and routes it to a handler registered
for its `Kind()`. Most calls map one to one:

| Hangfire | kiln |
|---|---|
| `BackgroundJob.Enqueue(() => mailer.Send(to))` | `client.Enqueue(ctx, SendEmail{To: to})` |
| `BackgroundJob.Schedule(() => ..., TimeSpan.FromMinutes(30))` | `client.Enqueue(ctx, args, kiln.Delay(30*time.Minute))` |
| `BackgroundJob.Schedule(() => ..., runAt)` | `client.Enqueue(ctx, args, kiln.At(runAt))` |
| `BackgroundJob.ContinueJobWith(parentId, () => ...)` | `client.Enqueue(ctx, args, kiln.After{parentID})` |
| `ContinueJobWith(..., JobContinuationOptions.OnAnyFinishedState)` | `kiln.AfterFinished{parentID}` |
| `BackgroundJob.Delete(id)` / `BackgroundJob.Requeue(id)` | `client.Delete(ctx, id)` / `client.Requeue(ctx, id)` |
| `RecurringJob.AddOrUpdate("report", () => ..., Cron.Daily())` | `client.SetRecurring(ctx, "report", "@daily", Report{})` |
| `new RecurringJobOptions { TimeZone = tz }` | `kiln.TZ("America/Sao_Paulo")` |
| `RecurringJob.TriggerJob("report")` / `RemoveIfExists("report")` | `client.TriggerRecurring(ctx, "report")` / `client.RemoveRecurring(ctx, "report")` |
| `[Queue("critical")]` | `kiln.Queue("critical")`, or `InsertOptions()` on the args type |
| `[AutomaticRetry(Attempts = 5)]` | `kiln.MaxAttempts(5)` |
| `[AutomaticRetry(DelaysInSeconds = new[] { 60, 300 })]` | `kiln.Handle(mux, h, kiln.Delays(time.Minute, 5*time.Minute))` |
| `[DisableConcurrentExecution(60)]` | `kiln.Limit{Key: "reports", Max: 1}` |
| Ace semaphore / rate limiter | `kiln.Limit{Key: k, Max: n}` / `kiln.Limit{Key: k, Rate: n, Per: time.Second}` |
| Pro `BatchJob.StartNew(x => x.Enqueue(...))` | `b := &kiln.Batch{}; b.Add(args); client.StartBatch(ctx, b)` |
| Pro nested batches, `x.StartNew(...)` inside a batch | `outer.AddBatch(inner)`, or `client.StartBatch(ctx, &kiln.Batch{Parent: id})` |
| Pro `BatchJob.ContinueBatchWith(batchId, ...)` | `b.Then(args)`, or `client.Enqueue(ctx, args, kiln.AfterBatch(batchID))` |
| `CancellationToken` parameter | the handler's `ctx`, cancelled with cause `kiln.ErrCanceled` on `Delete` |
| Hangfire.Console `context.WriteLine("...")` / `WriteProgressBar()` | `j.Logf("...")` / `j.SetProgress(n)` |
| `RecurringJob.AddOrUpdate` for every job at startup, removing stale ones by hand | `client.SyncRecurring(ctx, group, specs...)` |
| `[JobDisplayName("Welcome email for {0}")]` | `kiln.Title("Welcome email for " + to)`, or a `Title()` method on the args |
| `context.SetJobParameter("cursor", c)` | `j.SetParam(ctx, "cursor", c)` / `j.Param("cursor", &c)` |
| `IClientFilter` / `IServerFilter` | `kiln.NewClient(store, mw...)` / `mux.Use(mw...)` |
| `Enqueue<IMailer>(x => x.Send(...))` resolved from DI | a method value with its dependencies: `kiln.Handle(mux, mailer.Send)` |
| `AddHangfireServer(o => { o.WorkerCount = 20; o.Queues = ... })` | `kiln.ServerConfig{Pools: []kiln.Pool{{Queues: []string{"critical", "default"}, Workers: 20}}}` |
| `UseSqlServerStorage(conn)` | `mssqlstore.New(ctx, db)` on the same SQL Server, or `pgstore`, `mysqlstore`, `sqlitestore` |
| `app.UseHangfireDashboard("/hangfire")` | `http.Handle("/kiln/", dashboard.New(client, dashboard.Options{Prefix: "/kiln", Authorize: auth}))` |

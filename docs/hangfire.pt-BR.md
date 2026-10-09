# Vindo do Hangfire

O Hangfire serializa uma chamada de método; o kiln enfileira um valor tipado e o roteia para um handler
registrado para o seu `Kind()`. A maioria das chamadas tem uma correspondência direta:

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
| `[Queue("critical")]` | `kiln.Queue("critical")`, ou `InsertOptions()` no tipo dos args |
| `[AutomaticRetry(Attempts = 5)]` | `kiln.MaxAttempts(5)` |
| `[AutomaticRetry(DelaysInSeconds = new[] { 60, 300 })]` | `kiln.Handle(mux, h, kiln.Delays(time.Minute, 5*time.Minute))` |
| `[DisableConcurrentExecution(60)]` | `kiln.Limit{Key: "reports", Max: 1}` |
| Semáforo do Ace / rate limiter | `kiln.Limit{Key: k, Max: n}` / `kiln.Limit{Key: k, Rate: n, Per: time.Second}` |
| Pro `BatchJob.StartNew(x => x.Enqueue(...))` | `b := &kiln.Batch{}; b.Add(args); client.StartBatch(ctx, b)` |
| Lotes aninhados do Pro, `x.StartNew(...)` dentro de um lote | `outer.AddBatch(inner)`, ou `client.StartBatch(ctx, &kiln.Batch{Parent: id})` |
| Pro `BatchJob.ContinueBatchWith(batchId, ...)` | `b.Then(args)`, ou `client.Enqueue(ctx, args, kiln.AfterBatch(batchID))` |
| parâmetro `CancellationToken` | o `ctx` do handler, cancelado com a causa `kiln.ErrCanceled` em `Delete` |
| Hangfire.Console `context.WriteLine("...")` / `WriteProgressBar()` | `j.Logf("...")` / `j.SetProgress(n)` |
| `RecurringJob.AddOrUpdate` para cada job na inicialização, removendo os antigos manualmente | `client.SyncRecurring(ctx, group, specs...)` |
| `[JobDisplayName("Welcome email for {0}")]` | `kiln.Title("Welcome email for " + to)`, ou um método `Title()` nos args |
| `context.SetJobParameter("cursor", c)` | `j.SetParam(ctx, "cursor", c)` / `j.Param("cursor", &c)` |
| `IClientFilter` / `IServerFilter` | `kiln.NewClient(store, mw...)` / `mux.Use(mw...)` |
| `Enqueue<IMailer>(x => x.Send(...))` resolvido via DI | um method value com suas dependências: `kiln.Handle(mux, mailer.Send)` |
| `AddHangfireServer(o => { o.WorkerCount = 20; o.Queues = ... })` | `kiln.ServerConfig{Pools: []kiln.Pool{{Queues: []string{"critical", "default"}, Workers: 20}}}` |
| `UseSqlServerStorage(conn)` | `mssqlstore.New(ctx, db)` no mesmo SQL Server, ou `pgstore`, `mysqlstore`, `sqlitestore` |
| `app.UseHangfireDashboard("/hangfire")` | `http.Handle("/kiln/", dashboard.New(client, dashboard.Options{Prefix: "/kiln", Authorize: auth}))` |

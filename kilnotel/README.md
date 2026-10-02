# kilnotel

OpenTelemetry tracing and metrics for [kiln](https://github.com/rafaelaugustos/kiln). It depends only on
the OpenTelemetry API, so it works with whatever SDK and exporters your application already configures.

## Install

```
go get github.com/rafaelaugustos/kiln/kilnotel
```

## Wiring

```go
client := kiln.NewClient(store, kilnotel.EnqueueMiddleware())
mux.Use(kilnotel.Middleware())
unregister, err := kilnotel.Observe(server, store)
```

Everything goes to the global tracer provider, meter provider and propagator unless you pass
`WithTracerProvider`, `WithMeterProvider` or `WithPropagator`. The global propagator does nothing until one
is set, and without one a job's trace stops at its enqueue span, so set it at startup:

```go
otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
```

Pass `Middleware` to `mux.Use` before any other middleware. The others then run inside the job's span, and
the outcome it records is based on the same error kiln sees.

## Traces

`EnqueueMiddleware` starts one `kiln enqueue` producer span per insert (`Enqueue`, `EnqueueMany`, a batch)
and writes its context into every job's `Meta` with the propagator (`traceparent` and `tracestate` for W3C
Trace Context). It copies `Meta` maps before writing, so maps you pass in are never changed.

`Middleware` reads that context back and runs each attempt in a `kiln process <kind>` consumer span that is
a child of the enqueue span. A request, the jobs it enqueued and the jobs those enqueue share one trace.

| Attribute | `kiln enqueue` | `kiln process <kind>` |
|---|---|---|
| `messaging.system` | `kiln` | `kiln` |
| `messaging.operation.type` | `send` | `process` |
| `messaging.destination.name` | the queue, if every job in the call uses it | the queue |
| `messaging.message.id` | the job id, for a single job | the job id |
| `messaging.batch.message_count` | the number of jobs, if more than one | |
| `kiln.job.kind` | the kind, if every job in the call has it | the kind |
| `kiln.job.attempt` | | the attempt number |

A failed insert or attempt records the error and sets the span status to Error; for a panic the exception
event carries the stack. A snooze adds a `kiln.job.snoozed` event and leaves the status unset.

## Metrics

| Name | Type | Unit | Attributes | Value |
|---|---|---|---|---|
| `kiln.jobs.processed` | counter | `{job}` | `kind`, `queue`, `outcome` | attempts run |
| `kiln.job.duration` | histogram | `s` | `kind`, `queue`, `outcome` | time in the handler |
| `kiln.job.delay` | histogram | `s` | `kind`, `queue` | time from `RunAt` to the handler starting |
| `kiln.server.running` | gauge | `{job}` | | jobs running on the server |
| `kiln.server.capacity` | gauge | `{job}` | | jobs the server can run at once |
| `kiln.completer.pending` | gauge | `{job}` | | finished jobs whose outcome is not stored yet |
| `kiln.queue.jobs` | gauge | `{job}` | `queue`, `state` | jobs `enqueued`, `processing`, `scheduled` or `throttled` |

`outcome` is `succeeded`, `failed`, `snoozed`, `canceled` or `interrupted`. An attempt that kiln will
retry counts as `failed`; `canceled` means the job was deleted while it ran; `interrupted` means the
server shut down before it finished and the job went back to the queue.

`Middleware` records the first three. `Observe` reports the gauges each time metrics are collected; the
queue gauges query the store with a 5 second timeout. They describe the whole store, so when several
servers share one, pass the store on one of them and a nil inspector on the rest. A process that only
enqueues can pass a nil server. Call the returned function to stop reporting.

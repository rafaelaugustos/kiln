# Middleware and observability

## Middleware

`Mux.Use` wraps every handler (logging, panics-to-errors, tracing); `NewClient`'s variadic
`EnqueueMiddleware` wraps every insert the same way (e.g. to stamp tenant metadata).

## Observability

`kilnotel` traces every job from the request that enqueued it to the handler that ran it, and records
job counts, durations and the delay between a job's scheduled time and its start:

```go
client := kiln.NewClient(store, kilnotel.EnqueueMiddleware())
mux.Use(kilnotel.Middleware())
unregister, err := kilnotel.Observe(server, store)
```

It depends only on the OpenTelemetry API, so it reports through whatever SDK and exporters the
application already has. `Server.Stats()` and `Server.Healthy()` cover the same ground without it.

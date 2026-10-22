# Middleware e observabilidade

## Middleware

`Mux.Use` envolve todo handler (logging, panics-to-errors, tracing); o `EnqueueMiddleware` variádico do
`NewClient` envolve toda inserção do mesmo jeito (por exemplo, para marcar metadados de tenant).

## Observabilidade

`kilnotel` rastreia cada job desde a requisição que o enfileirou até o handler que o executou, e
registra contagens de jobs, durações e o atraso entre o horário agendado de um job e o seu início:

```go
client := kiln.NewClient(store, kilnotel.EnqueueMiddleware())
mux.Use(kilnotel.Middleware())
unregister, err := kilnotel.Observe(server, store)
```

Ele depende só da API do OpenTelemetry, então reporta através de qualquer SDK e exporters que a
aplicação já tenha. `Server.Stats()` e `Server.Healthy()` cobrem o mesmo terreno sem precisar dele.

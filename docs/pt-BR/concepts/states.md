# Estados

```mermaid
stateDiagram-v2
    direction LR
    [*] --> enqueued
    [*] --> scheduled: roda depois
    [*] --> awaiting: espera por outros jobs
    [*] --> throttled: retido por um limit
    awaiting --> enqueued
    scheduled --> enqueued
    throttled --> enqueued
    enqueued --> processing
    processing --> succeeded
    processing --> scheduled: retentativa
    processing --> failed: sem tentativas
    processing --> deleted: cancelado
    failed --> enqueued: reenfileiramento
```

Um job começa em `awaiting` se tiver dependências não resolvidas, `scheduled` se roda no futuro,
`throttled` se está esperando uma vaga de `Limit`, ou `enqueued` caso contrário. Como no Hangfire,
`failed` não é um estado final: o job fica lá até um operador reenfileirá-lo ou excluí-lo.

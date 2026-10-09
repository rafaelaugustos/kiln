# Enfileirando

## Opções de enfileiramento

As opções vêm depois dos args, e um tipo de args pode fornecer seus próprios padrões com um método
`InsertOptions() []kiln.InsertOption`, então quem chama não precisa repeti-las:

```go
client.Enqueue(ctx, SendEmail{To: to},
	kiln.Queue("emails"),
	kiln.Delay(10*time.Minute),
	kiln.MaxAttempts(5),
	kiln.Tags{"welcome"},
	kiln.Title("Welcome email for "+to),
)
```

`Title` nomeia o job no dashboard, que caso contrário mostra o seu tipo; um tipo de args também pode ter
um método `Title() string`. É possível filtrar por tags nas listas de jobs do dashboard.

## Enfileiramento transacional

`EnqueueTx` e `EnqueueManyTx` recebem um `driver.Writer` vinculado à sua própria transação, então o job é
inserido atomicamente junto com os dados de negócio que o geraram:

```go
w := store.Tx(tx)
client.EnqueueTx(ctx, w, SendReceipt{OrderID: id})
tx.Commit(ctx)
w.Notify(ctx)
```

Chame `Notify` depois do commit, nunca antes. Nesse ponto o job já está commitado, então um erro de
`Notify` não muda nada sobre ele: registre o erro e siga em frente, não transforme isso em uma requisição
com falha, ou um cliente que tenta de novo vai criar o pedido duas vezes. Sem `Notify` o job roda do
mesmo jeito, no próximo poll.

O writer transacional de cada store é um `driver.TxWriter` (incluindo `Notify`, um no-op no
`memstore.Tx`), então código que roda em mais de um store pode guardar um desses em vez de verificar o
tipo. No PostgreSQL através de `database/sql` (sqlx, bun, GORM com o driver stdlib do pgx),
`store.SQLTx(tx)` recebe um `*sql.Tx`.

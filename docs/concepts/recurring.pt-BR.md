# Jobs recorrentes

`SetRecurring` agenda um job em uma especificação cron (sintaxe padrão de 5/6 campos, mais `L`, `W`, `#`,
`@every`, ...), avaliada em um `TZ` dado, UTC quando omitido: `0 3 * * *` sem
`kiln.TZ("America/Sao_Paulo")` dispara à meia-noite em São Paulo. `Misfire` controla o que acontece com
ocorrências perdidas enquanto nenhum servidor estava rodando: `MisfireOnce` (padrão, recupera uma vez),
`MisfireAll` (roda toda ocorrência perdida, com um teto), ou `MisfireSkip` (descarta as antigas). Por
padrão, ocorrências se sobrepõem: se uma execução ainda está rolando quando a próxima vence, a próxima é
enfileirada de qualquer jeito. Passe `kiln.Overlap(false)` para jobs que não podem rodar duas vezes ao
mesmo tempo, e a ocorrência é pulada em vez disso.

`SyncRecurring` declara um grupo de jobs recorrentes de uma vez: ele define os que recebe e remove os
desse grupo que não estão mais lá, então excluir um agendamento do seu código exclui ele do store no
próximo deploy. Jobs definidos com `SetRecurring` não pertencem a nenhum grupo e ficam intocados.

```go
client.SyncRecurring(ctx, "reports",
	kiln.RecurringSpec{ID: "daily-sales", Spec: "0 7 * * *", Args: SalesReport{}, Options: []kiln.RecurringOption{kiln.TZ("America/Sao_Paulo")}},
	kiln.RecurringSpec{ID: "weekly-stock", Spec: "@weekly", Args: StockReport{}},
)
```

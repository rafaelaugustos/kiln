# Limites e rate limits

`Limit{Key, Max}` limita quantos jobs que compartilham uma chave podem estar `processing` ao mesmo
tempo, em todo o cluster, independente de fila ou pool de workers. Um job acima do teto fica em
`throttled` até uma vaga abrir. Isso é o equivalente do kiln ao `DisableConcurrentExecution` do Hangfire
e aos semáforos do Hangfire Ace, sem precisar de um pacote separado.

`Rate` e `Per` limitam quantos jobs de uma chave podem começar por período, e `Burst` quantos podem
começar em sequência (o padrão é `Rate`). Quando um job é admitido o kiln reserva o horário de início
dele, então um backlog de 10.000 jobs contra um limite de 100/s é liberado nesse ritmo, cada job escrito
uma única vez, em vez de ficar tentando de novo até caber:

```go
client.Enqueue(ctx, ChargeCard{OrderID: id}, kiln.Limit{Key: "stripe", Rate: 100, Per: time.Second})
```

`Max` e `Rate` podem ser combinados em uma mesma chave: `Max` limita quantos rodam ao mesmo tempo, `Rate`
com que frequência eles começam.

A taxa se mantém mesmo quando o kiln fica para trás. Se a admissão travar por um tempo, porque o banco de
dados estava lento ou um lock estava sendo segurado, os jobs cujo horário de início já passou nesse
meio-tempo não começam todos de uma vez quando ela volta: `Burst` deles começam e o resto recebe novos
horários de início, no fim da fila. Nenhuma janela de duração `Per` vê mais que `Rate + Burst` inícios de
uma chave.

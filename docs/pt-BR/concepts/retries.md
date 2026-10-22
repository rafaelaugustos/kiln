# Retentativas e cancelamento

## Retentativas

Todo tipo recebe um `MaxAttempts` (padrão 10) e um `Backoff` que calcula o atraso antes da próxima
tentativa a partir do número da tentativa e do erro. `kiln.Exponential`, `kiln.Constant` e `kiln.Delays`
cobrem os casos comuns; um handler também pode retornar `kiln.Permanent(err)` para falhar sem
retentativa, ou `kiln.Snooze(d)` para se reagendar sem contar como uma falha.
Um job com falha que você reenfileira ganha todas as suas tentativas de volta. Faça as retentativas
durarem mais que a pior interrupção do que um job chama, e alerte sobre o que falha mesmo assim: veja
[Retentativas e alertas](../operating.md).

## Cancelamento

`client.Delete` em um job que está `processing` cancela o `context.Context` desse job no servidor que
estiver executando ele, com `kiln.ErrCanceled` como a causa (`context.Cause(ctx)`). Um handler que para
e retorna um erro é registrado como `deleted`.

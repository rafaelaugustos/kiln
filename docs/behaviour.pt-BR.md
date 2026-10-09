# Comportamentos que vale conhecer

- **Falhou não é final.** Um job que esgota as tentativas ou retorna `Permanent` fica em `failed` até
  alguém reenfileirá-lo ou excluí-lo, como no Hangfire. Continuações que esperam o seu sucesso (`After`,
  `Needs`) e lotes que o contêm também esperam. Excluir o job com falha exclui essas continuações;
  continuações `AfterFinished` rodam de qualquer forma.
- **Reenfileirar começa do zero.** Reenfileirar um job `failed`, `succeeded` ou `deleted` reseta sua
  contagem de tentativas, então ele ganha todo o `MaxAttempts` de novo e as retentativas recomeçam o
  backoff a partir do primeiro delay, como se tivesse acabado de ser enfileirado. Reenfileirar um job
  `scheduled` apenas faz ele rodar agora; as tentativas já usadas são mantidas.
- **Ao menos uma vez.** Um job pode rodar mais de uma vez: um worker pode cair depois do efeito colateral
  do handler e antes de o resultado ser salvo, e os jobs de um worker travado recebem uma retentativa em
  outro lugar. Deixe os handlers idempotentes; `SetParam` mantém checkpoints e chaves de idempotência
  entre as tentativas.
- **Histórico.** O histórico de um job registra transições que têm um motivo (retentativas, adiamentos,
  cancelamentos, reenfileiramentos, resgates). Um sucesso simples não adiciona nenhuma entrada; os
  timestamps dele ficam no próprio job.
- **Workers mortos.** Os jobs de um servidor que para de enviar heartbeat recebem uma retentativa depois
  que ele fica em silêncio por `DeadAfter`. Veja [Operando o kiln](operating.md) para os tempos
  envolvidos.

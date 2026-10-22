# Desempenho

Apple M4 Max. PostgreSQL e MySQL rodam em Docker na mesma máquina; o SQLite escreve no SSD local com
`synchronous=NORMAL`. Medianas; veja o código do benchmark para as faixas de valores.

| | PostgreSQL | MySQL 8.4 | SQLite (modernc) |
|---|---|---|---|
| Insert em massa, 10k jobs por chamada | ~250k jobs/s | ~77k jobs/s | ~300k jobs/s |
| Captura + finalização, 50 por busca | ~45k jobs/s | ~15k jobs/s | ~80k jobs/s |
| Um servidor, 100 workers, handler no-op | ~21k jobs/s | ~6k jobs/s | ~70k jobs/s |
| Do enfileiramento até o handler começar | p50 3.3ms, p99 6.6ms | ~5–20ms com `redisbus`, sem ele depende do `PollInterval` | p50 0.16ms no mesmo processo |

Os números do MySQL são dominados pela latência de commit nesse setup (binlog com `sync_binlog=1`,
4-6ms por commit); um servidor com um disco mais rápido tem um resultado bem melhor.

Comparado com o [River](https://github.com/riverqueue/river) v0.47 no mesmo PostgreSQL, alternando
rodadas entre as duas bibliotecas ([bench/](https://github.com/rafaelaugustos/kiln/blob/main/bench/README.md)):

| Cenário | kiln | River (padrão) | River (pausa de busca de 1ms) |
|---|---|---|---|
| Insert em massa | 251k jobs/s | 134k (`InsertMany`), 222k (`InsertManyFast`) | |
| 8 goroutines inserindo um job por vez | 5.1k jobs/s | 4.4k jobs/s | |
| Esvaziar 50k jobs no-op, 100 workers | 21.2k jobs/s | 1.0k jobs/s | 20.3k jobs/s |
| Esvaziar 20k jobs com um handler de 1ms | 19.9k jobs/s | 1.0k jobs/s | 13.7k jobs/s |
| Do enfileiramento até o handler começar, p50 | 3.3ms | 55ms | 4.7ms |

Por padrão, o River busca no máximo uma vez a cada 100ms, e é isso que limita ele a cerca de 1k jobs/s;
com essa pausa reduzida, o esvaziamento no-op empata, e a latência p99 dos dois ficou ruidosa demais
nessa máquina para decidir a favor de um ou de outro.

## Sob uma carga parecida com produção

Os números acima são benchmarks de propósito único. Para ver o kiln em algo mais parecido com produção,
um pequeno backend de loja roda o mesmo código nos quatro stores: PostgreSQL e MySQL com um processo de
API e dois workers, SQLite e memstore em um único processo. Fazer um pedido grava o pedido e, na mesma
transação, enfileira um fluxo: um job de pagamento sob `Limit{Max: 4, Rate: 20, Per: time.Second, Burst:
5}`, depois um e-mail de confirmação e uma nota fiscal que esperam por ele. O gateway de pagamento é um
fake que rejeita mais de 20 requisições por segundo ou 4 de uma vez, falha 15% das cobranças e deixa
pagamentos PIX pendentes. Os servidores rodam com heartbeat de 2s, `DeadAfter` de 15s e `LeaderTTL` de
6s; o MySQL faz poll a cada 200ms e não tem bus.

| | v0.3.1 | v0.4.0 |
|---|---|---|
| Segundo mais movimentado no gateway, 150 pedidos de 15 clientes | até 40 requisições (429s) | 24–25 em todos os stores, sem 429 |
| O mesmo, com a linha de limits travada por 1s no meio da execução (PostgreSQL) | 40 | 25 |
| Job de pagamento, do enfileiramento até o início, MySQL p50 | 684ms | 72ms |
| Continuação, do pai concluído até o início do filho, MySQL p50 | 61ms | 46ms |
| Os mesmos dois no PostgreSQL | 6ms / 5ms | 7ms / 6ms |
| Export retomado depois que o worker levou um `kill -9`, MySQL | 41s | 14s |
| O mesmo com SQLite, onde o único processo reinicia | 38s | 24s |

Jobs resgatados agora reiniciam dentro de 100ms depois do resgate; o resto desse tempo é perceber que o
worker sumiu (`DeadAfter`, mais `LeaderTTL` quando o servidor morto era o líder). Ao longo das execuções,
25 compradores disputando 5 unidades em estoque sempre terminaram com 5 pedidos, 20 rejeições e nenhum
job esquecido para trás de um pedido com rollback, e toda cobrança chegou ao gateway exatamente uma vez.

## Sob caos

`soak/` roda processos worker contra PostgreSQL ou MySQL pelo tempo que você pedir, enquanto mata eles
com `SIGKILL`, para eles com `SIGTERM` e reinicia o banco de dados, e depois verifica que todo job chegou
a um estado final, que nenhum rodou mais vezes do que suas tentativas mais as execuções interrompidas,
que limits e rates se mantiveram, e que o store não precisa de reparo. Os jobs misturam jobs simples,
retentativas, jobs que sempre falham, fluxos fan-in que leem as saídas dos pais, jobs lentos, e jobs sob
um mutex, um `Max`, um `Rate` e os dois.

Uma execução de 15 minutos no PostgreSQL, 4 workers, 50 enfileiramentos por segundo:

| | |
|---|---|
| Jobs | 57.211 |
| Caos | 31 `SIGKILL`s, 22 `SIGTERM`s, 3 restarts do banco de dados de cerca de 1.5s |
| Jobs perdidos | 0 |
| Jobs executados mais vezes do que o permitido | 0 |
| Pico de concorrência por chave limitada | igual ou abaixo do seu `Max` |
| Store depois da execução | contadores de limit em 0, nada para o Sweep reparar |

Uma execução de 24 horas no PostgreSQL 17, 4 workers, 20 enfileiramentos por segundo, em um droplet da
DigitalOcean com 2 vCPU / 2 GB, com o kiln v0.8.0:

| | |
|---|---|
| Jobs | 2.233.828 |
| Caos | 2.886 `SIGKILL`s, 2.139 `SIGTERM`s, 360 restarts do banco de dados |
| Jobs perdidos | 0 |
| Jobs executados mais vezes do que o permitido | 0 |
| Jobs órfãos resgatados depois de um `SIGKILL` | 2.307 |
| Pico de concorrência por chave limitada | igual ou abaixo do seu `Max`, ao longo de cerca de 85.000 execuções cada |
| Inícios por janela nas chaves com `Rate` | no máximo 6 de 8 permitidos em 1s, 32 de 34 em 10s |
| Store depois da execução | contadores de limit em 0, nada para o Sweep reparar, esvaziado em 7s |
| Memória do harness e dos seus 4 workers | 44 a 58 MB ao longo das 24 horas |

Os workers registraram erros do store em volta de cada restart do banco de dados (capturas, heartbeats,
leases), como esperado; nenhum deles custou um job.

```
cd soak && go run . -duration 24h -rate 20
```

`soak/vps` roda a mesma coisa em um servidor que só tem Docker: um compose file com seu próprio
PostgreSQL, que guarda o resumo, os logs dos workers, CPU e memória a cada 5 minutos, e um dump do banco
de dados em `soak/vps/evidence`.

Veja [soak/README.md](https://github.com/rafaelaugustos/kiln/blob/main/soak/README.md) para as flags.
`-container none` pula os restarts do banco de dados quando outros testes compartilham o mesmo banco.

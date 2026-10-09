# Operando o kiln

| `ServerConfig` | Padrão | O que controla |
|---|---|---|
| `PollInterval` | 1s | Com que frequência um pool ocioso procura trabalho quando nenhuma notificação chega |
| `HeartbeatInterval` | 5s | Com que frequência um servidor avisa que está vivo |
| `DeadAfter` | 60s | Silêncio após o qual os jobs em execução de um servidor recebem uma retentativa em outro lugar |
| `LeaderTTL` | 15s | Lease do líder que executa os jobs recorrentes, resgate, varredura e limpeza |
| `ShutdownTimeout` | 30s | Por quanto tempo `Run` espera pelos jobs em execução depois que o seu contexto é cancelado |
| `KillGrace` | 5s | Espera extra após cancelar jobs que ignoraram o shutdown |
| `Timeout` | 30m | Timeout padrão por job |

**Recuperação de um worker morto.** Um servidor que termina de forma limpa finaliza os jobs em execução,
ou os devolve, antes de `Run` retornar. Um servidor que morre é percebido depois de `DeadAfter`, e dentro
de mais um `HeartbeatInterval` seus jobs são resgatados: eles voltam direto para as próprias filas, e a
tentativa resgatada conta para o `MaxAttempts`. Com os valores padrão, um job cujo worker foi matado
começa de novo depois de uns 60 a 70 segundos. Um job resgatado duas vezes seguidas espera pelo seu
backoff antes da próxima tentativa, então um job que derruba o processo que o executa não passa de
worker para worker sem uma pausa. Se o servidor morto também era o líder, um novo líder assume depois de
`LeaderTTL` e espera `DeadAfter + HeartbeatInterval` antes de resgatar qualquer coisa, o que soma mais ou
menos 20 segundos. Diminua `DeadAfter` para perceber workers mortos mais rápido (ele precisa ficar acima
de `3*HeartbeatInterval + KillGrace + 5s`), e mantenha jobs longos retomáveis com checkpoints de
`SetParam`.

**Deploys.** No SIGTERM um servidor para de pegar jobs novos e espera até `ShutdownTimeout` pelos seus
jobs em execução. Um job que ainda está rodando depois disso é cancelado com `ErrShutdown` e volta para
a sua fila sem gastar uma tentativa, então outro servidor o executa de novo desde o início (ou a partir
dos seus checkpoints de `SetParam`). Configure `ShutdownTimeout` acima do seu job mais longo, e dê ao seu
orquestrador um grace period maior que `ShutdownTimeout + KillGrace` (o `terminationGracePeriodSeconds`
do Kubernetes, o `stop_grace_period` do Docker), ou o processo é matado antes, e seus jobs esperam
`DeadAfter` para serem resgatados.

**Uma fila por serviço.** Um servidor só pega os tipos para os quais tem handlers, então dois serviços
podem compartilhar um banco de dados e até uma fila sem rodar os jobs um do outro. Só que compartilhar a
fila tem um custo: a consulta de captura passa pelos jobs prontos do outro serviço até chegar aos seus
próprios tipos, cerca de 34ms a cada 100.000 deles no PostgreSQL. Dê a cada serviço suas próprias filas,
e essa travessia desaparece.

**Conexões.** `pgstore` abre seu próprio pool (`MaxConns`, 8 por padrão) mais uma conexão para `LISTEN`,
além do pool da sua aplicação: reserve até 9 conexões por processo que abre um store. `mysqlstore` e
`sqlitestore` usam o `*sql.DB` que você passa, reservando uma das conexões dele para suas próprias
escritas, então deixe `SetMaxOpenConns` em 2 ou mais.

**Polling.** Um servidor ocioso roda uma consulta indexada por pool a cada `PollInterval`, e, quando não
recebe notificações (MySQL sem um `Bus`), mais uma a cada 100ms para liberar jobs atrasados na hora
certa, além do seu heartbeat e do trabalho periódico do líder. Cada consulta é barata, mas elas se
acumulam: um processo de API e dois workers ociosos no MySQL rodaram cerca de 70 consultas por segundo
com o `PollInterval` padrão, e 200 com 200ms. Um `Bus` remove a verificação de 100ms e deixa o
`PollInterval` ficar longo.

**Rate limits (limite de taxa).** O `Rate` e o `Burst` de uma chave limitam os horários em que o store
admite os jobs dela, ou seja, os torna capturáveis: nenhuma janela de duração `Per` admite mais que
`Rate + Burst` deles, mesmo quando a admissão atrasa e depois recupera o atraso. A admissão lê o relógio
do banco de dados quando roda, depois de qualquer espera por um lock, então uma admissão lenta não
acumula os jobs, e inicia as admissões seguintes considerando a latência da captura. O que a admissão não
consegue ver é uma pausa entre uma admissão e o seu commit, como um flush de disco lento ou um container
de banco de dados pausado: os jobs admitidos pouco antes da pausa ficam capturáveis quando ela termina,
junto com os admitidos logo depois dela, e podem começar juntos, um pouco acima da taxa configurada.
Verificar a taxa de novo no momento da captura adicionaria uma escrita a cada captura de um job com rate
limit, então o kiln não faz isso; deixe alguma margem no destino, acima da taxa configurada.

**Retentativas e alertas.** Um job que esgota as tentativas fica em `failed` até alguém reenfileirá-lo
ou excluí-lo, e os jobs que esperam por ele ficam em `awaiting` até então. Dê a cada tipo uma janela de
retentativa maior que a pior interrupção que você espera do que ele chama: a janela é a soma das esperas
entre as tentativas, e quando elas somam quinze segundos, uma API de pagamento que fica fora do ar por
quatro minutos falha todo job que rodou nesse meio-tempo. Registrado com `Exponential(2*time.Second,
2*time.Minute)` e com `MaxAttempts(12)`, um tipo espera de seis a doze minutos no total, já que cada
espera é sorteada entre a metade e o valor cheio. Depois, alerte sobre os jobs que falham mesmo assim:
`kilnotel.Observe` reporta eles como o gauge `kiln.jobs.failed` (`kiln_jobs_failed` no Prometheus), e a
aba de jobs com falha no dashboard reenfileira todos de uma vez, o que também libera o que estava
esperando por eles.

**Saúde.** `Server.Healthy()` falha quando o servidor se isolou por fencing, o heartbeat dele está
desatualizado, ou os resultados estão se acumulando; use isso para readiness e liveness probes.
`Server.Stats()` tem os contadores.

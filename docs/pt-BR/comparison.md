# Comparação

O kiln segue o modelo do Hangfire. Em Go, as bibliotecas com que ele costuma ser comparado são
[River](https://github.com/riverqueue/river) e [Asynq](https://github.com/hibiken/asynq), ambas maduras e
com comunidades maiores. Esta tabela compara apenas funcionalidades, da forma como cada projeto as
documentava em outubro de 2026:

| | kiln | Hangfire | River | Asynq |
|---|---|---|---|---|
| Armazenamento | PostgreSQL, MySQL, SQL Server, SQLite | SQL Server; Redis com o Pro; outros pela comunidade | PostgreSQL; SQLite (experimental) | Redis |
| Licença | MIT | LGPL-3.0, Pro e Ace pagos | MPL-2.0, Pro pago | MIT |
| Retentativas com backoff | sim | sim | sim | sim |
| Job espera por outros jobs | sim | sim, vários pais com o Pro | Pro (workflows) | não |
| Lotes com continuação | sim | Pro | Pro, como workflow | não |
| Limite de concorrência por chave | sim | um mutex; Ace para mais | Pro | no handler, com `x/rate` |
| Rate limit por chave | sim | Ace | não | não |
| Jobs únicos | sim | terceiros | sim | sim |
| Cron com fusos horários | sim, com uma política de misfire | sim | sim; agendamento durável com o Pro | sim |
| Enfileiramento transacional | sim | sim | sim | não |
| Cancelar um job em execução | sim | sim | sim | sim, best effort |
| Pausar uma fila | sim | terceiros | sim | sim |
| Dashboard | no módulo | no pacote | app separado (River UI) | app separado (Asynqmon) |
| OpenTelemetry | `kilnotel` | pacote contrib | `rivercontrib` | métricas Prometheus |

Vai migrar uma aplicação Hangfire? [Vindo do Hangfire](hangfire.md) mapeia suas chamadas e atributos para
os equivalentes do kiln, um a um.

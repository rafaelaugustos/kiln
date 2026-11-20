# Bancos de dados

O kiln guarda jobs em um banco de dados através de um store. Escolha o que corresponde ao banco de dados
que você já usa; trocar depois significa mudar a linha que constrói o store, não o código que enfileira
ou processa os jobs. Os jobs também podem morar no Redis, sozinhos ou atrás das transações do seu banco de
dados: [Onde os jobs moram](placement.md) compara os três.

| Backend | Pacote | Notas |
|---|---|---|
| PostgreSQL (CI na 17) | `pgstore` | avisos via `LISTEN`/`NOTIFY`, pgx v5, opção de schema |
| SQL Server 2019+ e Azure SQL (CI na 2022) | `mssqlstore` | qualquer `*sql.DB` do `go-mssqldb`, opção de prefixo de tabela; precisa de `READ_COMMITTED_SNAPSHOT`; os servidores são avisados através de um `Bus`, ou fazem poll sem um |
| MySQL 8.0.19+ (CI na 8.4) | `mysqlstore` | qualquer `*sql.DB`, opção de prefixo de tabela; os servidores são avisados através de um `Bus`, ou fazem poll sem um |
| SQLite 3.38+ | `sqlitestore` | qualquer driver `database/sql`, WAL; servidores no mesmo processo são avisados direto, outros processos através de um `Bus` |
| Redis 7+ e Valkey (CI no Redis 7) | `redisstore` | go-redis v9, opção de prefixo de chave; avisos via Pub/Sub; a durabilidade segue a persistência do servidor |
| em memória | `memstore` | testes e ferramentas de um único processo; tudo se perde quando o processo termina |

Todo backend passa pela mesma suíte de conformidade, `drivertest.Run`, então a semântica (retentativas,
continuações, lotes, unicidade, limites, fencing) não muda quando o banco de dados muda. Escrever um
novo backend significa implementar `driver.Store` e fazer essa suíte passar; veja
[Escrevendo um store](custom-store.md).

| | PostgreSQL | MySQL | SQL Server | SQLite | Redis | em memória |
|---|---|---|---|---|---|---|
| Avisa os servidores | `LISTEN`/`NOTIFY` | através de um `Bus` | através de um `Bus` | no próprio processo; outros através de um `Bus` | Pub/Sub | no próprio processo |
| Enfileirar na sua transação | `Tx(pgx.Tx)`, `SQLTx(*sql.Tx)` | `Tx(*sql.Tx)` | `Tx(*sql.Tx)` | `Tx(*sql.Tx)` | através de um outbox no PostgreSQL ou MySQL | `Begin()` |
| Onde ficam seus dados | um schema (`Schema`) | um prefixo de tabela (`Prefix`) | um prefixo de tabela (`Prefix`) | um prefixo de tabela (`Prefix`) | um prefixo de chave (`Prefix`) | memória |
| Console do job, página de Limites | sim | sim | sim | sim | sim | sim |

O PostgreSQL avisa os servidores com `LISTEN`/`NOTIFY`. MySQL e SQL Server não têm nada equivalente, e o
SQLite só consegue avisar servidores no próprio processo, então os três aceitam um `driver.Bus`.
`redisbus` é um sobre Redis Pub/Sub:

```go
rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", ContextTimeoutEnabled: true})
store, err := mysqlstore.New(ctx, db, mysqlstore.Bus(redisbus.New(rdb)))
```

Com um bus, jobs novos, continuações liberadas e cancelamentos chegam a todos os servidores em
milissegundos. Sem um, os servidores encontram trabalho novo no próximo poll (`PollInterval`) e
verificam a cada 100ms se há jobs vencidos. Eventos são só dicas: um evento perdido atrasa um job até o
próximo poll, mas nunca perde o job.

Com `mysqlstore`, comece suas próprias transações com `sql.LevelReadCommitted` antes de passá-las para
`store.Tx`. Em `REPEATABLE READ` o InnoDB usa gap locks que podem fazer enfileiramentos concorrentes
esperarem pelo seu commit.

```go
db, _ := sql.Open("mysql", "user:pass@tcp(localhost:3306)/app")
store, err := mysqlstore.New(ctx, db)
```

`mssqlstore` recebe um `*sql.DB` aberto com `github.com/microsoft/go-mssqldb`, e o banco de dados precisa
de row versioning para READ COMMITTED — o Azure SQL Database já vem com isso ativado:

```sql
ALTER DATABASE app SET READ_COMMITTED_SNAPSHOT ON
```

```go
db, _ := sql.Open("sqlserver", "sqlserver://user:pass@localhost:1433?database=app")
store, err := mssqlstore.New(ctx, db)
```

Capturas, finalizações e admissões bloqueiam linhas com `READPAST`, então os servidores pulam o trabalho
um do outro em vez de esperar, e transações escolhidas como vítimas de deadlock recebem uma nova
tentativa. Nomes de fila, tipos e chaves de limite são comparados de forma case-sensitive, seja qual for
o collation do banco. Funciona em nível de compatibilidade 130 ou superior, e é testado no SQL Server
2022.

`sqlitestore` recebe um `*sql.DB` de qualquer driver SQLite para `database/sql`; seus testes e o CI usam
`modernc.org/sqlite`, que não precisa de cgo. O SQLite permite apenas um writer por vez, então o store
mantém uma conexão reservada do pool só para suas escritas e inicia toda transação de escrita com
`BEGIN IMMEDIATE`; leituras rodam em paralelo, em modo WAL.

```go
db, _ := sql.Open("sqlite", "file:app.db?_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)&_txlock=immediate")
store, err := sqlitestore.New(ctx, db)
```

- `busy_timeout` precisa estar na DSN: ele é configurado por conexão, e `New` rejeita um pool sem ele.
- `New` muda o arquivo para WAL. Bancos de dados em memória não conseguem usar WAL; use `memstore` para
  esses casos.
- Use `_txlock=immediate` nas transações que você passa para `store.Tx`, e mantenha elas curtas: enquanto
  uma está aberta, todo outro writer do arquivo espera, incluindo os servidores do kiln.
- Deixe `SetMaxOpenConns` em 2 ou mais.

O `redisstore` recebe qualquer client go-redis e precisa do Redis 7.0 ou superior, com
`maxmemory-policy noeviction`:

```go
rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", ContextTimeoutEnabled: true})
store, err := redisstore.New(ctx, rdb)
```

Ele carrega seu código no servidor como uma biblioteca de funções e roda cada operação como uma única
chamada, então não há locks de linha para esperar. O que o Redis muda em durabilidade, memória e
transações, e como enfileirar dentro de uma transação do banco de dados enquanto os jobs rodam no Redis,
está em [Onde os jobs moram](placement.md).

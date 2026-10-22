# Primeiros passos

O kiln precisa do Go 1.27 ou mais recente.

## Com SQLite, nada para instalar

```
go get github.com/rafaelaugustos/kiln github.com/rafaelaugustos/kiln/sqlitestore modernc.org/sqlite
```

```go
package main

import (
	"context"
	"database/sql"
	"log"
	"os"
	"os/signal"

	"github.com/rafaelaugustos/kiln"
	"github.com/rafaelaugustos/kiln/sqlitestore"
	_ "modernc.org/sqlite"
)

type SendEmail struct {
	To string
}

func (SendEmail) Kind() string { return "send_email" }

func sendEmail(ctx context.Context, j *kiln.Job[SendEmail]) error {
	log.Printf("sending email to %s (attempt %d)", j.Args.To, j.Attempt)
	return nil
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	db, err := sql.Open("sqlite", "file:jobs.db?_pragma=busy_timeout(5000)")
	if err != nil {
		log.Fatal(err)
	}
	store, err := sqlitestore.New(ctx, db)
	if err != nil {
		log.Fatal(err)
	}
	client := kiln.NewClient(store)

	mux := kiln.NewMux()
	kiln.Handle(mux, sendEmail)
	server, err := kiln.NewServer(client, mux, kiln.ServerConfig{})
	if err != nil {
		log.Fatal(err)
	}

	if _, err := client.Enqueue(ctx, SendEmail{To: "ada@example.com"}); err != nil {
		log.Fatal(err)
	}
	if err := server.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
```

`go run .` cria `jobs.db`, enfileira o job e o executa na hora. Um tipo de job é qualquer struct com um
`Kind()`; o `kiln.Handle` registra a função que o executa, e `Job[T]` entrega a ela os args decodificados,
junto com `Attempt`, `Meta`, `Tags` e o resto. Ctrl+C para o servidor; o que ainda não terminou fica no
arquivo para a próxima execução.

## Com PostgreSQL

```
docker run -d --name kiln-postgres -p 5432:5432 -e POSTGRES_PASSWORD=kiln postgres:17
go get github.com/rafaelaugustos/kiln/pgstore
```

Troque o store e mantenha o resto do programa, importando `github.com/jackc/pgx/v5/pgxpool` e
`github.com/rafaelaugustos/kiln/pgstore` no lugar dos pacotes do SQLite:

```go
pool, err := pgxpool.New(ctx, "postgres://postgres:kiln@localhost:5432/postgres")
if err != nil {
	log.Fatal(err)
}
store, err := pgstore.New(ctx, pool)
if err != nil {
	log.Fatal(err)
}
defer store.Close()
```

`pgstore.New` copia as configurações do pool para um pool próprio, com 8 conexões por padrão
(`pgstore.MaxConns` muda isso), então o kiln e a aplicação nunca esperam pelas conexões um do outro, e a
aplicação pode continuar usando ou fechar o seu próprio pool.

`New` cria suas tabelas em um schema `kiln` na primeira vez. MySQL e SQL Server funcionam do mesmo jeito
através de `mysqlstore` e `mssqlstore`; [Bancos de dados](backends.md) cobre o que cada banco de dados
precisa.

Programas completos e executáveis para cada tópico abaixo estão em
[examples/](https://github.com/rafaelaugustos/kiln/blob/main/examples/): [basic](https://github.com/rafaelaugustos/kiln/blob/main/examples/basic),
[workflow](https://github.com/rafaelaugustos/kiln/blob/main/examples/workflow), [recurring](https://github.com/rafaelaugustos/kiln/blob/main/examples/recurring), [throttling](https://github.com/rafaelaugustos/kiln/blob/main/examples/throttling),
[transactional](https://github.com/rafaelaugustos/kiln/blob/main/examples/transactional) e [dashboard](https://github.com/rafaelaugustos/kiln/blob/main/examples/dashboard).

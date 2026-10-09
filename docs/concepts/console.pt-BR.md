# Console do job

Um handler pode escrever linhas de log e uma barra de progresso que aparecem na página do job no
dashboard enquanto ele roda, como o Hangfire.Console:

```go
func importRows(ctx context.Context, j *kiln.Job[Import]) error {
	for i, row := range j.Args.Rows {
		j.Logf("importing %s", row.ID)
		j.SetProgress(100 * i / len(j.Args.Rows))
	}
	return nil
}
```

Nenhuma das duas chamadas recebe um context ou retorna um erro. O kiln guarda elas em buffer e escreve
em segundo plano, e mais uma vez depois que o handler retorna, então o console fica completo quando o
job termina. Uma tentativa guarda até 1000 linhas, cada linha com até 4 KiB; as linhas ficam com o job,
agrupadas por tentativa, até ele ser removido.

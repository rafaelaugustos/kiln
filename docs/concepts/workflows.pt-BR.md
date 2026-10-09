# Workflows e lotes

## Continuações e fluxos

Um job pode depender de outros por id (`After`, para um conjunto fixo de ids já no store) ou por índice
(`Needs`, para jobs sendo enviados juntos via `EnqueueMany`, resolvidos contra os ids que o kiln atribui
na mesma chamada). `AfterFinished` roda independente de o pai ter tido sucesso ou não. Uma dependência em
um job que nunca pode terminar tem efeito cascata: o dependente é inserido direto em `deleted`.

```go
var flow kiln.Flow
fetch := flow.Add(FetchData{URL: src})
flow.Add(ProcessData{}, kiln.Needs{fetch})
client.EnqueueMany(ctx, flow...)
```

Uma continuação pode ler o que seus pais produziram: `j.ParentOutputs(ctx)` retorna a saída de cada pai
que teve sucesso, indexada por id.

## Lotes

Um `Batch` agrupa jobs e, opcionalmente, uma continuação (`Then`) que roda assim que todo job do lote
terminar:

```go
b := &kiln.Batch{Description: "nightly-import"}
b.Add(ImportFile{Name: "a.csv"})
b.Add(ImportFile{Name: "b.csv"})
b.Then(SendSummary{})
client.StartBatch(ctx, b)
```

Lotes podem ser aninhados. Um lote externo termina assim que os próprios jobs terminam e todo lote
aninhado nele também termina, e as continuações de um lote aninhado contam como parte do lote externo:

```go
month := &kiln.Batch{Description: "monthly close"}
for _, acct := range accounts {
	b := &kiln.Batch{Description: acct.Name}
	b.Add(CloseAccount{ID: acct.ID})
	b.Then(EmailStatement{ID: acct.ID})
	month.AddBatch(b)
}
month.Then(SendReport{})
client.StartBatch(ctx, month)
```

Um job em execução também pode abrir um lote dentro do próprio lote com `kiln.Batch{Parent: j.BatchID}`.

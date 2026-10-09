# Escrevendo um store

Um store guarda os jobs do kiln em um banco de dados. Cada store neste repositório é um pacote que
implementa `driver.Store`, e todos eles passam pela mesma suíte de conformidade, então retentativas,
dependências, lotes, chaves únicas e limites se comportam da mesma forma em cada um. Um novo store ganha
esse comportamento do mesmo jeito: implemente a interface, depois faça o `drivertest.Run` passar.

A [documentação do pacote driver](https://pkg.go.dev/github.com/rafaelaugustos/kiln/driver) é o
contrato. A visão geral dela explica estados, capturas, limites e taxas; a documentação de cada método
detalha o que esse método precisa fazer.

## O que implementar

`driver.Store` embute cinco interfaces:

| Interface | O que cobre |
|---|---|
| `Writer` | inserir jobs, abrir e selar lotes |
| `Worker` | capturar jobs, aplicar seus resultados, heartbeats, `SetMeta` |
| `Coordinator` | o relógio do store, o lease do líder, promoção, resgates, reparos, limpeza e jobs recorrentes |
| `Admin` | excluir, reenfileirar, pausar, e definições de jobs recorrentes |
| `Inspector` | o lado de leitura que o dashboard usa |

Mais cinco são opcionais. O kiln verifica se elas existem e funciona sem elas:

| Interface | Sem ela |
|---|---|
| `Notifier`, ou `Bus` para avisar outros processos | os servidores encontram jobs novos no próximo poll |
| `Transactor` | `StartBatch` insere passo a passo e limpa depois de uma falha |
| `Console` | o console do job fica vazio |
| `LimitReader` | o dashboard fica sem a página de Limites |

Para deixar aplicações enfileirarem nas próprias transações, dê ao store também um método que envolve
uma transação em um `driver.Writer`, como o `Tx` faz nos stores daqui. Um writer que consegue admitir e
notificar depois do commit implementa `driver.TxWriter`.

## As regras que a maioria dos stores erra primeiro

- **Um relógio só.** Horários de execução, idades e expirações vêm do relógio do store
  (`Coordinator.Now`), nunca dos servidores.
- **Escritas com fencing.** Uma captura incrementa o `Claim` do job, e toda escrita de um job em
  execução (`Finish`, `SetMeta`, `WriteConsole`) só é aplicada sob essa mesma captura. Um servidor que
  perdeu um job não consegue sobrescrevê-lo.
- **Finish é seguro de reenviar.** Os resultados são independentes, cada um se aplica em um único passo
  atômico com todos os seus efeitos colaterais, e um resultado enviado duas vezes volta como `Stale` na
  segunda vez.
- **Pule o que outros estão retendo.** Todo servidor chama `Promote`, então um store com locks de linha
  deve pular as linhas que outra transação está retendo em vez de esperar por elas, e deixá-las para a
  próxima chamada.

## Rode a suíte de conformidade

```go
func TestConformance(t *testing.T) {
	drivertest.Run(t, func(t *testing.T) driver.Store {
		s := mystore.New(t) // a fresh, empty store for each test
		t.Cleanup(s.Close)
		return s
	})
}
```

Cada teste abre um store próprio, que precisa estar vazio, e os testes de um grupo rodam em paralelo. Os
testes dormem e fazem poll no relógio real, então o relógio do store precisa andar junto com ele. Os
grupos das interfaces opcionais pulam um store que não as implementa.

## Onde procurar

- `memstore` é a implementação de referência: tudo sob um único mutex, a leitura mais direta do
  contrato.
- `sqlitestore` é o store de banco de dados mais simples, já que o SQLite roda um writer por vez.
- `pgstore`, `mysqlstore` e `mssqlstore` mostram locks de linha que pulam o que outros estão retendo
  (`SKIP LOCKED`, `READPAST`), e notificações.

Dentro da v1, os métodos de `driver.Store` não mudam; novas capacidades chegam como interfaces
opcionais. Veja [Compatibilidade](compatibility.md).

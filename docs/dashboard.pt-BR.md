# Dashboard

```go
mux.Handle("/kiln/", dashboard.New(client, dashboard.Options{
	Prefix:    "/kiln",
	Authorize: func(r *http.Request) dashboard.Access { return dashboard.ReadOnly },
}))
```

`Authorize` roda em toda requisição e retorna `Denied`, `ReadOnly` ou `ReadWrite`, então o controle de
acesso se encaixa em qualquer auth que a sua aplicação já tenha. `dashboard.AllowAll` é para
desenvolvimento local: concede acesso de leitura/escrita só para requisições endereçadas a `localhost` ou
a um IP loopback, e nega todo o resto. O dashboard mostra contagens em tempo real e um gráfico de
concluídos/com falha, jobs por estado com filtros por fila, tipo, lote e tag, e ações em massa, detalhe
do job com títulos, argumentos/metadados/saída que podem ser redigidos e o console em tempo real,
retentativas, agendamentos recorrentes e seus grupos, filas, servidores, lotes, e limites com o que cada
chave está rodando e retendo, e espelha tudo isso em uma API JSON em `<prefix>/api/...` para scripting.
É renderizado no servidor, sem assets externos e com um CSP estrito.

O dashboard fala inglês e português brasileiro. Um seletor no cabeçalho lembra a escolha de cada pessoa;
`Options.Language` define o padrão, e sem ele, o idioma do navegador decide. Um outro idioma é só um
arquivo JSON em `dashboard/locales`.

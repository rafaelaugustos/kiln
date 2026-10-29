# Compatibilidade

O kiln segue o versionamento semântico. Estas garantias valem para toda a v1.
[Atualizando](upgrading.md) explica os passos de cada release.

**A API.** Identificadores exportados do pacote `kiln` e de `dashboard`, `kilntest`, `kilnotel`,
`redisbus`, `cron`, `memstore`, `drivertest`, e os `New`, `Migrate` e opções dos stores, não são
removidos nem alterados. Novos podem aparecer, e os tipos struct podem ganhar campos, então construa
structs usando os nomes dos campos.

**A interface do store.** Os métodos de `driver.Store` e das interfaces que ele embute não mudam. Uma
capacidade adicionada depois chega como uma nova interface opcional, do mesmo jeito que `driver.Console`
e `driver.LimitReader` chegaram, que o kiln verifica e funciona sem elas. Um store escrito para a v1.0
continua compilando, e os casos de conformidade de uma nova interface pulam os stores que não a
implementam.

**O schema.** Toda mudança de schema dentro da v1 é aditiva (tabelas novas, colunas anuláveis, índices)
e é aplicada por `New`, ou por `Migrate` quando a aplicação roda com `NoMigrate`. Servidores em duas
versões menores consecutivas rodam lado a lado durante um rolling deploy; o módulo `compat` verifica
isso em toda release, no PostgreSQL, MySQL e SQLite. Releases de patch nunca mudam o schema.

**O dashboard.** Campos da sua API JSON são adicionados, nunca renomeados ou removidos. As páginas HTML
podem mudar em qualquer release.

**Comportamento.** O que a documentação diz faz parte da API. Fazer o código bater com a documentação é
uma correção, não uma mudança que quebra compatibilidade.

**Módulos.** Todo módulo no repositório é lançado junto, sob a mesma versão. Atualize-os juntos.

**Go.** O kiln precisa de uma versão do Go que o time do Go ainda dê suporte, Go 1.27 ou mais recente
por enquanto: o código dele constrói structs com promoted fields, algo que o Go 1.27 introduziu. O
mínimo só muda em releases menores, e nunca passa da mais antiga das duas últimas releases do Go.

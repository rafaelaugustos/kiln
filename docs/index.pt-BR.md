---
hide:
  - toc
---

# kiln { style="display: none" }

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/rafaelaugustos/kiln/main/.github/logo-dark.png">
    <img src="https://raw.githubusercontent.com/rafaelaugustos/kiln/main/.github/logo.png" alt="kiln" width="200">
  </picture>
</p>

<p align="center">
  Jobs em segundo plano para Go, guardados em PostgreSQL, MySQL, SQL Server ou SQLite.
</p>

<p align="center">
  <a href="https://pkg.go.dev/github.com/rafaelaugustos/kiln"><img src="https://pkg.go.dev/badge/github.com/rafaelaugustos/kiln.svg" alt="Referência do Go"></a>
  <a href="https://github.com/rafaelaugustos/kiln/actions/workflows/ci.yml"><img src="https://github.com/rafaelaugustos/kiln/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://codecov.io/gh/rafaelaugustos/kiln"><img src="https://codecov.io/gh/rafaelaugustos/kiln/graph/badge.svg" alt="Cobertura"></a>
  <a href="https://github.com/rafaelaugustos/kiln/releases"><img src="https://img.shields.io/github/v/release/rafaelaugustos/kiln" alt="Última versão"></a>
  <a href="https://github.com/rafaelaugustos/kiln/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue" alt="Licença MIT"></a>
</p>

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/rafaelaugustos/kiln/main/.github/dashboard-dark.png">
    <img src="https://raw.githubusercontent.com/rafaelaugustos/kiln/main/.github/dashboard-light.png" alt="O dashboard do kiln: jobs por estado, vazão na última hora, filas e servidores">
  </picture>
</p>

O kiln executa jobs em segundo plano para programas Go, da mesma forma que o Hangfire faz para .NET. Jobs
são linhas no banco de dados que você já tem: eles sobrevivem a restarts e crashes, podem ser enfileirados
na mesma transação que os dados que os geraram, e você pode acompanhá-los e reenfileirá-los a partir de um
dashboard que já vem com a biblioteca.

- **Retentativas** com backoff exponencial ou personalizado, adiamentos, timeouts e falhas permanentes
- **Workflows**: jobs que esperam um ou vários outros jobs e leem suas saídas, e lotes, aninhados se
  necessário, com um job que roda quando o lote inteiro termina
- **Limites** por chave, em todos os servidores: quantos jobs rodam ao mesmo tempo e quantos começam por
  segundo
- **Jobs recorrentes** a partir de especificações cron, com fusos horários, uma política para execuções
  perdidas, e `SyncRecurring` para mantê-los sincronizados com o seu código
- **Console do job**: linhas de log e uma barra de progresso direto de dentro de um handler, em tempo real
  no dashboard
- **Jobs únicos**, enquanto um job está ativo ou por uma janela de tempo, com replace e debounce
- **Enfileiramento transacional**: o job só existe se a sua transação for commitada
- **Cancelamento** de um job em execução, a partir de qualquer processo
- **Dashboard** em inglês ou português brasileiro, e uma API JSON, montado no seu próprio servidor HTTP
- **OpenTelemetry** com traces desde a requisição que enfileirou um job até o handler que o executou
- **PostgreSQL, MySQL, SQL Server e SQLite**, mais um store em memória para testes, todos seguindo a mesma
  suíte de conformidade

Tudo isso está neste repositório, sob a licença MIT. Não existe uma edição paga.

## Por onde começar

- [Primeiros passos](getting-started.md): um programa funcionando em cinco minutos, com SQLite ou
  PostgreSQL
- [Como funciona](how-it-works.md): o que é um job, e como os servidores dividem o trabalho
- [Conceitos](concepts/states.md): estados, enfileiramento, retentativas, workflows, limites, jobs
  recorrentes e mais
- [Bancos de dados](backends.md): o que cada banco de dados precisa, e como escrever seu próprio store
- [Vindo do Hangfire](hangfire.md): chamadas do Hangfire e seus equivalentes no kiln

## Status

O kiln roda em produção na [Sodexo](https://www.sodexo.com), na [Zeep Labs](https://github.com/zeeplabs),
na [Starbem](https://github.com/Starbem) e no [Orbit](https://getcortexlabs.com/products/orbit/), da Cortex Labs.
Se o seu time também usa, abra um pull request para se adicionar aqui.

A API é final desde a v1.0.0-rc.1. A v1.0.0 sai depois que a release candidate rodar em produção por
algumas semanas sem precisar de mudança na API, e a partir daí a página de [Compatibilidade](compatibility.md)
descreve o que se mantém estável. Toda mudança é listada no [changelog](changelog.md), e cada release é
testado contra o anterior rodando no mesmo banco de dados, então um rolling upgrade de uma versão para a
próxima continua funcionando.

---
layout: home
title: Jobs em segundo plano para Go
description: Jobs em segundo plano para Go, guardados em PostgreSQL, MySQL, SQL Server ou SQLite. Retentativas, workflows, limites, jobs recorrentes e um dashboard, no banco que você já tem.

hero:
  name: kiln
  text: Jobs em segundo plano para Go.
  tagline: Guardados em PostgreSQL, MySQL, SQL Server ou SQLite. Retentativas, workflows, limites, jobs recorrentes e um dashboard, no banco que você já tem.
  image:
    src: /icon.svg
    alt: kiln
  actions:
    - theme: brand
      text: Primeiros passos
      link: /pt-BR/getting-started
    - theme: alt
      text: Como funciona
      link: /pt-BR/how-it-works
    - theme: alt
      text: GitHub
      link: https://github.com/rafaelaugustos/kiln

features:
  - title: Jobs no seu banco
    details: Jobs são linhas ao lado dos seus dados. Sobrevivem a restarts e crashes, e podem ser enfileirados na mesma transação que os dados que os geraram.
    link: /pt-BR/concepts/enqueueing
    linkText: Enfileirando
  - title: Retentativas e workflows
    details: Backoff exponencial ou personalizado, adiamentos e timeouts. Jobs que esperam outros jobs e leem suas saídas, e lotes com um job que roda quando terminam.
    link: /pt-BR/concepts/workflows
    linkText: Workflows e lotes
  - title: Dashboard incluso
    details: Acompanhe, reenfileire e cancele jobs por um dashboard em inglês ou português, montado no seu próprio servidor HTTP, com uma API JSON.
    link: /pt-BR/dashboard
    linkText: O dashboard
---

<div class="home-body">

<p class="home-shot">
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

</div>

export interface Mode {
  id: 'db' | 'redis' | 'both'
  tab: string
  title: string
  body: string
  wal: number
  commits: number
  rows: number
  facts: string[]
}

export interface Copy {
  base: string
  hero: {
    eyebrow: string
    aside: string
    line1: string
    line2: string
    sub: string
    primary: string
    secondary: string
    copied: string
  }
  line: {
    lanes: [string, string, string]
    button: string
    done: string
    retry: string
    hint: string
  }
  stores: string[]
  features: {
    eyebrow: string
    line1: string
    line2: string
    sub: string
    cards: { id: string; title: string; body: string; link: string; tag: string }[]
  }
  placement: {
    eyebrow: string
    line1: string
    line2: string
    sub: string
    modes: Mode[]
    labels: { wal: string; commits: string; rows: string; per: string; app: string; db: string; redis: string }
    more: string
    note: string
  }
  code: { eyebrow: string; line1: string; line2: string; sub: string; link: string }
  dash: { eyebrow: string; line1: string; line2: string; sub: string; chips: string[]; link: string; alt: string }
  proof: {
    eyebrow: string
    line1: string
    line2: string
    stats: { value: number; label: string }[]
    sub: string
    link: string
    users: string
  }
  finale: { line1: string; line2: string; primary: string; secondary: string; note: string }
}

const en: Copy = {
  base: '',
  hero: {
    eyebrow: 'Open source. No paid edition.',
    aside: 'Background jobs for Go',
    line1: 'Jobs go in raw.',
    line2: 'They come out done.',
    sub: 'Retries, workflows, limits, recurring jobs and a dashboard, the way Hangfire does it for .NET. In the database you already have, in Redis, or in both.',
    primary: 'Fire it up',
    secondary: 'See how it works',
    copied: 'Copied. Go build something.',
  },
  line: {
    lanes: ['Enqueued', 'Processing', 'Succeeded'],
    button: 'Throw a job in',
    done: 'done',
    retry: 'retry',
    hint: 'A real queue would be less cute. Not by much.',
  },
  stores: ['PostgreSQL', 'MySQL', 'SQL Server', 'SQLite', 'Redis', 'In memory'],
  features: {
    eyebrow: '01 / The little big details',
    line1: 'Everything a queue needs.',
    line2: 'Nothing to pay for.',
    sub: 'The parts other libraries keep for the paid tier are in the box, under MIT.',
    cards: [
      { id: 'retries', title: 'Retries', body: 'It failed. It waits a bit, and it goes again. Backoff, snoozes and timeouts included.', link: '/concepts/retries', tag: 'attempt' },
      { id: 'flows', title: 'Workflows', body: 'This one runs when those two are done, and reads what they returned.', link: '/concepts/workflows', tag: 'after' },
      { id: 'limits', title: 'Limits', body: 'Two at a time, twenty a second, across every server. The API you call will thank you.', link: '/concepts/limits', tag: 'max 2' },
      { id: 'recurring', title: 'Recurring', body: 'Cron specs with time zones, and a plan for the runs you missed while deploying.', link: '/concepts/recurring', tag: '*/5 * * * *' },
      { id: 'unique', title: 'Unique jobs', body: 'The same job twice is one job. The second click on the button is forgiven.', link: '/concepts/unique', tag: 'duplicate' },
      { id: 'tx', title: 'Transactional', body: 'The job exists only if your transaction commits. Rollback, and it never happened.', link: '/concepts/enqueueing', tag: 'commit' },
    ],
  },
  placement: {
    eyebrow: '02 / Where jobs live',
    line1: 'Your database called.',
    line2: 'It wants a break.',
    sub: 'Keep the jobs next to your data, move them to Redis, or do both: written in your transaction, run in Redis. Same code, one line changes.',
    modes: [
      {
        id: 'db', tab: 'In your database', title: 'Everything in one place',
        body: 'Jobs are rows next to your data. One system to run, and nothing acknowledged is ever lost.',
        wal: 1326, commits: 2.4, rows: 4.2,
        facts: ['Enqueue in your transaction', 'As durable as your database', 'Nothing else to run'],
      },
      {
        id: 'redis', tab: 'In Redis', title: 'The database does nothing',
        body: 'Queue, retries, limits and history live in Redis. Fast, and off your database entirely.',
        wal: 0, commits: 0, rows: 0,
        facts: ['Zero writes to your database', 'As durable as Redis is set up to be', 'No enqueue in your transaction'],
      },
      {
        id: 'both', tab: 'Both', title: 'Written here, run there',
        body: 'An outbox: the job commits with your data, then moves to Redis. One insert, one delete.',
        wal: 387, commits: 1, rows: 2,
        facts: ['Enqueue in your transaction', '71% less written to the log', 'Rollback means it never ran'],
      },
    ],
    labels: { wal: 'WAL bytes', commits: 'commits', rows: 'rows written', per: 'per job, measured on PostgreSQL 17', app: 'Your code', db: 'Your database', redis: 'Redis' },
    more: 'Read the trade-offs',
    note: 'Measured, not estimated: 10,000 orders, one job each.',
  },
  code: {
    eyebrow: '03 / Five minutes',
    line1: 'A type, a handler,',
    line2: 'and you are done.',
    sub: 'Arguments are Go types, so a typo is a compile error and not a job that fails at 3 a.m.',
    link: 'Getting started',
  },
  dash: {
    eyebrow: '04 / The dashboard',
    line1: 'Watch it work.',
    line2: 'Poke it when it does not.',
    sub: 'Retry, cancel and requeue from a dashboard that comes with the library, in English or Brazilian Portuguese, mounted on your own HTTP server.',
    chips: ['Retry', 'Cancel', 'Requeue 421 failed', 'Pause queue'],
    link: 'The dashboard',
    alt: 'The kiln dashboard: jobs by state, throughput over the last hour, queues and servers',
  },
  proof: {
    eyebrow: '05 / We tried to break it',
    line1: 'Twenty-four hours of chaos.',
    line2: 'Zero jobs lost.',
    stats: [
      { value: 2233828, label: 'jobs run' },
      { value: 2886, label: 'kill -9' },
      { value: 360, label: 'database restarts' },
      { value: 0, label: 'jobs lost' },
    ],
    sub: 'On a 2 vCPU droplet, with every limit held and nothing run more often than allowed.',
    link: 'How it was measured',
    users: 'In production at',
  },
  finale: {
    line1: 'Go on.',
    line2: 'Throw something in.',
    primary: 'Get started',
    secondary: 'Star it on GitHub',
    note: 'MIT licensed. There is no paid edition.',
  },
}

const pt: Copy = {
  base: '/pt-BR',
  hero: {
    eyebrow: 'Open source. Sem edição paga.',
    aside: 'Jobs em segundo plano para Go',
    line1: 'O job entra cru.',
    line2: 'Sai pronto.',
    sub: 'Retentativas, workflows, limites, jobs recorrentes e um dashboard, do jeito que o Hangfire faz no .NET. No banco que você já tem, no Redis, ou nos dois.',
    primary: 'Acender o forno',
    secondary: 'Ver como funciona',
    copied: 'Copiado. Vai lá construir.',
  },
  line: {
    lanes: ['Na fila', 'Processando', 'Concluído'],
    button: 'Jogar um job',
    done: 'prontos',
    retry: 'de novo',
    hint: 'Uma fila de verdade é menos fofa. Mas não muito.',
  },
  stores: ['PostgreSQL', 'MySQL', 'SQL Server', 'SQLite', 'Redis', 'Em memória'],
  features: {
    eyebrow: '01 / Os pequenos grandes detalhes',
    line1: 'Tudo que uma fila precisa.',
    line2: 'Nada para pagar.',
    sub: 'O que as outras bibliotecas guardam para o plano pago já vem na caixa, sob MIT.',
    cards: [
      { id: 'retries', title: 'Retentativas', body: 'Falhou. Espera um pouco e vai de novo. Backoff, sonecas e timeouts inclusos.', link: '/concepts/retries', tag: 'tentativa' },
      { id: 'flows', title: 'Workflows', body: 'Este roda quando aqueles dois terminarem, e lê o que eles devolveram.', link: '/concepts/workflows', tag: 'depois' },
      { id: 'limits', title: 'Limites', body: 'Dois por vez, vinte por segundo, somando todos os servidores. A API que você chama agradece.', link: '/concepts/limits', tag: 'máx 2' },
      { id: 'recurring', title: 'Recorrentes', body: 'Cron com fuso horário, e um plano para as execuções que você perdeu durante o deploy.', link: '/concepts/recurring', tag: '*/5 * * * *' },
      { id: 'unique', title: 'Jobs únicos', body: 'O mesmo job duas vezes é um job só. O segundo clique no botão está perdoado.', link: '/concepts/unique', tag: 'duplicado' },
      { id: 'tx', title: 'Transacional', body: 'O job só existe se a sua transação der commit. Deu rollback, nunca aconteceu.', link: '/concepts/enqueueing', tag: 'commit' },
    ],
  },
  placement: {
    eyebrow: '02 / Onde os jobs moram',
    line1: 'Seu banco ligou.',
    line2: 'Ele quer uma folga.',
    sub: 'Deixe os jobs ao lado dos seus dados, mande para o Redis, ou faça os dois: gravado na sua transação, executado no Redis. O mesmo código, muda uma linha.',
    modes: [
      {
        id: 'db', tab: 'No seu banco', title: 'Tudo num lugar só',
        body: 'Os jobs são linhas ao lado dos seus dados. Um sistema só para cuidar, e nada confirmado se perde.',
        wal: 1326, commits: 2.4, rows: 4.2,
        facts: ['Enfileira na sua transação', 'Tão durável quanto o seu banco', 'Nada a mais para rodar'],
      },
      {
        id: 'redis', tab: 'No Redis', title: 'O banco não faz nada',
        body: 'Fila, retentativas, limites e histórico moram no Redis. Rápido, e totalmente fora do seu banco.',
        wal: 0, commits: 0, rows: 0,
        facts: ['Zero escritas no seu banco', 'Tão durável quanto o Redis estiver configurado', 'Sem enfileirar na sua transação'],
      },
      {
        id: 'both', tab: 'Os dois', title: 'Gravado aqui, executado lá',
        body: 'Um outbox: o job entra no commit junto com os seus dados e depois vai para o Redis. Um insert, um delete.',
        wal: 387, commits: 1, rows: 2,
        facts: ['Enfileira na sua transação', '71% menos escrita no log', 'Deu rollback, nunca rodou'],
      },
    ],
    labels: { wal: 'bytes de WAL', commits: 'commits', rows: 'linhas gravadas', per: 'por job, medido no PostgreSQL 17', app: 'Seu código', db: 'Seu banco', redis: 'Redis' },
    more: 'Ler os trade-offs',
    note: 'Medido, não estimado: 10.000 pedidos, um job cada.',
  },
  code: {
    eyebrow: '03 / Cinco minutos',
    line1: 'Um tipo, um handler,',
    line2: 'e está pronto.',
    sub: 'Os argumentos são tipos Go, então um erro de digitação é erro de compilação, e não um job falhando às 3 da manhã.',
    link: 'Primeiros passos',
  },
  dash: {
    eyebrow: '04 / O dashboard',
    line1: 'Veja funcionando.',
    line2: 'Cutuque quando não estiver.',
    sub: 'Reexecute, cancele e reenfileire por um dashboard que vem com a biblioteca, em inglês ou português, montado no seu próprio servidor HTTP.',
    chips: ['Reexecutar', 'Cancelar', 'Reenfileirar 421 falhos', 'Pausar fila'],
    link: 'O dashboard',
    alt: 'O dashboard do kiln: jobs por estado, vazão na última hora, filas e servidores',
  },
  proof: {
    eyebrow: '05 / A gente tentou quebrar',
    line1: 'Vinte e quatro horas de caos.',
    line2: 'Zero jobs perdidos.',
    stats: [
      { value: 2233828, label: 'jobs executados' },
      { value: 2886, label: 'kill -9' },
      { value: 360, label: 'restarts do banco' },
      { value: 0, label: 'jobs perdidos' },
    ],
    sub: 'Num droplet de 2 vCPU, com todo limite respeitado e nada executado mais vezes do que o permitido.',
    link: 'Como foi medido',
    users: 'Em produção na',
  },
  finale: {
    line1: 'Vai.',
    line2: 'Joga alguma coisa aí dentro.',
    primary: 'Começar',
    secondary: 'Dar uma estrela no GitHub',
    note: 'Licença MIT. Não existe edição paga.',
  },
}

export function copyFor(lang: string): Copy {
  return lang.toLowerCase().startsWith('pt') ? pt : en
}

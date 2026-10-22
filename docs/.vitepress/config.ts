import { execSync } from 'node:child_process'
import { existsSync, readFileSync } from 'node:fs'
import { type DefaultTheme } from 'vitepress'
import { withMermaid } from 'vitepress-plugin-mermaid'

const site = 'https://kiln.rafaelaugusto.dev'
const repo = 'https://github.com/rafaelaugustos/kiln'
const api = 'https://pkg.go.dev/github.com/rafaelaugustos/kiln'
const cortex = '<a href="https://getcortexlabs.com/">Cortex Labs</a>'

function latestVersion() {
  try {
    return execSync('git describe --tags --abbrev=0 --match "v*"', { stdio: ['ignore', 'pipe', 'ignore'] }).toString().trim()
  } catch {
    const changelog = readFileSync(new URL('../../CHANGELOG.md', import.meta.url), 'utf8')
    return changelog.match(/^## (v\S+)/m)?.[1] ?? ''
  }
}

const version = latestVersion()

function versionMenu(prefix: string, changelog: string, releases: string): DefaultTheme.NavItem {
  return {
    text: version,
    items: [
      { text: changelog, link: `${prefix}/changelog` },
      { text: releases, link: `${repo}/releases` },
    ],
  }
}

function sidebar(prefix: string, t: Record<string, string>): DefaultTheme.SidebarItem[] {
  const page = (key: string, path: string) => ({ text: t[key], link: `${prefix}/${path}` })
  return [
    {
      text: t.start,
      items: [page('getting', 'getting-started'), page('internals', 'how-it-works')],
    },
    {
      text: t.concepts,
      items: [
        page('states', 'concepts/states'),
        page('enqueueing', 'concepts/enqueueing'),
        page('retries', 'concepts/retries'),
        page('workflows', 'concepts/workflows'),
        page('unique', 'concepts/unique'),
        page('limits', 'concepts/limits'),
        page('queues', 'concepts/queues'),
        page('recurring', 'concepts/recurring'),
        page('console', 'concepts/console'),
        page('middleware', 'concepts/middleware'),
        page('testing', 'concepts/testing'),
        page('behaviour', 'behaviour'),
      ],
    },
    {
      text: t.operating,
      items: [
        page('running', 'operating'),
        page('dashboard', 'dashboard'),
        page('upgrading', 'upgrading'),
        page('compatibility', 'compatibility'),
      ],
    },
    {
      text: t.stores,
      items: [page('backends', 'backends'), page('customStore', 'custom-store')],
    },
    {
      text: t.more,
      items: [
        page('hangfire', 'hangfire'),
        page('comparison', 'comparison'),
        page('performance', 'performance'),
        page('changelog', 'changelog'),
        { text: t.api, link: api },
      ],
    },
  ]
}

const en = {
  start: 'Start',
  concepts: 'Concepts',
  operating: 'Operating',
  stores: 'Stores',
  more: 'More',
  getting: 'Getting started',
  internals: 'How it works',
  states: 'States',
  enqueueing: 'Enqueueing',
  retries: 'Retries and cancellation',
  workflows: 'Workflows and batches',
  unique: 'Unique jobs',
  limits: 'Limits and rate limits',
  queues: 'Queues and workers',
  recurring: 'Recurring jobs',
  console: 'Job console',
  middleware: 'Middleware and observability',
  testing: 'Testing',
  behaviour: 'Behaviour worth knowing',
  running: 'Running kiln',
  dashboard: 'Dashboard',
  upgrading: 'Upgrading',
  compatibility: 'Compatibility',
  backends: 'Storage backends',
  customStore: 'Writing a store',
  hangfire: 'Coming from Hangfire',
  comparison: 'How kiln compares',
  performance: 'Performance',
  changelog: 'Changelog',
  api: 'API reference',
}

const pt = {
  start: 'Começo',
  concepts: 'Conceitos',
  operating: 'Operação',
  stores: 'Stores',
  more: 'Mais',
  getting: 'Primeiros passos',
  internals: 'Como funciona',
  states: 'Estados',
  enqueueing: 'Enfileirando',
  retries: 'Retentativas e cancelamento',
  workflows: 'Workflows e lotes',
  unique: 'Jobs únicos',
  limits: 'Limites e rate limits',
  queues: 'Filas e workers',
  recurring: 'Jobs recorrentes',
  console: 'Console do job',
  middleware: 'Middleware e observabilidade',
  testing: 'Testes',
  behaviour: 'Comportamentos que vale conhecer',
  running: 'Rodando o kiln',
  dashboard: 'Dashboard',
  upgrading: 'Atualizando',
  compatibility: 'Compatibilidade',
  backends: 'Bancos de dados',
  customStore: 'Escrevendo um store',
  hangfire: 'Vindo do Hangfire',
  comparison: 'Comparação',
  performance: 'Performance',
  changelog: 'Changelog',
  api: 'Referência da API',
}

export default withMermaid({
  title: 'kiln',
  description: 'Background jobs for Go, kept in PostgreSQL, MySQL, SQL Server or SQLite.',
  cleanUrls: true,
  lastUpdated: existsSync(new URL('../../.git', import.meta.url)),
  srcExclude: ['node_modules/**'],
  sitemap: { hostname: site },
  vite: {
    optimizeDeps: { include: ['mermaid > fastdom', 'mermaid > fastdom/extensions/fastdom-promised.js'] },
  },
  markdown: {
    config(md) {
      const code = md.renderer.rules.code_inline!
      md.renderer.rules.code_inline = (...args) => code(...args).replace('<code', '<code v-pre')
    },
  },
  head: [
    ['link', { rel: 'icon', type: 'image/svg+xml', href: '/icon.svg' }],
    ['meta', { name: 'theme-color', content: '#ea580c' }],
    ['meta', { property: 'og:type', content: 'website' }],
    ['meta', { property: 'og:site_name', content: 'kiln' }],
    ['meta', { property: 'og:image', content: 'https://raw.githubusercontent.com/rafaelaugustos/kiln/main/.github/logo.png' }],
  ],
  themeConfig: {
    logo: '/icon.svg',
    search: {
      provider: 'local',
      options: {
        locales: {
          'pt-BR': {
            translations: {
              button: { buttonText: 'Buscar', buttonAriaLabel: 'Buscar' },
              modal: {
                noResultsText: 'Nada encontrado para',
                resetButtonTitle: 'Limpar a busca',
                footer: { selectText: 'abrir', navigateText: 'navegar', closeText: 'fechar' },
              },
            },
          },
        },
      },
    },
    socialLinks: [{ icon: 'github', link: repo }],
  },
  locales: {
    root: {
      label: 'English',
      lang: 'en',
      themeConfig: {
        nav: [
          { text: 'Guide', link: '/getting-started', activeMatch: '^/(?!pt-BR)(?!$)' },
          { text: 'API', link: api },
          versionMenu('', 'Changelog', 'Releases'),
        ],
        sidebar: sidebar('', en),
        outline: { level: [2, 3], label: 'On this page' },
        editLink: { pattern: `${repo}/edit/main/docs/:path`, text: 'Edit this page on GitHub' },
        footer: {
          message: 'Released under the MIT License.',
          copyright: `Maintained by Rafael Augusto. Supported by ${cortex}.`,
        },
      },
    },
    'pt-BR': {
      label: 'Português',
      lang: 'pt-BR',
      link: '/pt-BR/',
      description: 'Jobs em segundo plano para Go, guardados em PostgreSQL, MySQL, SQL Server ou SQLite.',
      themeConfig: {
        nav: [
          { text: 'Guia', link: '/pt-BR/getting-started', activeMatch: '^/pt-BR/.+' },
          { text: 'API', link: api },
          versionMenu('/pt-BR', 'Changelog', 'Versões'),
        ],
        sidebar: sidebar('/pt-BR', pt),
        outline: { level: [2, 3], label: 'Nesta página' },
        editLink: { pattern: `${repo}/edit/main/docs/:path`, text: 'Editar esta página no GitHub' },
        docFooter: { prev: 'Anterior', next: 'Próxima' },
        lastUpdated: { text: 'Atualizado em' },
        darkModeSwitchLabel: 'Aparência',
        lightModeSwitchTitle: 'Mudar para o tema claro',
        darkModeSwitchTitle: 'Mudar para o tema escuro',
        sidebarMenuLabel: 'Menu',
        returnToTopLabel: 'Voltar ao topo',
        langMenuLabel: 'Mudar o idioma',
        notFound: {
          title: 'Página não encontrada',
          quote: 'Esta página não está aqui.',
          linkText: 'Voltar ao início',
        },
        footer: {
          message: 'Publicado sob a licença MIT.',
          copyright: `Mantido por Rafael Augusto. Com apoio da ${cortex}.`,
        },
      },
    },
  },
})

import { defineUserConfig, PageHeader } from 'vuepress'
import { viteBundler } from '@vuepress/bundler-vite'
import { defaultTheme } from '@vuepress/theme-default'
import { path } from '@vuepress/utils'

import { registerComponentsPlugin } from '@vuepress/plugin-register-components'

function htmlDecode(input: string): string {
  return input.replace("&#39;", "'").replace("&amp;", "&").replace("&quot;", '"')
}

function fixPageHeader(header: PageHeader) {
  header.title = htmlDecode(header.title)
  header.children.forEach(child => fixPageHeader(child))
}

export default defineUserConfig({
  lang: 'en-GB',
  title: 'Mail Backup',
  description: "Automatically backup your JMAP mailboxes to a local git repository.",

  head: [
    ['meta', { name: "description", content: "Automatically backup your Fastmail and JMAP mailboxes to a local git repository with daily snapshots." }],
    ['link', { rel: 'icon', href: '/logo.svg', type: 'image/svg+xml' }],
    ['link', { rel: 'icon', href: '/favicon.ico', sizes: 'any' }],
  ],

  bundler: viteBundler(),

  extendsPage(page, app) {
    const fixedHeaders = page.headers || []
    fixedHeaders.forEach(header => fixPageHeader(header))

    page.headers = fixedHeaders;
  },

  theme: defaultTheme({
    logo: '/logo.svg',

    repo: "SierraSoftworks/mail-backup",
    docsDir: 'docs',
    navbar: [
      {
        text: "Getting Started",
        link: "/guide/README.md",
      },
      {
        text: "Advanced",
        children: [
          '/advanced/filters.md',
          '/advanced/storage-layout.md'
        ]
      },
      {
        text: "Reference",
        children: [
          '/reference/config.md',
          '/reference/cli.md'
        ]
      },
      {
        text: "Report an Issue",
        link: "https://github.com/SierraSoftworks/mail-backup/issues/new",
        target: "_blank"
      }
    ],

    sidebar: {
      '/': [
        {
          text: "Getting Started",
          children: [
            '/guide/README.md',
            '/guide/daemon.md',
            '/guide/restore.md',
            '/guide/telemetry.md'
          ]
        },
        {
          text: "Reference",
          children: [
            '/reference/config.md',
            '/reference/cli.md'
          ]
        },
        {
          text: "Advanced",
          children: [
            '/advanced/filters.md',
            '/advanced/storage-layout.md'
          ]
        }
      ],
    }
  }),

  plugins: [
    registerComponentsPlugin({
      componentsDir: path.resolve(__dirname, './components'),
    })
  ]
})

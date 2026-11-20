import OrbitSignature from './OrbitSignature.vue'
import DocMotion from './DocMotion.vue'
import KilnHome from './home/KilnHome.vue'
import './orbit-signature.css'
import { h } from 'vue'
import DefaultTheme from 'vitepress/theme'
import type { Theme } from 'vitepress'
import '@fontsource/instrument-serif/400-italic.css'
import './custom.css'
import './home/home.css'

export default {
  extends: DefaultTheme,
  Layout: () => h(DefaultTheme.Layout, null, {
    'home-hero-before': () => h(KilnHome),
    'doc-before': () => h(DocMotion),
    'layout-bottom': () => h(OrbitSignature),
  }),
} satisfies Theme

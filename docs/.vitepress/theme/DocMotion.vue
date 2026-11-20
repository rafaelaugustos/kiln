<script setup lang="ts">
import { nextTick, onBeforeUnmount, onMounted, watch } from 'vue'
import { useRoute } from 'vitepress'
import type { WebRuntime } from '@rafaelaugusto/jolt/web'

const route = useRoute()
let rt: WebRuntime | undefined
let create: ((doc: Document) => WebRuntime) | undefined
let live = false

async function play() {
  await nextTick()
  if (!live || !create) return
  rt?.dispose()
  rt = create(document)
  const doc = document.querySelector<HTMLElement>('.VPDoc .vp-doc > div')
  if (!doc) return
  const title = doc.querySelector<HTMLElement>('h1')
  const blocks = [...doc.children].filter((el): el is HTMLElement => el instanceof HTMLElement && el !== title).slice(0, 7)
  try {
    if (title) rt.animateText(title, { by: 'word', stagger: 45, y: 26, duration: 520 })
  } catch {
    rt.dispose()
    rt = undefined
    return
  }
  blocks.forEach((el, i) => {
    if (el.getAnimations().length > 0) return
    try {
      rt!.animate(el, { opacity: [0, 1], y: [18, 0] }, { type: 'spring', bounce: 0, duration: 560, delay: 120 + i * 55 })
    } catch {
      return
    }
  })
}

onMounted(async () => {
  live = true
  create = (await import('@rafaelaugusto/jolt/web')).createWebRuntime
  play()
})

watch(() => route.path, play)

onBeforeUnmount(() => {
  live = false
  rt?.dispose()
})
</script>

<template>
  <span class="doc-motion" hidden />
</template>

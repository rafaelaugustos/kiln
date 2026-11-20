<script setup lang="ts">
import { ref } from 'vue'
import { loop, onScreen, useMotion, wait, type Runtime } from './motion'

const props = defineProps<{ id: string; tag: string }>()
const root = ref<HTMLElement>()

const CLAY = '#8a7a6b'
const EMBER = '#ff7a2f'
const GLAZE = '#f6f1e9'

type Scene = (rt: Runtime, q: (sel: string) => HTMLElement, all: (sel: string) => HTMLElement[]) => Promise<unknown>

const scenes: Record<string, Scene> = {
  async retries(rt, q) {
    const tok = q('.tok')
    const tag = q('.tag')
    rt.set(tok, { x: 0, rotate: 0, backgroundColor: CLAY })
    tag.textContent = `${props.tag} 1`
    await rt.animate(tok, { x: 92 }, { type: 'spring', bounce: 0, duration: 520 })
    await rt.animate(tok, { x: [86, 98, 88, 96, 92], backgroundColor: EMBER }, { duration: 320 })
    rt.animate(tok, { rotate: [0, -30, 0], backgroundColor: CLAY }, { duration: 420 })
    await rt.animate(tok, { x: 0 }, 'bouncy')
    tag.textContent = `${props.tag} 2`
    await wait(420)
    await rt.animate(tok, { x: 196, backgroundColor: GLAZE }, { type: 'spring', bounce: 0.2, duration: 760 })
    await wait(700)
    await rt.animate(tok, { opacity: [1, 0] }, { duration: 200 })
    rt.set(tok, { x: 0, opacity: 1 })
  },
  async flows(rt, q, all) {
    const nodes = all('.node')
    const edges = all('.edge') as unknown as SVGGeometryElement[]
    const lines = edges.map((e) => rt.drawPath(e, { progress: 0 }))
    rt.set(nodes, { backgroundColor: CLAY, scale: 1 })
    lines.forEach((l) => l.set(0))
    await rt.animate(nodes[0], { backgroundColor: GLAZE, scale: [1, 1.25, 1] }, { duration: 420 })
    await lines[0].animate(1, { duration: 420 })
    await rt.animate(nodes[1], { backgroundColor: GLAZE, scale: [1, 1.25, 1] }, { duration: 420 })
    await lines[1].animate(1, { duration: 420 })
    await rt.animate(nodes[2], { backgroundColor: EMBER, scale: [1, 1.45, 0.94, 1] }, { duration: 520 })
    await wait(500)
    await rt.animate(nodes[2], { backgroundColor: GLAZE }, { duration: 300 })
    await wait(800)
    lines.forEach((l) => l.dispose())
  },
  async limits(rt, _q, all) {
    const toks = all('.tok')
    toks.forEach((t, i) => rt.set(t, { x: i * 22, y: 0, opacity: 1, backgroundColor: CLAY }))
    for (let i = 0; i < toks.length; i += 2) {
      const pair = toks.slice(i, i + 2)
      await Promise.all(pair.map((t, j) => rt.animate(t, { x: 150 + j * 30, backgroundColor: EMBER }, { type: 'spring', bounce: 0.2, duration: 560 })))
      await wait(620)
      await Promise.all(pair.map((t) => rt.animate(t, { x: 236, opacity: 0, backgroundColor: GLAZE }, { duration: 360 })))
    }
    await wait(300)
  },
  async recurring(rt, q) {
    const hand = q('.hand')
    const tok = q('.tok')
    for (let turn = 1; turn <= 4; turn++) {
      rt.set(tok, { x: 0, opacity: 0, scale: 0.4, backgroundColor: EMBER })
      await rt.animate(hand, { rotate: turn * 90 }, { type: 'spring', bounce: 0.45, duration: 520 })
      await rt.animate(tok, { x: 96, opacity: [0, 1, 1, 0], scale: [0.4, 1, 1, 0.8] }, { duration: 760 })
    }
    rt.set(hand, { rotate: 0 })
  },
  async unique(rt, _q, all) {
    const [held, twin] = all('.tok')
    rt.set(held, { backgroundColor: EMBER, scale: 1 })
    rt.set(twin, { x: -110, opacity: 0, rotate: 0, backgroundColor: CLAY })
    await rt.animate(twin, { x: -34, opacity: 1 }, { type: 'spring', bounce: 0, duration: 420 })
    rt.animate(held, { scale: [1, 1.18, 1] }, { duration: 300 })
    await rt.animate(twin, { x: [-34, -64, -120], rotate: [0, -40, -90], opacity: [1, 1, 0] }, { duration: 620 })
    await wait(900)
  },
  async tx(rt, q, all) {
    const tok = q('.tok')
    const [commit, rollback] = all('.pill')
    rt.set([commit, rollback], { opacity: 0.35, scale: 1 })
    rt.set(tok, { opacity: 0.25, scale: 0.9, backgroundColor: CLAY })
    await wait(350)
    rt.animate(commit, { opacity: 1, scale: [1, 1.12, 1] }, { duration: 320 })
    await rt.animate(tok, { opacity: 1, scale: [0.9, 1.25, 0.96, 1], backgroundColor: EMBER }, { duration: 460 })
    await wait(900)
    rt.set(commit, { opacity: 0.35 })
    rt.set(tok, { opacity: 0.25, scale: 0.9, backgroundColor: CLAY })
    await wait(350)
    rt.animate(rollback, { opacity: 1, scale: [1, 1.12, 1] }, { duration: 320 })
    await rt.animate(tok, { opacity: 0, scale: [0.9, 0.3], rotate: [0, 50] }, { duration: 420 })
    await wait(700)
    rt.set(tok, { rotate: 0 })
  },
}

useMotion((rt, alive) => {
  const el = root.value!
  const q = (sel: string) => el.querySelector<HTMLElement>(sel)!
  const all = (sel: string) => [...el.querySelectorAll<HTMLElement>(sel)]
  const scene = scenes[props.id]
  if (!scene || rt.reducedMotion()) return
  let visible = false
  return onScreen(el, (v) => {
    const was = visible
    visible = v
    if (v && !was) loop(() => scene(rt, q, all), () => visible && alive(), 500)
  }, 0.4)
})
</script>

<template>
  <div ref="root" class="kd" :class="`kd-${id}`" aria-hidden="true">
    <template v-if="id === 'retries'">
      <span class="tag">{{ tag }} 1</span>
      <div class="track"><i class="tok" /><i class="wall" /></div>
    </template>
    <template v-else-if="id === 'flows'">
      <svg class="wires" viewBox="0 0 240 96" fill="none">
        <path class="edge" d="M52 26 C 110 26, 110 48, 176 48" />
        <path class="edge" d="M52 70 C 110 70, 110 48, 176 48" />
      </svg>
      <i class="node n1" /><i class="node n2" /><i class="node n3" />
      <span class="tag">{{ tag }}</span>
    </template>
    <template v-else-if="id === 'limits'">
      <span class="tag">{{ tag }}</span>
      <div class="track"><i v-for="n in 6" :key="n" class="tok" /><i class="gate" /></div>
    </template>
    <template v-else-if="id === 'recurring'">
      <div class="clock"><i class="hand" /></div>
      <div class="track"><i class="tok" /></div>
      <span class="tag">{{ tag }}</span>
    </template>
    <template v-else-if="id === 'unique'">
      <div class="track"><i class="tok" /><i class="tok" /></div>
      <span class="tag">{{ tag }}</span>
    </template>
    <template v-else-if="id === 'tx'">
      <div class="pills"><b class="pill">COMMIT</b><b class="pill">ROLLBACK</b></div>
      <div class="track"><i class="tok" /></div>
    </template>
  </div>
</template>

<script setup lang="ts">
import { ref } from 'vue'
import type { Copy } from './copy'
import { loop, onScreen, useMotion, wait, type Runtime } from './motion'

const props = defineProps<{ copy: Copy['line'] }>()

const stage = ref<HTMLElement>()
const tokens = ref<HTMLElement>()
const glow = ref<HTMLElement>()
const burst = ref<HTMLElement>()
const button = ref<HTMLButtonElement>()
const counter = ref<HTMLElement>()
const flames = ref<HTMLElement[]>([])

const CLAY = '#8a7a6b'
const EMBER = '#ff7a2f'
const GLAZE = '#f6f1e9'
const HALF = 17

let throwIn: (() => void) | undefined

function enqueue() {
  throwIn?.()
}

useMotion((rt: Runtime, alive) => {
  const el = stage.value!
  let visible = false
  let waiting = 0
  let inside = 0
  let active = 0
  let done = 0
  let gate = Promise.resolve()
  const stack: HTMLElement[] = []
  const still = rt.reducedMotion()

  function lock() {
    let open!: () => void
    const before = gate
    gate = new Promise<void>((r) => (open = r))
    return before.then(() => open)
  }

  function heat(delta: number) {
    inside += delta
    rt.animate(glow.value!, { opacity: inside > 0 ? 1 : 0.35, scale: inside > 0 ? 1.25 : 1 }, 'smooth')
  }

  function badge(tok: HTMLElement, text: string) {
    const b = document.createElement('span')
    b.className = 'kl-badge'
    b.textContent = text
    tok.append(b)
    rt.animate(b, { opacity: [0, 1, 1, 0], y: [6, 0, 0, -8] }, { duration: 1500 }).then(() => {
      rt.release(b)
      b.remove()
    })
  }

  function retire(tok: HTMLElement) {
    rt.animate(tok, { opacity: 0, scale: 0.5 }, { duration: 280 }).then(() => {
      rt.release(tok)
      tok.remove()
    })
  }

  async function job(manual: boolean) {
    const w = el.clientWidth
    const h = el.clientHeight
    const tok = document.createElement('div')
    const check = document.createElement('i')
    tok.className = 'kl-tok'
    check.className = 'kl-check'
    check.textContent = '✓'
    tok.append(check)
    tokens.value!.append(tok)
    active++
    const slot = waiting++ % 4
    const q = { x: 0.13 * w - HALF, y: (0.34 + 0.11 * slot) * h - HALF }
    const k = { x: 0.5 * w - HALF, y: 0.56 * h - HALF }
    rt.set(tok, { x: q.x - 46, y: q.y, scale: 0.3, opacity: 0, backgroundColor: CLAY })
    await rt.animate(tok, { x: q.x, scale: 1, opacity: 1 }, 'bouncy')
    await wait(manual ? 120 : 250 + Math.random() * 500)
    const fails = Math.random() < (manual ? 0.2 : 0.3)
    for (let attempt = 1; alive(); attempt++) {
      const leave = await lock()
      waiting = Math.max(waiting - 1, 0)
      await rt.animate(tok, { x: k.x, y: k.y }, { type: 'spring', bounce: 0.12, duration: 620 })
      heat(1)
      rt.animate(tok, { backgroundColor: EMBER }, { duration: 420 })
      await rt.animate(tok, { scale: [1, 0.84, 1.06, 0.9, 1] }, { duration: 880 })
      heat(-1)
      if (!fails || attempt > 1) {
        leave()
        break
      }
      await rt.animate(tok, { x: [k.x - 8, k.x + 8, k.x - 5, k.x + 5, k.x] }, { duration: 300 })
      badge(tok, `${props.copy.retry} 2/3`)
      leave()
      waiting++
      rt.animate(tok, { backgroundColor: CLAY, rotate: [0, -24, 0] }, { duration: 420 })
      await rt.animate(tok, { x: q.x, y: q.y }, 'bouncy')
      await wait(650)
    }
    if (!alive()) return
    const n = ++done
    const d = { x: 0.87 * w - HALF, y: (0.72 - 0.078 * ((n - 1) % 6)) * h - HALF }
    rt.animate(check, { opacity: [0, 1], scale: [0.3, 1] }, { duration: 300 })
    rt.animate(tok, { backgroundColor: GLAZE, rotate: [0, 12, 0] }, { duration: 460 })
    await rt.animate(tok, { x: d.x, y: d.y }, { type: 'spring', bounce: 0.38, duration: 720 })
    rt.rollTo(counter.value!, n, { duration: 520 })
    if (manual) rt.confetti(burst.value!, { count: 36, colors: [EMBER, '#ffb37a', GLAZE, '#ffd9b8'] })
    active--
    stack.push(tok)
    if (stack.length > 6) retire(stack.shift()!)
  }

  throwIn = () => {
    if (active < 9) job(true)
  }

  const press = rt.press(button.value!, { scale: 0.94 }, { transition: 'snappy' })
  const off = onScreen(el, (v) => {
    const was = visible
    visible = v
    if (!v || was || still) return
    const going = () => visible && alive()
    flames.value.forEach((f, i) => {
      loop(() => rt.animate(f, { scaleY: [1, 1.35, 0.85, 1.15, 1], opacity: [0.7, 1, 0.6, 0.9, 0.7] }, { duration: 1300 + i * 260 }), going, 0)
    })
    loop(async () => {
      if (active < 4) job(false)
      await wait(1400 + Math.random() * 1100)
    }, going, 0)
  })
  if (still) {
    job(false)
  }
  return () => {
    off()
    press.dispose()
    throwIn = undefined
  }
})
</script>

<template>
  <div class="kl">
    <div ref="stage" class="kl-stage">
      <div class="kl-lanes">
        <span v-for="lane in copy.lanes" :key="lane">{{ lane }}</span>
      </div>
      <div class="kl-rail" />
      <div class="kl-kiln" aria-hidden="true">
        <div class="kl-dome" />
        <div class="kl-mouth">
          <i ref="glow" class="kl-glow" />
          <i v-for="n in 3" :key="n" ref="flames" class="kl-flame" :class="`f${n}`" />
        </div>
        <div class="kl-base" />
      </div>
      <div ref="tokens" class="kl-tokens" aria-hidden="true" />
      <div ref="burst" class="kl-burst" aria-hidden="true" />
    </div>
    <div class="kl-foot">
      <button ref="button" class="kl-button" type="button" @click="enqueue">
        <span>{{ copy.button }}</span>
        <i aria-hidden="true">+</i>
      </button>
      <p class="kl-count"><b ref="counter">0</b> {{ copy.done }}</p>
    </div>
    <p class="kl-hint">{{ copy.hint }}</p>
  </div>
</template>

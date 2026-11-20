<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import type { Copy, Mode } from './copy'
import { loop, onScreen, useMotion, wait, type Runtime } from './motion'

const props = defineProps<{ copy: Copy['placement']; lang: string }>()

const active = ref<Mode['id']>('db')
const mode = computed(() => props.copy.modes.find((m) => m.id === active.value)!)
const root = ref<HTMLElement>()
const map = ref<HTMLElement>()
const dots = ref<HTMLElement[]>([])
const values = ref<HTMLElement[]>([])
const bars = ref<HTMLElement[]>([])

const stats = computed(() => [
  { key: 'wal', label: props.copy.labels.wal, value: mode.value.wal, max: 1326, digits: 0 },
  { key: 'commits', label: props.copy.labels.commits, value: mode.value.commits, max: 2.4, digits: 1 },
  { key: 'rows', label: props.copy.labels.rows, value: mode.value.rows, max: 4.2, digits: 1 },
])

let apply: ((first: boolean) => void) | undefined

function pick(id: Mode['id']) {
  active.value = id
}

watch(active, () => apply?.(false), { flush: 'post' })

useMotion((rt: Runtime, alive) => {
  const el = root.value!
  const still = rt.reducedMotion()
  let visible = false
  let run = 0

  function show() {
    stats.value.forEach((s, i) => {
      const format = (v: number) => v.toLocaleString(props.lang, { minimumFractionDigits: s.digits, maximumFractionDigits: s.digits })
      rt.rollTo(values.value[i], s.value, { format, duration: 700, bounce: 0.1 })
      rt.animate(bars.value[i], { scaleX: Math.max(s.value / s.max, 0.004) }, { type: 'spring', bounce: 0.18, duration: 900 })
    })
  }

  async function flow(id: number) {
    const m = active.value
    const w = map.value!.clientWidth
    const at = (f: number) => f * w - 6
    const [a, b] = dots.value
    rt.set([a, b], { opacity: 0, x: at(0.14), y: 0 })
    if (m === 'db') {
      await rt.animate(a, { x: [at(0.14), at(0.5)], opacity: [0, 1, 1, 0] }, { duration: 1100 })
    } else if (m === 'redis') {
      await rt.animate(a, { x: [at(0.14), at(0.5), at(0.86)], y: [0, -34, 0], opacity: [0, 1, 0] }, { duration: 1300 })
    } else {
      await rt.animate(a, { x: [at(0.14), at(0.5)], opacity: [0, 1, 1, 0] }, { duration: 800 })
      rt.set(b, { x: at(0.5) })
      await rt.animate(b, { x: [at(0.5), at(0.86)], opacity: [0, 1, 1, 0] }, { duration: 800 })
    }
    if (id === run) await wait(350)
  }

  apply = (first) => {
    show()
    if (!first) run++
  }

  const off = onScreen(el, (v) => {
    const was = visible
    visible = v
    if (v && !was) {
      show()
      if (!still) {
        const id = ++run
        loop(() => flow(id), () => visible && alive() && id === run, 0)
      }
    }
  }, 0.3)

  watch(active, () => {
    if (!visible || still) return
    const id = ++run
    loop(() => flow(id), () => visible && alive() && id === run, 0)
  })

  return () => {
    off()
    apply = undefined
  }
})
</script>

<template>
  <div ref="root" class="kp">
    <div class="kp-tabs" role="tablist">
      <button
        v-for="m in copy.modes"
        :key="m.id"
        type="button"
        role="tab"
        class="kp-tab"
        :class="{ on: m.id === active }"
        :aria-selected="m.id === active"
        @click="pick(m.id)"
      >
        {{ m.tab }}
      </button>
    </div>

    <div class="kp-panel" :data-mode="active">
      <div class="kp-story">
        <h3>{{ mode.title }}</h3>
        <p>{{ mode.body }}</p>
        <ul>
          <li v-for="f in mode.facts" :key="f">{{ f }}</li>
        </ul>
      </div>

      <div class="kp-side">
        <div ref="map" class="kp-map" aria-hidden="true">
          <div class="kp-wire" />
          <i ref="dots" class="kp-dot" /><i ref="dots" class="kp-dot" />
          <div class="kp-box app"><b>{{ copy.labels.app }}</b></div>
          <div class="kp-box db"><b>{{ copy.labels.db }}</b><span><i /><i /><i /></span></div>
          <div class="kp-box redis"><b>{{ copy.labels.redis }}</b><span><i /><i /><i /></span></div>
        </div>

        <div class="kp-stats">
          <div v-for="s in stats" :key="s.key" class="kp-stat">
            <strong ref="values">0</strong>
            <span>{{ s.label }}</span>
            <div class="kp-bar"><i ref="bars" /></div>
          </div>
        </div>
        <p class="kp-per">{{ copy.labels.per }}</p>
      </div>
    </div>
  </div>
</template>

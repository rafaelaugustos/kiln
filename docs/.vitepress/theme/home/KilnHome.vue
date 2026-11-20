<script setup lang="ts">
import { computed, ref } from 'vue'
import { useData, withBase } from 'vitepress'
import { copyFor } from './copy'
import { loop, onScreen, provideMotion, useMotion } from './motion'
import Demo from './Demo.vue'
import Line from './Line.vue'
import Placement from './Placement.vue'
import Star from './Star.vue'

const { lang, theme } = useData()
const t = computed(() => copyFor(lang.value))
const to = (path: string) => withBase(`${t.value.base}${path}`)
const repo = 'https://github.com/rafaelaugustos/kiln'
const install = 'go get github.com/rafaelaugustos/kiln'
const shots = 'https://raw.githubusercontent.com/rafaelaugustos/kiln/main/.github'
const users = [
  { name: 'Sodexo', href: 'https://www.sodexo.com' },
  { name: 'Zeep Labs', href: 'https://github.com/zeeplabs' },
  { name: 'Starbem', href: 'https://github.com/Starbem' },
  { name: 'Orbit', href: 'https://getcortexlabs.com/products/orbit/' },
]
const version = computed(() => {
  const nav = (theme.value.nav ?? []) as { text?: string }[]
  return nav.find((n) => /^v\d/.test(n.text ?? ''))?.text ?? 'MIT'
})

const sample = `type SendEmail struct {
	To string
}

func (SendEmail) Kind() string { return "send_email" }

func sendEmail(ctx context.Context, j *kiln.Job[SendEmail]) error {
	return mailer.Send(ctx, j.Args.To)
}

kiln.Handle(mux, sendEmail)
client.Enqueue(ctx, SendEmail{To: "ada@example.com"})`

const code = computed(() =>
  sample
    .replace(/[&<>]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;' })[c]!)
    .replace(/("[^"]*")/g, '<i class="s">$1</i>')
    .replace(/\b(type|struct|func|return|string|error)\b/g, '<i class="k">$1</i>')
    .replace(/\b(kiln)\./g, '<i class="p">$1</i>.'),
)

const root = ref<HTMLElement>()
const line1 = ref<HTMLElement>()
const line2 = ref<HTMLElement>()
const mark = ref<HTMLElement>()
const belt = ref<HTMLElement>()
const chip = ref<HTMLButtonElement>()
const chipText = ref<HTMLElement>()
const spark = ref<HTMLElement>()
const frame = ref<HTMLElement>()
const scene = ref<HTMLElement>()
const floats = ref<HTMLElement[]>([])
const numbers = ref<HTMLElement[]>([])
const proof = ref<HTMLElement>()
const copied = ref(false)

let celebrate: (() => void) | undefined

async function copy() {
  try {
    await navigator.clipboard.writeText(install)
  } catch {
    return
  }
  copied.value = true
  celebrate?.()
  setTimeout(() => (copied.value = false), 2400)
}

const motion = provideMotion()

useMotion((rt) => {
  const el = root.value!
  const still = rt.reducedMotion()
  const stops: (() => void)[] = []
  el.classList.add('is-live')

  rt.animateText(line1.value!, { by: 'letter', stagger: 26, y: 56, duration: 640 })
  rt.animateText(line2.value!, { by: 'letter', stagger: 26, y: 56, duration: 640, delay: 320 })

  const seen = new WeakSet<Element>()
  const io = new IntersectionObserver(
    (entries) => {
      for (const e of entries) {
        if (!e.isIntersecting || seen.has(e.target)) continue
        seen.add(e.target)
        io.unobserve(e.target)
        const target = e.target as HTMLElement
        target.classList.add('is-in')
        const delay = Number(target.dataset.rise || 0) * 90
        try {
          rt.animate(target, { opacity: [0, 1], y: [28, 0] }, { type: 'spring', bounce: 0, duration: 760, delay })
        } catch (err) {
          console.error('kiln: reveal', err)
        }
      }
    },
    { threshold: 0.15 },
  )
  el.querySelectorAll('[data-rise]').forEach((n) => io.observe(n))
  stops.push(() => io.disconnect())

  const press = rt.press(chip.value!, { scale: 0.96 }, { transition: 'snappy' })
  stops.push(() => press.dispose())
  celebrate = () => {
    rt.confetti(spark.value!, { count: 42, colors: ['#ff7a2f', '#ffb37a', '#f6f1e9', '#ffd9b8'] })
    rt.animate(chipText.value!, { y: [8, 0], opacity: [0, 1] }, 'snappy')
  }

  if (!still) {
    rt.animate(mark.value!, { rotate: [0, 360] }, { duration: 16000, ease: 'linear', repeat: 1000 })
    const half = belt.value!.scrollWidth / 2
    rt.animate(belt.value!, { x: [0, -half] }, { duration: 30000, ease: 'linear', repeat: 1000 })
    let afloat = false
    stops.push(
      onScreen(scene.value!, (visible) => {
        const was = afloat
        afloat = visible
        if (!visible || was) return
        floats.value.forEach((f, i) => {
          loop(() => rt.animate(f, { y: [0, -9, 0], rotate: [0, i % 2 ? 2 : -2, 0] }, { duration: 3200 + i * 520 }), () => afloat, 0)
        })
      }, 0.1),
    )
    const tilt = (e: PointerEvent) => {
      const r = scene.value!.getBoundingClientRect()
      const x = (e.clientX - r.left) / r.width - 0.5
      const y = (e.clientY - r.top) / r.height - 0.5
      rt.animate(frame.value!, { rotateY: x * 9, rotateX: -y * 7 }, { type: 'spring', bounce: 0, duration: 520 })
    }
    const level = () => rt.animate(frame.value!, { rotateY: 0, rotateX: 0 }, 'smooth')
    scene.value!.addEventListener('pointermove', tilt)
    scene.value!.addEventListener('pointerleave', level)
    stops.push(() => {
      scene.value?.removeEventListener('pointermove', tilt)
      scene.value?.removeEventListener('pointerleave', level)
    })
  }

  let counted = false
  stops.push(
    onScreen(proof.value!, (visible) => {
      if (!visible || counted) return
      counted = true
      t.value.proof.stats.forEach((s, i) => {
        rt.rollTo(numbers.value[i], s.value, {
          format: (v) => Math.round(v).toLocaleString(lang.value),
          duration: 1500,
          bounce: 0,
          delay: i * 140,
        })
      })
    }, 0.4),
  )

  return () => {
    celebrate = undefined
    stops.forEach((stop) => stop())
  }
}, motion)
</script>

<template>
  <div ref="root" class="kh">
    <section class="kh-hero">
      <div class="kh-shell">
        <div class="kh-bar">
          <span class="kh-eyebrow"><i class="kh-dot" />{{ t.hero.eyebrow }}</span>
          <a class="kh-eyebrow kh-quiet" :href="`${repo}/releases`">{{ version }} ↗</a>
        </div>

        <div class="kh-hero-grid">
          <div class="kh-hero-text">
            <p class="kh-kicker" data-rise>{{ t.hero.aside }}</p>
            <h1 class="kh-title">
              <span ref="line1" class="kh-line">{{ t.hero.line1 }}</span>
              <span ref="mark" class="kh-mark" aria-hidden="true"><Star /></span>
              <em ref="line2" class="kh-line kh-serif">{{ t.hero.line2 }}</em>
            </h1>
            <p class="kh-sub" data-rise="1">{{ t.hero.sub }}</p>
            <div class="kh-actions" data-rise="2">
              <a class="kh-pill" :href="to('/getting-started')">{{ t.hero.primary }}<span aria-hidden="true">↗</span></a>
              <a class="kh-link" :href="to('/how-it-works')">{{ t.hero.secondary }}<span aria-hidden="true">↓</span></a>
            </div>
            <div class="kh-install" data-rise="3">
              <button ref="chip" class="kh-chip" type="button" :aria-label="install" @click="copy">
                <span class="kh-prompt" aria-hidden="true">$</span>
                <code ref="chipText">{{ copied ? t.hero.copied : install }}</code>
                <i ref="spark" class="kh-spark" aria-hidden="true" />
              </button>
            </div>
          </div>
          <Line class="kh-hero-stage" :copy="t.line" data-rise="2" />
        </div>
      </div>

      <div class="kh-belt" aria-hidden="true">
        <div ref="belt" class="kh-belt-track">
          <template v-for="n in 4" :key="n">
            <span v-for="s in t.stores" :key="`${n}-${s}`">{{ s }}<Star /></span>
          </template>
        </div>
      </div>
    </section>

    <section class="kh-section">
      <div class="kh-shell">
        <header class="kh-head">
          <p class="kh-eyebrow" data-rise>{{ t.features.eyebrow }}</p>
          <h2 data-rise="1">{{ t.features.line1 }}<em class="kh-serif">{{ t.features.line2 }}</em></h2>
          <p class="kh-lede" data-rise="2">{{ t.features.sub }}</p>
        </header>
        <div class="kh-cards">
          <div v-for="(c, i) in t.features.cards" :key="c.id" class="kh-cell" :data-rise="i % 3">
            <a class="kh-card" :href="to(c.link)">
              <Demo :id="c.id" :tag="c.tag" />
              <div class="kh-card-text">
                <h3>{{ c.title }}<span aria-hidden="true">↗</span></h3>
                <p>{{ c.body }}</p>
              </div>
            </a>
          </div>
        </div>
      </div>
    </section>

    <section class="kh-section kh-band">
      <div class="kh-shell">
        <header class="kh-head">
          <p class="kh-eyebrow" data-rise>{{ t.placement.eyebrow }}</p>
          <h2 data-rise="1">{{ t.placement.line1 }}<em class="kh-serif">{{ t.placement.line2 }}</em></h2>
          <p class="kh-lede" data-rise="2">{{ t.placement.sub }}</p>
        </header>
        <Placement :copy="t.placement" :lang="lang" data-rise="1" />
        <div class="kh-foot" data-rise="2">
          <span class="kh-eyebrow kh-quiet">{{ t.placement.note }}</span>
          <a class="kh-link" :href="to('/placement')">{{ t.placement.more }}<span aria-hidden="true">↗</span></a>
        </div>
      </div>
    </section>

    <section class="kh-section">
      <div class="kh-shell kh-split">
        <header class="kh-head">
          <p class="kh-eyebrow" data-rise>{{ t.code.eyebrow }}</p>
          <h2 data-rise="1">{{ t.code.line1 }}<em class="kh-serif">{{ t.code.line2 }}</em></h2>
          <p class="kh-lede" data-rise="2">{{ t.code.sub }}</p>
          <p class="kh-more" data-rise="3"><a class="kh-link" :href="to('/getting-started')">{{ t.code.link }}<span aria-hidden="true">↗</span></a></p>
        </header>
        <div class="kh-code" data-rise="1">
          <div class="kh-code-bar"><i /><i /><i /><span>main.go</span></div>
          <pre><code v-html="code" /></pre>
        </div>
      </div>
    </section>

    <section class="kh-section kh-paper">
      <div class="kh-shell">
        <header class="kh-head">
          <p class="kh-eyebrow" data-rise>{{ t.dash.eyebrow }}</p>
          <h2 data-rise="1">{{ t.dash.line1 }}<em class="kh-serif">{{ t.dash.line2 }}</em></h2>
          <p class="kh-lede" data-rise="2">{{ t.dash.sub }}</p>
        </header>
        <div ref="scene" class="kh-scene" data-rise="1">
          <div ref="frame" class="kh-frame">
            <img class="kh-shot-dark" :src="`${shots}/dashboard-dark.png`" :alt="t.dash.alt" loading="lazy" />
            <img class="kh-shot-light" :src="`${shots}/dashboard-light.png`" :alt="t.dash.alt" loading="lazy" />
          </div>
          <span v-for="(c, i) in t.dash.chips" :key="c" ref="floats" class="kh-float" :class="`f${i}`" aria-hidden="true">{{ c }}</span>
        </div>
        <div class="kh-foot" data-rise="2">
          <span />
          <a class="kh-link" :href="to('/dashboard')">{{ t.dash.link }}<span aria-hidden="true">↗</span></a>
        </div>
      </div>
    </section>

    <section class="kh-section">
      <div ref="proof" class="kh-shell">
        <header class="kh-head">
          <p class="kh-eyebrow" data-rise>{{ t.proof.eyebrow }}</p>
          <h2 data-rise="1">{{ t.proof.line1 }}<em class="kh-serif">{{ t.proof.line2 }}</em></h2>
        </header>
        <div class="kh-stats">
          <div v-for="(s, i) in t.proof.stats" :key="s.label" class="kh-stat" :data-rise="i">
            <strong ref="numbers">0</strong>
            <span>{{ s.label }}</span>
          </div>
        </div>
        <div class="kh-foot" data-rise="1">
          <span class="kh-lede">{{ t.proof.sub }}</span>
          <a class="kh-link" :href="to('/performance')">{{ t.proof.link }}<span aria-hidden="true">↗</span></a>
        </div>
        <div class="kh-users" data-rise="2">
          <span class="kh-eyebrow kh-quiet">{{ t.proof.users }}</span>
          <a v-for="u in users" :key="u.name" :href="u.href" target="_blank" rel="noopener noreferrer">{{ u.name }}</a>
        </div>
      </div>
    </section>

    <section class="kh-finale">
      <div class="kh-shell">
        <h2 data-rise>{{ t.finale.line1 }}<em class="kh-serif">{{ t.finale.line2 }}</em></h2>
        <div class="kh-actions" data-rise="1">
          <a class="kh-pill" :href="to('/getting-started')">{{ t.finale.primary }}<span aria-hidden="true">↗</span></a>
          <a class="kh-link" :href="repo" target="_blank" rel="noopener noreferrer">{{ t.finale.secondary }}<span aria-hidden="true">↗</span></a>
        </div>
        <p class="kh-eyebrow kh-quiet" data-rise="2">{{ t.finale.note }}</p>
      </div>
      <p class="kh-word" aria-hidden="true">kiln<i>.</i></p>
    </section>
  </div>
</template>

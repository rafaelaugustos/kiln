import { inject, onBeforeUnmount, onMounted, provide, type InjectionKey } from 'vue'
import type { WebRuntime } from '@rafaelaugusto/jolt/web'

export type Runtime = WebRuntime

export interface Motion {
  ready: Promise<Runtime>
}

const key: InjectionKey<Motion> = Symbol('kiln-motion')

export function provideMotion() {
  let resolve!: (rt: Runtime) => void
  const motion: Motion = { ready: new Promise<Runtime>((r) => (resolve = r)) }
  let rt: Runtime | undefined
  let gone = false
  provide(key, motion)
  onMounted(async () => {
    const { createWebRuntime } = await import('@rafaelaugusto/jolt/web')
    if (gone) return
    rt = createWebRuntime(document)
    resolve(rt)
  })
  onBeforeUnmount(() => {
    gone = true
    rt?.dispose()
  })
  return motion
}

export function useMotion(setup: (rt: Runtime, alive: () => boolean) => void | (() => void), own?: Motion) {
  const motion = own ?? inject(key)
  let live = false
  let cleanup: void | (() => void)
  onMounted(() => {
    live = true
    motion?.ready
      .then((rt) => {
        if (live) cleanup = setup(rt, () => live)
      })
      .catch((err) => console.error('kiln: motion', err))
  })
  onBeforeUnmount(() => {
    live = false
    cleanup?.()
  })
}

export function wait(ms: number) {
  return new Promise<void>((r) => setTimeout(r, ms))
}

export function onScreen(el: Element, change: (visible: boolean) => void, threshold = 0.2) {
  const io = new IntersectionObserver(([e]) => change(e.isIntersecting), { threshold })
  io.observe(el)
  return () => io.disconnect()
}

export async function loop(run: () => Promise<unknown>, going: () => boolean, rest = 600) {
  while (going()) {
    try {
      await run()
    } catch {
      return
    }
    await wait(rest)
  }
}

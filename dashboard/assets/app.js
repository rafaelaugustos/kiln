const svgNS = 'http://www.w3.org/2000/svg';
const fmt = new Intl.NumberFormat('en-US');
const clock = new Intl.DateTimeFormat([], { hour: 'numeric', minute: '2-digit' });
const api = document.body.dataset.api;
const calm = matchMedia('(prefers-reduced-motion: reduce)');
const series = [
  { key: 'succeeded', label: 'Succeeded' },
  { key: 'failed', label: 'Failed' },
];

function node(tag, cls, text) {
  const e = document.createElement(tag);
  if (cls) e.className = cls;
  if (text !== undefined) e.textContent = text;
  return e;
}

function shape(tag, attrs) {
  const e = document.createElementNS(svgNS, tag);
  for (const [k, v] of Object.entries(attrs)) e.setAttribute(k, v);
  return e;
}

async function load(url) {
  const res = await fetch(url, { headers: { Accept: 'application/json' }, credentials: 'same-origin', cache: 'no-store' });
  if (!res.ok) throw new Error(`${res.status} ${url}`);
  return res.json();
}

function every(ms, fn, fail) {
  let timer = 0;
  let busy = false;
  const run = async () => {
    clearTimeout(timer);
    if (busy) return;
    busy = true;
    try {
      if (!document.hidden) await fn();
    } catch (err) {
      fail?.(err);
    } finally {
      busy = false;
    }
    timer = setTimeout(run, ms);
  };
  document.addEventListener('visibilitychange', () => {
    if (!document.hidden) run();
  });
  run();
  return run;
}

function count(v, capped) {
  return fmt.format(v) + (capped && v >= 100000 ? '+' : '');
}

function compact(v) {
  if (v >= 1e6) return `${+(v / 1e6).toFixed(1)}M`;
  if (v >= 1e3) return `${+(v / 1e3).toFixed(1)}k`;
  return String(v);
}

function nice(v) {
  const p = 10 ** Math.floor(Math.log10(Math.max(v, 1)));
  for (const m of [1, 2, 4, 6, 8, 10]) {
    const n = m * p;
    if (n >= v && n >= 2 && n % 2 === 0) return n;
  }
  return 10 * p;
}

function bar(x, y, w, h, r) {
  r = Math.min(r, h, w / 2);
  return `M${x},${y + h}V${y + r}Q${x},${y} ${x + r},${y}H${x + w - r}Q${x + w},${y} ${x + w},${y + r}V${y + h}Z`;
}

function labelEvery(slot, step) {
  const ks = [1, 2, 5, 10, 15, 30, 60, 120, 180, 240, 360, 720]
    .map(min => (min * 60) / step)
    .filter(k => Number.isInteger(k) && k >= 1);
  return ks.find(k => slot * k >= 64) ?? ks[ks.length - 1];
}

function mood(o) {
  const c = o.counts;
  if (!o.servers && !c.awaiting && !c.scheduled && !c.throttled && !c.enqueued && !c.processing && !c.failed && !c.succeeded && !c.deleted) return 'fresh';
  if (!o.servers) return 'cold';
  if (o.counts.failed) return 'failed';
  if (o.counts.processing) return 'busy';
  if (o.counts.enqueued) return 'waiting';
  return 'idle';
}

function heat(o) {
  if (!o.servers) return 'off';
  if (!o.running || !o.workers) return 'idle';
  if (o.running * 3 < o.workers) return 'low';
  if (o.running * 3 < o.workers * 2) return 'mid';
  return 'high';
}

function bump(n) {
  if (calm.matches) return;
  n.classList.remove('bump');
  void n.offsetWidth;
  n.classList.add('bump');
}

function live() {
  const hero = document.querySelector('[data-hero]');
  const dot = document.querySelector('[data-live]');
  const text = dot?.querySelector('[data-live-text]');
  const link = up => {
    if (!dot) return;
    dot.dataset.live = up ? 'up' : 'down';
    text.textContent = up ? 'Live' : 'Reconnecting…';
  };
  every(5000, async () => {
    const o = await load(`${api}/overview`);
    link(true);
    const values = { ...o.counts, servers: o.servers, recurring: o.recurring, queues: o.queues, workers: o.workers, running: o.running, paused: o.paused_queues };
    for (const n of document.querySelectorAll('[data-count]')) {
      const v = values[n.dataset.count];
      if (v === undefined) continue;
      const t = count(v, o.counts.capped);
      if (n.textContent !== t) {
        n.textContent = t;
        bump(n);
      }
      if (n.classList.contains('pill')) n.hidden = v === 0;
    }
    for (const n of document.querySelectorAll('[data-plural]')) {
      const v = values[n.dataset.plural];
      if (v !== undefined) n.textContent = v === 1 ? n.dataset.one : n.dataset.other;
    }
    for (const n of document.querySelectorAll('[data-show]')) n.hidden = !values[n.dataset.show];
    for (const n of document.querySelectorAll('[data-stat]')) n.classList.toggle('zero', !values[n.dataset.stat]);
    for (const n of document.querySelectorAll('[data-meter]')) n.setAttribute('width', `${o.workers ? Math.min(100, (o.running / o.workers) * 100).toFixed(1) : 0}%`);
    if (!hero) return;
    hero.dataset.heat = heat(o);
    const m = mood(o);
    if (hero.dataset.mood === m) return;
    if (hero.dataset.mood === 'failed' && (m === 'busy' || m === 'waiting' || m === 'idle')) party(hero);
    hero.dataset.mood = m;
    for (const n of hero.querySelectorAll('[data-mood-is]')) {
      const on = n.dataset.moodIs === m;
      n.hidden = !on;
      n.classList.toggle('enter', on);
    }
  }, () => link(false));
}

const quips = {
  fresh: ['Ready when you are.', 'Fire me up!'],
  cold: ['Zzz…', 'Five more minutes…'],
  failed: ['Some pots cracked.', 'Not my best batch.', 'Let’s look at those?'],
  busy: ['Firing away.', 'Careful, 1200 °C in here.', 'Hot hands!'],
  waiting: ['Waiting for a worker…', 'Anyone there?'],
  idle: ['Just keeping warm.', 'Nice and quiet.'],
  party: ['All fixed. Nice!', 'Clean batch!'],
};

function say(hero, key) {
  const list = quips[key] ?? quips.idle;
  let b = hero.querySelector('.quip');
  if (!b) {
    b = node('span', 'quip');
    b.setAttribute('aria-hidden', 'true');
    hero.append(b);
  }
  const prev = b.textContent;
  b.textContent = list.find(q => q !== prev && Math.random() < 0.6) ?? list.find(q => q !== prev) ?? list[0];
  b.classList.remove('on');
  void b.offsetWidth;
  b.classList.add('on');
  clearTimeout(b.timer);
  b.timer = setTimeout(() => b.classList.remove('on'), 2400);
}

function party(hero) {
  say(hero, 'party');
  if (calm.matches) return;
  const box = hero.querySelector('.party');
  const colors = ['var(--flame-1)', 'var(--flame-2)', 'var(--succeeded)', 'var(--enqueued)', 'var(--processing)', 'var(--scheduled)'];
  const m = hero.querySelector('.mascot');
  const x = m.offsetLeft + m.offsetWidth / 2;
  const y = m.offsetTop + m.offsetHeight / 2;
  for (let i = 0; i < 28; i++) {
    const c = node('i');
    const a = Math.random() * Math.PI * 2;
    const d = 80 + Math.random() * 140;
    c.style.setProperty('--x', `${x}px`);
    c.style.setProperty('--y', `${y}px`);
    c.style.setProperty('--dx', `${Math.cos(a) * d}px`);
    c.style.setProperty('--dy', `${Math.sin(a) * d - 40}px`);
    c.style.setProperty('--r', `${(Math.random() - 0.5) * 720}deg`);
    c.style.setProperty('--s', `${6 + Math.random() * 6}px`);
    c.style.setProperty('--pc', colors[i % colors.length]);
    box.append(c);
  }
  setTimeout(() => box.replaceChildren(), 1500);
}

function mascot() {
  const hero = document.querySelector('[data-hero]');
  if (!hero) return;
  const hello = hero.querySelector('[data-hello]');
  const h = new Date().getHours();
  const name = hello.dataset.name;
  if (h < 5) hello.textContent = name ? `Up late, ${name}?` : 'Up late?';
  else hello.textContent = `${h < 12 ? 'Good morning' : h < 18 ? 'Good afternoon' : 'Good evening'}${name ? `, ${name}` : ''}`;
  const btn = hero.querySelector('[data-poke]');
  btn.addEventListener('click', () => {
    btn.classList.remove('poke');
    void btn.offsetWidth;
    btn.classList.add('poke');
    say(hero, hero.dataset.mood);
  });
  if (calm.matches || !matchMedia('(pointer: fine)').matches) return;
  const eyes = hero.querySelector('.kiln .eyes');
  let raf = 0;
  document.addEventListener('pointermove', e => {
    if (raf || hero.classList.contains('still')) return;
    raf = requestAnimationFrame(() => {
      raf = 0;
      const r = btn.getBoundingClientRect();
      const dx = e.clientX - (r.left + r.width / 2);
      const dy = e.clientY - (r.top + r.height * 0.62);
      const len = Math.hypot(dx, dy) || 1;
      const k = Math.min(1, len / 240) * 1.3;
      eyes.style.transform = `translate(${((dx / len) * k).toFixed(2)}px, ${((dy / len) * k).toFixed(2)}px)`;
    });
  }, { passive: true });
}

function still() {
  const sync = () => document.documentElement.classList.toggle('still', document.hidden);
  document.addEventListener('visibilitychange', sync);
  sync();
  const hero = document.querySelector('[data-hero]');
  if (!hero) return;
  new IntersectionObserver(([e]) => hero.classList.toggle('still', !e.isIntersecting)).observe(hero);
}

function chart(root) {
  let data = null;
  let fresh = true;
  const tip = node('div', 'tip');
  tip.hidden = true;
  const visible = () => !root.closest('[hidden]');
  const draw = () => {
    if (!data || !visible()) return;
    render(root, tip, data, fresh && !calm.matches);
    fresh = false;
  };
  new ResizeObserver(draw).observe(root);
  const run = every(Number(root.dataset.every) || 60000, async () => {
    if (!visible()) return;
    data = await load(root.dataset.src);
    draw();
    const panel = root.closest('.range') ?? root.closest('.panel');
    for (const s of series) {
      const n = panel.querySelector(`[data-total="${s.key}"]`);
      if (n) n.textContent = fmt.format(data.points.reduce((sum, p) => sum + p[s.key], 0));
    }
  }, () => {
    if (data) return;
    const msg = node('div', 'chart-msg error');
    msg.append(node('span', '', 'Couldn’t load this chart. Trying again…'));
    root.replaceChildren(msg);
  });
  return () => {
    fresh = true;
    if (data) draw();
    run();
  };
}

function render(root, tip, data, intro) {
  const pts = data.points;
  const W = root.clientWidth;
  const H = root.clientHeight;
  if (!pts.length || W <= 0) return;
  const m = { t: 14, r: 16, b: 26, l: 48 };
  const w = W - m.l - m.r;
  const h = H - m.t - m.b;
  const max = nice(Math.max(...pts.map(p => series.reduce((n, s) => n + p[s.key], 0))));
  const slot = w / pts.length;
  const bw = Math.max(1, Math.min(18, slot - Math.max(2, slot * 0.3)));
  const y = v => m.t + h - (v / max) * h;
  const totals = series.map(s => pts.reduce((n, p) => n + p[s.key], 0));
  const sums = series.map((s, i) => `${fmt.format(totals[i])} ${s.label.toLowerCase()}`);
  const svg = shape('svg', { width: W, height: H, viewBox: `0 0 ${W} ${H}`, role: 'img', 'aria-label': sums.join(', ') });
  if (intro) svg.classList.add('intro');

  for (const v of [0, max / 2, max]) {
    const yy = Math.round(y(v)) + 0.5;
    svg.append(shape('line', { class: v === 0 ? 'base-line' : 'grid-line', x1: m.l, x2: W - m.r, y1: yy, y2: yy }));
    const t = shape('text', { class: 'tick', x: m.l - 10, y: yy + 3.5, 'text-anchor': 'end' });
    t.textContent = compact(v);
    svg.append(t);
  }

  const cursor = shape('rect', { class: 'cursor', x: 0, y: m.t, width: slot, height: h, rx: 4, visibility: 'hidden' });
  svg.append(cursor);

  const cols = pts.map((p, i) => {
    const g = shape('g', { class: 'col' });
    g.style.setProperty('--i', i);
    const x = m.l + i * slot + (slot - bw) / 2;
    const segs = series.filter(s => p[s.key] > 0);
    let base = m.t + h;
    segs.forEach((s, j) => {
      if (j > 0) base -= 2;
      const hh = Math.max(2, (p[s.key] / max) * h);
      const r = j === segs.length - 1 ? Math.min(3, bw / 3) : 0;
      g.append(shape('path', { class: `f-${s.key}`, d: bar(x, base - hh, bw, hh, r) }));
      base -= hh;
    });
    svg.append(g);
    return g;
  });

  const step = data.step;
  const k = labelEvery(slot, step);
  pts.forEach((p, i) => {
    const at = new Date(p.at);
    const x = m.l + i * slot + slot / 2;
    if (Math.round(at.getTime() / 1000 / step) % k !== 0 || x < m.l + 24 || x > W - m.r - 24) return;
    const t = shape('text', { class: 'tick', x, y: H - 8, 'text-anchor': 'middle' });
    t.textContent = clock.format(at);
    svg.append(t);
  });

  const hit = shape('rect', { x: m.l, y: 0, width: w, height: H, fill: 'transparent' });
  svg.append(hit);
  let on = -1;
  hit.addEventListener('pointermove', e => {
    const box = svg.getBoundingClientRect();
    const i = Math.min(pts.length - 1, Math.max(0, Math.floor((e.clientX - box.left - m.l) / slot)));
    if (i === on) return;
    on = i;
    svg.classList.add('hover');
    cols.forEach((g, j) => g.classList.toggle('on', j === i));
    cursor.setAttribute('x', m.l + i * slot);
    cursor.setAttribute('visibility', 'visible');
    const p = pts[i];
    const from = new Date(p.at);
    const title = step > 60 ? `${clock.format(from)} – ${clock.format(new Date(from.getTime() + step * 1000))}` : clock.format(from);
    tip.replaceChildren(node('div', 't', title), ...series.map(s => {
      const row = node('div', 'row');
      row.append(node('i', `f-${s.key}`), node('span', '', s.label), node('b', '', fmt.format(p[s.key])));
      return row;
    }));
    tip.hidden = false;
    const cx = m.l + i * slot;
    const tw = tip.offsetWidth;
    tip.style.left = `${cx + slot + tw + 12 > W ? Math.max(0, cx - tw - 8) : cx + slot + 8}px`;
    tip.style.top = `${m.t}px`;
  });
  hit.addEventListener('pointerleave', () => {
    on = -1;
    svg.classList.remove('hover');
    cursor.setAttribute('visibility', 'hidden');
    tip.hidden = true;
  });

  const parts = [svg, tip];
  if (totals.every(n => n === 0)) {
    const msg = node('div', 'chart-msg');
    msg.append(node('span', '', root.dataset.empty || 'Nothing finished yet.'));
    parts.push(msg);
  }
  root.replaceChildren(...parts);
}

function ranges(panel, refresh) {
  const seg = panel.querySelector('.seg');
  const tabs = [...seg.querySelectorAll('[role="tab"]')];
  const key = 'kiln.range';
  const pick = (tab, focus) => {
    tabs.forEach((t, i) => {
      const on = t === tab;
      t.setAttribute('aria-selected', on);
      t.tabIndex = on ? 0 : -1;
      const p = document.getElementById(t.getAttribute('aria-controls'));
      if (p.hidden === on) {
        p.hidden = !on;
        p.classList.toggle('enter', on);
      }
      if (on) {
        seg.dataset.at = i;
        refresh.get(p.querySelector('[data-chart]'))?.();
      }
    });
    if (focus) tab.focus();
    try {
      localStorage.setItem(key, tab.dataset.range);
    } catch {}
  };
  seg.addEventListener('click', e => {
    const t = e.target.closest('[role="tab"]');
    if (t) pick(t);
  });
  seg.addEventListener('keydown', e => {
    const i = tabs.indexOf(document.activeElement);
    if (i < 0) return;
    const d = { ArrowRight: 1, ArrowLeft: -1 }[e.key];
    if (!d) return;
    e.preventDefault();
    pick(tabs[(i + d + tabs.length) % tabs.length], true);
  });
  let saved = null;
  try {
    saved = localStorage.getItem(key);
  } catch {}
  const t = tabs.find(t => t.dataset.range === saved);
  if (t && t.getAttribute('aria-selected') !== 'true') pick(t);
}

function bulk() {
  const form = document.getElementById('bulk');
  if (!form) return;
  const boxes = () => [...document.querySelectorAll('input[name="id"][form="bulk"]')];
  const all = document.querySelector('[data-all]');
  const label = form.querySelector('[data-selected]');
  const sync = () => {
    const list = boxes();
    const n = list.filter(b => b.checked).length;
    for (const b of form.querySelectorAll('[data-needs-selection]')) b.disabled = n === 0;
    label.hidden = n === 0;
    label.textContent = `${fmt.format(n)} selected`;
    if (all) {
      all.checked = n > 0 && n === list.length;
      all.indeterminate = n > 0 && n < list.length;
    }
  };
  let last = -1;
  document.addEventListener('click', e => {
    const b = e.target;
    if (!(b instanceof HTMLInputElement) || !b.matches('input[name="id"][form="bulk"]')) return;
    const list = boxes();
    const i = list.indexOf(b);
    if (e.shiftKey && last >= 0) {
      for (const x of list.slice(Math.min(i, last), Math.max(i, last) + 1)) x.checked = b.checked;
    }
    last = i;
    sync();
  });
  all?.addEventListener('change', () => {
    for (const b of boxes()) b.checked = all.checked;
    sync();
  });
  sync();
}

function flash() {
  const f = document.querySelector('[data-flash]');
  if (!f) return;
  const u = new URL(location.href);
  if (u.searchParams.has('done')) {
    u.searchParams.delete('done');
    u.searchParams.delete('n');
    history.replaceState(history.state, '', u);
  }
  f.querySelector('[data-dismiss]')?.addEventListener('click', () => {
    if (calm.matches) return f.remove();
    f.classList.add('out');
    f.addEventListener('animationend', () => f.remove(), { once: true });
  });
}

document.addEventListener('submit', e => {
  const msg = e.submitter?.dataset.confirm;
  if (msg && !window.confirm(msg)) e.preventDefault();
});

for (const s of document.querySelectorAll('[data-autosubmit]')) {
  s.addEventListener('change', () => s.form.requestSubmit());
}

const refresh = new Map();
for (const c of document.querySelectorAll('[data-chart]')) refresh.set(c, chart(c));
for (const p of document.querySelectorAll('[data-ranges]')) ranges(p, refresh);
live();
still();
mascot();
bulk();
flash();

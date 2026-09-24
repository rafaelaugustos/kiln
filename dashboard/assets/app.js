const svgNS = 'http://www.w3.org/2000/svg';
const fmt = new Intl.NumberFormat('en-US');
const clock = new Intl.DateTimeFormat([], { hour: 'numeric', minute: '2-digit' });
const api = document.body.dataset.api;
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

function every(ms, fn) {
  let timer = 0;
  let busy = false;
  const run = async () => {
    clearTimeout(timer);
    if (busy) return;
    busy = true;
    try {
      if (!document.hidden) await fn();
    } catch {
    } finally {
      busy = false;
    }
    timer = setTimeout(run, ms);
  };
  document.addEventListener('visibilitychange', () => {
    if (!document.hidden) run();
  });
  run();
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

function live() {
  every(5000, async () => {
    const o = await load(`${api}/overview`);
    const values = { ...o.counts, servers: o.servers, recurring: o.recurring, queues: o.queues, workers: o.workers, running: o.running };
    for (const n of document.querySelectorAll('[data-count]')) {
      const v = values[n.dataset.count];
      if (v === undefined) continue;
      n.textContent = count(v, o.counts.capped);
      if (n.classList.contains('pill')) n.hidden = v === 0;
    }
  });
}

function chart(root) {
  let data = null;
  const tip = node('div', 'tip');
  tip.hidden = true;
  const draw = () => data && render(root, tip, data);
  new ResizeObserver(draw).observe(root);
  every(Number(root.dataset.every) || 60000, async () => {
    data = await load(root.dataset.src);
    draw();
    const panel = root.closest('.panel');
    for (const s of series) {
      const n = panel.querySelector(`[data-total="${s.key}"]`);
      if (n) n.textContent = fmt.format(data.points.reduce((sum, p) => sum + p[s.key], 0));
    }
  });
}

function render(root, tip, data) {
  const pts = data.points;
  if (!pts.length) return;
  const W = root.clientWidth;
  const H = root.clientHeight;
  const m = { t: 14, r: 16, b: 26, l: 48 };
  const w = W - m.l - m.r;
  const h = H - m.t - m.b;
  const max = nice(Math.max(...pts.map(p => series.reduce((n, s) => n + p[s.key], 0))));
  const slot = w / pts.length;
  const bw = Math.max(1, Math.min(18, slot - Math.max(2, slot * 0.3)));
  const y = v => m.t + h - (v / max) * h;
  const sums = series.map(s => `${fmt.format(pts.reduce((n, p) => n + p[s.key], 0))} ${s.label.toLowerCase()}`);
  const svg = shape('svg', { width: W, height: H, viewBox: `0 0 ${W} ${H}`, role: 'img', 'aria-label': sums.join(', ') });

  for (const v of [0, max / 2, max]) {
    const yy = Math.round(y(v)) + 0.5;
    svg.append(shape('line', { class: v === 0 ? 'base-line' : 'grid-line', x1: m.l, x2: W - m.r, y1: yy, y2: yy }));
    const t = shape('text', { class: 'tick', x: m.l - 10, y: yy + 3.5, 'text-anchor': 'end' });
    t.textContent = compact(v);
    svg.append(t);
  }

  const cursor = shape('rect', { class: 'cursor', x: 0, y: m.t, width: slot, height: h, rx: 3, visibility: 'hidden' });
  svg.append(cursor);

  const cols = pts.map((p, i) => {
    const g = shape('g', { class: 'col' });
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

  root.replaceChildren(svg, tip);
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

document.addEventListener('submit', e => {
  const msg = e.submitter?.dataset.confirm;
  if (msg && !window.confirm(msg)) e.preventDefault();
});

for (const s of document.querySelectorAll('[data-autosubmit]')) {
  s.addEventListener('change', () => s.form.requestSubmit());
}

for (const c of document.querySelectorAll('[data-chart]')) chart(c);
live();
bulk();

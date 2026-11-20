local P, OP, A, NOW, call, fmt, floor
local S = {}
local H = {}

local ARCH = {succeeded = true, deleted = true}
local FINAL = {succeeded = true, failed = true, deleted = true}
local BUSY = {enqueued = true, processing = true}
local MASK = {succeeded = 1, failed = 2, deleted = 4}
local STAT = {succeeded = 'succeeded', failed = 'failed', deleted = 'deleted', scheduled = 'retried'}
local STATES = {'awaiting', 'scheduled', 'throttled', 'enqueued', 'processing', 'succeeded', 'failed', 'deleted'}
local VALID = {
  awaiting = true, scheduled = true, throttled = true, enqueued = true, processing = true, succeeded = true,
  failed = true, deleted = true,
}
local NUM = {
  pri = true, att = true, max = true, claim = true, run = true, cr = true, at = true, fin = true,
  cancel = true, b = true, ab = true, ufor = true, gr = true, pend = true, prog = true, lseq = true,
  kids = true,
}
local FIELDS = {
  'st', 'q', 'k', 'args', 'meta', 'tags', 'title', 'pri', 'att', 'max', 'claim', 'tmo', 'run', 'cr', 'at',
  'fin', 'srv', 'cancel', 'b', 'ab', 'rec', 'uk', 'ufor', 'lim', 'gr', 'deps', 'pend', 'out', 'prog',
  'lseq', 'lr', 'kids',
}
local BLANK = {
  q = '', k = '', args = '', title = '', tmo = '', srv = '', rec = '', uk = '', lim = '', lr = '',
  pri = 0, att = 0, max = 0, claim = 0, run = 0, cr = 0, at = 0, fin = 0, cancel = 0, b = 0, ab = 0, ufor = 0,
  gr = 0, pend = 0, prog = 0, lseq = 0, kids = 0,
}
local JOB = {__index = BLANK}

local jobs, limits, batches, ready, queues, events, resolved, done, admit, admitting
local zops, tally, stats, kinds, retries, digits, padded

local function reset(args)
  call, fmt, floor = redis.call, string.format, math.floor
  P, OP = args[1], args[2]
  A = cjson.decode(args[3])
  local tm = call('TIME')
  NOW = tonumber(tm[1]) * 1000000 + tonumber(tm[2])
  jobs, limits, batches = {}, {}, {}
  ready, queues, events = {}, {}, {}
  resolved, done, admit, admitting = {}, {}, {}, {}
  zops, tally, stats, kinds = {}, {}, {}, {}
  digits, padded = {}, {}
  retries = 0
end

local function I(n)
  local s = digits[n]
  if not s then
    s = fmt('%.0f', n)
    digits[n] = s
  end
  return s
end

local function M(id)
  local s = padded[id]
  if not s then
    s = fmt('%016.0f', id)
    padded[id] = s
  end
  return s
end

local function PM(pri, id)
  return fmt('%05d%016.0f', 32767 - pri, id)
end

local function L(t)
  if t == nil or next(t) == nil then
    return nil
  end
  return t
end

local function fail(code, msg)
  error({e = code, m = msg}, 0)
end

local function has(mask, st)
  local bit = MASK[st]
  if not bit then
    return false
  end
  return floor(mask / bit) % 2 == 1
end

local function set(j, f, v)
  if j[f] ~= v then
    j[f] = v
    j.d[f] = true
  end
end

local function jkey(id)
  return P .. 'j:' .. I(id)
end

local function qkey(q, k)
  return P .. 'q:' .. q .. '/' .. k
end

local function pmid(m)
  return tonumber(string.sub(m, 6))
end

function S.job(id)
  local j = jobs[id]
  if j ~= nil then
    return j or nil
  end
  local h = call('HGETALL', jkey(id))
  if #h == 0 then
    jobs[id] = false
    return nil
  end
  j = setmetatable({id = id, d = {}}, JOB)
  for i = 1, #h, 2 do
    local f, v = h[i], h[i + 1]
    if NUM[f] then
      j[f] = tonumber(v)
    else
      j[f] = v
    end
  end
  jobs[id] = j
  return j
end

local function zadd(key, score, member)
  local t = zops[key]
  if not t then
    t = {}
    zops[key] = t
  end
  t[member] = score
end

local function zrem(key, member)
  local t = zops[key]
  if not t then
    t = {}
    zops[key] = t
  end
  t[member] = false
end

local function count(q, st, n)
  local t = tally[q]
  if not t then
    t = {}
    tally[q] = t
  end
  t[st] = (t[st] or 0) + n
end

function S.flushz()
  for key, t in pairs(zops) do
    local add, rem = {}, {}
    for m, score in pairs(t) do
      if score then
        add[#add + 1] = score
        add[#add + 1] = m
      else
        rem[#rem + 1] = m
      end
    end
    for i = 1, #rem, 3000 do
      call('ZREM', key, unpack(rem, i, math.min(i + 2999, #rem)))
    end
    for i = 1, #add, 3000 do
      call('ZADD', key, unpack(add, i, math.min(i + 2999, #add)))
    end
  end
  zops = {}
end

function S.deps(j)
  if j.ds then
    return j.ds
  end
  local ds = {}
  if j.deps and j.deps ~= '' then
    for p, on, b, r in string.gmatch(j.deps, '(%d+):(%d+):(%d)(%d)') do
      ds[#ds + 1] = {p = tonumber(p), on = tonumber(on), b = b == '1', r = r == '1'}
    end
  end
  j.ds = ds
  return ds
end

local function depstr(ds)
  local parts = {}
  for i, d in ipairs(ds) do
    parts[i] = I(d.p) .. ':' .. I(d.on) .. ':' .. (d.b and '1' or '0') .. (d.r and '1' or '0')
  end
  return table.concat(parts, ',')
end

function S.dep(j, parent, batch)
  for _, d in ipairs(S.deps(j)) do
    if d.p == parent and d.b == batch and not d.r then
      return d
    end
  end
  return nil
end

function S.limit(key)
  local l = limits[key]
  if l then
    return l
  end
  local h = call('HGETALL', P .. 'l:' .. key)
  l = {key = key, max = 0, rate = 0, per = 0, burst = 0, rsv = 0, refs = 0, tat = 0, atat = 0}
  if #h == 0 then
    l.d = true
    call('ZADD', P .. 'lk', 0, key)
  else
    for i = 1, #h, 2 do
      l[h[i]] = tonumber(h[i + 1])
    end
  end
  limits[key] = l
  return l
end

function S.batch(id)
  local b = batches[id]
  if b ~= nil then
    return b or nil
  end
  local h = call('HGETALL', P .. 'b:' .. I(id))
  if #h == 0 then
    batches[id] = false
    return nil
  end
  b = {id = id, par = 0, desc = '', tot = 0, seal = 0, live = 0, nest = 0, unf = 0, cr = 0, fin = 0, c = {}}
  for i = 1, #h, 2 do
    local f, v = h[i], h[i + 1]
    if f == 'desc' or f == 'meta' then
      b[f] = v
    elseif VALID[f] then
      b.c[f] = tonumber(v)
    else
      b[f] = tonumber(v)
    end
  end
  batches[id] = b
  return b
end

function S.ready(q)
  if not queues[q] then
    queues[q] = true
    ready[#ready + 1] = q
  end
end

function S.flush()
  S.flushz()
  for q, t in pairs(tally) do
    for st, n in pairs(t) do
      if n ~= 0 then
        call('HINCRBY', P .. 'qc:' .. q, st, n)
      end
    end
  end
  if retries ~= 0 then
    call('INCRBY', P .. 'retries', retries)
  end
  if next(stats) then
    local minute = I(floor(NOW / 60000000) * 60)
    for f, n in pairs(stats) do
      call('HINCRBY', P .. 'st:' .. minute, f, n)
      call('HINCRBY', P .. 'tot', f, n)
    end
    call('ZADD', P .. 'stm', minute, minute)
  end
  for id, j in pairs(jobs) do
    if j and not j.gone then
      if j.dd then
        j.deps = depstr(j.ds)
        j.d.deps = true
      end
      local key = jkey(id)
      local a = {}
      if j.new then
        for _, f in ipairs(FIELDS) do
          local v = j[f]
          if v ~= nil and v ~= '' and v ~= 0 then
            a[#a + 1] = f
            a[#a + 1] = NUM[f] and I(v) or v
          end
        end
      else
        local del = {}
        for f in pairs(j.d) do
          local v = j[f]
          if v == nil or v == '' then
            del[#del + 1] = f
          else
            a[#a + 1] = f
            a[#a + 1] = NUM[f] and I(v) or v
          end
        end
        if #del > 0 then
          call('HDEL', key, unpack(del))
        end
      end
      if #a > 0 then
        call('HSET', key, unpack(a))
      end
    end
  end
  for key, l in pairs(limits) do
    if l.d and not l.gone then
      call('HSET', P .. 'l:' .. key, 'max', I(l.max), 'rate', I(l.rate), 'per', I(l.per), 'burst', I(l.burst),
        'rsv', I(l.rsv), 'refs', I(l.refs), 'tat', I(l.tat), 'atat', I(l.atat))
    end
  end
  for id, b in pairs(batches) do
    if b and b.d and not b.gone then
      local a = {'par', I(b.par), 'tot', I(b.tot), 'seal', I(b.seal), 'live', I(b.live), 'nest', I(b.nest),
        'unf', I(b.unf), 'cr', I(b.cr), 'fin', I(b.fin)}
      if b.desc ~= '' then
        a[#a + 1] = 'desc'
        a[#a + 1] = b.desc
      end
      if b.meta then
        a[#a + 1] = 'meta'
        a[#a + 1] = b.meta
      end
      for _, st in ipairs(STATES) do
        a[#a + 1] = st
        a[#a + 1] = I(b.c[st] or 0)
      end
      call('HSET', P .. 'b:' .. I(id), unpack(a))
    end
  end
end

function S.publish()
  local out = {}
  for _, e in ipairs(events) do
    out[#out + 1] = e
  end
  for _, q in ipairs(ready) do
    out[#out + 1] = {2, q, '0'}
  end
  if #out > 0 then
    call('PUBLISH', P .. 'ev', cjson.encode(out))
  end
end

function S.index(j)
  local st = j.st
  count(j.q, st, 1)
  if j.b ~= 0 then
    local b = S.batch(j.b)
    b.c[st] = (b.c[st] or 0) + 1
    if not ARCH[st] then
      b.live = b.live + 1
    end
    b.d = true
  end
  if st == 'enqueued' then
    local pm = PM(j.pri, j.id)
    local kk = j.q .. '/' .. j.k
    if not kinds[kk] then
      kinds[kk] = true
      call('SADD', P .. 'qk:' .. j.q, j.k)
    end
    zadd(P .. 'q:' .. kk, '0', pm)
    zadd(P .. 's:enqueued', '0', pm)
    zadd(P .. 'qr:' .. j.q, I(j.run), M(j.id))
    S.ready(j.q)
  elseif st == 'scheduled' then
    zadd(P .. 's:scheduled', I(j.run), M(j.id))
    if j.att > 0 then
      retries = retries + 1
    end
    if j.gr == 1 then
      local l = S.limit(j.lim)
      l.rsv = l.rsv + 1
      l.d = true
      S.ready(j.q)
    end
  elseif st == 'throttled' then
    local pm = PM(j.pri, j.id)
    call('ZADD', P .. 't:' .. j.lim, 0, pm)
    zadd(P .. 's:throttled', '0', pm)
  elseif st == 'processing' then
    local m = M(j.id)
    zadd(P .. 'p:' .. j.srv, I(j.id), m)
    zadd(P .. 's:processing', I(j.id), m)
  elseif st == 'awaiting' then
    zadd(P .. 's:awaiting', I(j.id), M(j.id))
  else
    zadd(P .. 's:' .. st, I(j.fin), M(j.id))
  end
end

function S.unindex(j)
  local st = j.st
  count(j.q, st, -1)
  if j.b ~= 0 then
    local b = S.batch(j.b)
    b.c[st] = (b.c[st] or 0) - 1
    if not ARCH[st] then
      b.live = b.live - 1
    end
    b.d = true
  end
  if st == 'enqueued' then
    local pm = PM(j.pri, j.id)
    zrem(qkey(j.q, j.k), pm)
    zrem(P .. 's:enqueued', pm)
    zrem(P .. 'qr:' .. j.q, M(j.id))
  elseif st == 'scheduled' then
    zrem(P .. 's:scheduled', M(j.id))
    if j.att > 0 then
      retries = retries - 1
    end
    if j.gr == 1 then
      local l = S.limit(j.lim)
      l.rsv = l.rsv - 1
      l.d = true
    end
  elseif st == 'throttled' then
    local pm = PM(j.pri, j.id)
    call('ZREM', P .. 't:' .. j.lim, pm)
    zrem(P .. 's:throttled', pm)
  elseif st == 'processing' then
    local m = M(j.id)
    zrem(P .. 'p:' .. j.srv, m)
    zrem(P .. 's:processing', m)
  else
    zrem(P .. 's:' .. st, M(j.id))
  end
end

function S.bump(st)
  local f = STAT[st]
  stats[f] = (stats[f] or 0) + 1
end

function S.rawu(key)
  local v = call('HGET', P .. 'u', key)
  if not v then
    return nil
  end
  local job, exp, dl = string.match(v, '^(%d+):(%d+):(%d+)$')
  return {job = tonumber(job), exp = tonumber(exp), dl = tonumber(dl)}
end

function S.setu(key, job, exp, dl)
  call('HSET', P .. 'u', key, I(job) .. ':' .. I(exp) .. ':' .. I(dl))
  if dl > 0 then
    call('ZADD', P .. 'ux', I(dl), key)
  elseif exp > 0 then
    call('ZADD', P .. 'ux', I(exp), key)
  else
    call('ZREM', P .. 'ux', key)
  end
end

function S.delu(key)
  call('HDEL', P .. 'u', key)
  call('ZREM', P .. 'ux', key)
end

function S.holder(key)
  local u = S.rawu(key)
  if not u or u.exp ~= 0 and u.exp <= NOW or u.dl ~= 0 and u.dl <= NOW then
    return nil
  end
  return u
end

function S.hold(j)
  if j.ufor > 0 then
    local exp = j.cr + j.ufor
    local t = S.rawu(j.uk)
    if t and t.job == j.id then
      exp = t.exp
    end
    S.setu(j.uk, j.id, exp, 0)
  elseif j.st == 'deleted' then
    S.release(j)
  else
    S.setu(j.uk, j.id, 0, 0)
  end
end

function S.release(j)
  if j.uk == '' or j.ufor > 0 then
    return
  end
  local u = S.rawu(j.uk)
  if u and u.job == j.id then
    S.delu(j.uk)
  end
end

function S.log(j, e)
  e.at = I(NOW)
  e.att = j.att
  local key = P .. 'h:' .. I(j.id)
  call('RPUSH', key, cjson.encode(e))
  call('LTRIM', key, -16, -1)
  set(j, 'lr', e.reason or '')
end

function S.admitting(key)
  if not admitting[key] then
    admitting[key] = true
    admit[#admit + 1] = key
  end
end

function S.move(j, to)
  local from = j.st
  S.unindex(j)
  set(j, 'st', to)
  if j.lim ~= '' and (BUSY[from] or false) ~= (BUSY[to] or false) then
    if BUSY[to] then
      call('SADD', P .. 'la:' .. j.lim, I(j.id))
    else
      call('SREM', P .. 'la:' .. j.lim, I(j.id))
      S.admitting(j.lim)
    end
  end
  if FINAL[to] then
    set(j, 'fin', NOW)
    S.release(j)
    S.bump(to)
    resolved[#resolved + 1] = {j, to}
  else
    set(j, 'fin', 0)
  end
  S.index(j)
  if j.b ~= 0 then
    S.complete(S.batch(j.b))
  end
end

function S.enqueue(j)
  if j.lim == '' then
    S.move(j, 'enqueued')
    return
  end
  S.move(j, 'throttled')
  S.admitting(j.lim)
end

function S.wake(j)
  if j.run > NOW then
    S.move(j, 'scheduled')
    return
  end
  S.enqueue(j)
end

local function interval(l)
  return math.max(floor(l.per / l.rate), 1)
end

local function tau(l)
  return (l.burst - 1) * interval(l)
end

local function slot(l)
  return math.max(NOW, l.tat - tau(l))
end

local function take(l, at)
  l.tat = math.max(l.tat, at) + interval(l)
  l.d = true
end

local function open(l)
  return l.atat - tau(l) - floor(interval(l) / 2) <= NOW
end

local function admitted(l)
  l.atat = math.max(l.atat, NOW) + interval(l)
  l.tat = math.max(l.tat, l.atat)
  l.d = true
end

local function full(l)
  return l.max > 0 and call('SCARD', P .. 'la:' .. l.key) >= l.max
end

function S.slot(l)
  return slot(l)
end

function S.admit(key, most)
  local l = S.limit(key)
  local wk = P .. 't:' .. key
  local n = 0
  while n < most and (l.rate == 0 or n < 1000) do
    local m = call('ZRANGE', wk, 0, 0)[1]
    if not m then
      break
    end
    local j = S.job(pmid(m))
    if not j or j.st ~= 'throttled' then
      call('ZREM', wk, m)
      zrem(P .. 's:throttled', m)
    else
      if j.gr == 1 and full(l) then
        break
      end
      local rated = l.rate > 0 and (j.gr == 0 or not open(l))
      local at = NOW
      if rated then
        at = slot(l)
      end
      if at > NOW then
        take(l, at)
        set(j, 'gr', 1)
        set(j, 'run', at)
        S.move(j, 'scheduled')
      else
        if full(l) then
          break
        end
        if rated then
          take(l, at)
        end
        if l.rate > 0 then
          admitted(l)
        end
        set(j, 'gr', 0)
        S.move(j, 'enqueued')
      end
      n = n + 1
    end
  end
  return n
end

function S.satisfy(c, d)
  d.r = true
  c.dd = true
  set(c, 'pend', c.pend - 1)
  if c.pend == 0 then
    S.wake(c)
  end
end

function S.follow(c, parent, st, how)
  if not c or c.st ~= 'awaiting' then
    return false
  end
  local d = S.dep(c, parent, false)
  if not d then
    return false
  end
  if has(d.on, st) then
    S.satisfy(c, d)
  elseif st ~= 'failed' then
    S.log(c, {st = 'deleted', reason = 'parent ' .. I(parent) .. ' ' .. how})
    S.move(c, 'deleted')
  else
    return false
  end
  return true
end

function S.cascade(p, st)
  if p.kids ~= 1 then
    return
  end
  for _, id in ipairs(call('SMEMBERS', P .. 'ch:' .. I(p.id))) do
    S.follow(S.job(tonumber(id)), p.id, st, st)
  end
end

function S.unblock(b)
  local key = P .. 'bd:' .. I(b.id)
  for _, id in ipairs(call('SMEMBERS', key)) do
    local c = S.job(tonumber(id))
    if c and c.st == 'awaiting' then
      local d = S.dep(c, b.id, true)
      if d then
        S.satisfy(c, d)
      end
    end
  end
  call('DEL', key)
end

function S.repair(c)
  local changed = false
  for _, d in ipairs(S.deps(c)) do
    if c.st ~= 'awaiting' then
      return true
    end
    if not d.r then
      if d.b then
        local b = S.batch(d.p)
        if not b or b.fin ~= 0 then
          S.satisfy(c, d)
          changed = true
        end
      else
        local p = S.job(d.p)
        if not p then
          changed = S.follow(c, d.p, 'deleted', 'pruned') or changed
        elseif FINAL[p.st] then
          changed = S.follow(c, p.id, p.st, p.st) or changed
        end
      end
    end
  end
  if c.st == 'awaiting' and c.pend == 0 then
    S.wake(c)
    return true
  end
  return changed
end

function S.idle(b)
  return b.seal == 1 and b.live == 0 and b.unf == 0 and b.fin == 0
end

function S.complete(b)
  if not b or not S.idle(b) then
    return
  end
  b.fin = NOW
  b.d = true
  call('ZREM', P .. 'bu', M(b.id))
  call('ZADD', P .. 'bf', I(b.id), M(b.id))
  done[#done + 1] = b
  local p = b.par ~= 0 and S.batch(b.par) or nil
  if p then
    p.unf = p.unf - 1
    p.d = true
    S.complete(p)
  end
end

function S.view(id, ov)
  if ov then
    local v = ov.batches[id]
    if v then
      return v
    end
  end
  local b = S.batch(id)
  if not b then
    return nil
  end
  local v = {sealed = b.seal == 1, finished = b.fin ~= 0, live = b.live, unf = b.unf, par = b.par}
  if ov then
    ov.batches[id] = v
  end
  return v
end

local function closed(v)
  return v.finished or v.sealed and v.live == 0 and v.unf == 0
end

function S.joinable(id, ov)
  local v = S.view(id, ov)
  if not v then
    fail('notfound', 'batch ' .. I(id))
  end
  if closed(v) then
    fail('closed', 'batch ' .. I(id))
  end
end

function S.newbatch(nb)
  local id = call('INCR', P .. 'bid')
  return {id = id, par = math.max(nb.parent or 0, 0), desc = nb.desc or '', meta = nb.meta, tot = 0, seal = 0,
    live = 0, nest = 0, unf = 0, cr = NOW, fin = 0, c = {}, d = true}
end

function S.addbatch(b)
  batches[b.id] = b
  call('ZADD', P .. 'bs', I(b.id), M(b.id))
  call('ZADD', P .. 'bu', I(b.id), M(b.id))
  local p = b.par ~= 0 and S.batch(b.par) or nil
  if p then
    p.nest = p.nest + 1
    p.unf = p.unf + 1
    p.d = true
    call('ZADD', P .. 'bn:' .. I(p.id), I(b.id), M(b.id))
  end
end

function S.seal(b)
  b.seal = 1
  b.d = true
  S.complete(b)
end

function S.settle()
  while true do
    if #resolved > 0 then
      local c = table.remove(resolved)
      S.cascade(c[1], c[2])
    elseif #done > 0 then
      S.unblock(table.remove(done))
    elseif #admit > 0 then
      local key = table.remove(admit)
      admitting[key] = nil
      S.admit(key, math.huge)
    else
      return
    end
  end
end

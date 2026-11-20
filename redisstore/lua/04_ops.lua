local LEX = {enqueued = true, throttled = true}
local DESC = {succeeded = true, failed = true, deleted = true, awaiting = true, processing = true}

function S.jobview(j)
  local par = {}
  for _, d in ipairs(S.deps(j)) do
    if not d.b then
      par[#par + 1] = I(d.p)
    end
  end
  return {
    id = I(j.id), claim = j.claim, k = j.k, q = j.q, args = j.args, meta = j.meta, tags = j.tags,
    title = j.title, pri = j.pri, att = j.att, max = j.max, tmo = j.tmo ~= '' and j.tmo or '0', run = I(j.run), cr = I(j.cr),
    at = I(j.at), b = I(j.b), rec = j.rec, par = L(par), lim = j.lim,
  }
end

function S.record(j, full)
  local r = S.jobview(j)
  r.st = j.st
  r.fin = I(j.fin)
  r.srv = j.srv
  r.cancel = j.cancel == 1 or nil
  r.ab = I(j.ab)
  r.pend = j.pend
  r.prog = j.prog
  if full then
    local id = I(j.id)
    r.hist = L(call('LRANGE', P .. 'h:' .. id, 0, -1))
    r.out = j.out
    r.ch = L(call('SMEMBERS', P .. 'ch:' .. id))
  end
  return r
end

local function scan(key, ckey, n)
  local cur = call('GET', ckey) or '0'
  local ms = call('ZRANGEBYSCORE', key, '(' .. cur, '+inf', 'LIMIT', 0, n)
  if #ms < n then
    call('DEL', ckey)
  else
    call('SET', ckey, I(tonumber(ms[#ms])))
  end
  return ms
end

local function scanlex(key, ckey, n)
  local cur = call('GET', ckey)
  local ms = call('ZRANGEBYLEX', key, cur and '(' .. cur or '-', '+', 'LIMIT', 0, n)
  if #ms < n then
    call('DEL', ckey)
  else
    call('SET', ckey, ms[#ms])
  end
  return ms
end

function H.insert(a)
  return S.insert(a.jobs or {})
end

function H.open_batch(a)
  if (a.parent or 0) > 0 then
    S.joinable(a.parent, nil)
  end
  local b = S.newbatch(a)
  S.addbatch(b)
  return I(b.id)
end

function H.seal_batch(a)
  local b = S.batch(a.id)
  if not b then
    fail('notfound', 'batch ' .. I(a.id))
  end
  S.seal(b)
  S.settle()
end

local function pick(queue, kinds, n)
  local key = P .. 'qk:' .. queue
  local lists = {}
  for _, k in ipairs(call('SMEMBERS', key)) do
    if not kinds or kinds[k] then
      local ms = call('ZRANGE', qkey(queue, k), 0, n - 1)
      if #ms == 0 then
        call('SREM', key, k)
      else
        lists[#lists + 1] = ms
      end
    end
  end
  if #lists < 2 then
    return lists[1] or {}
  end
  local all = {}
  for _, ms in ipairs(lists) do
    for _, m in ipairs(ms) do
      all[#all + 1] = m
    end
  end
  table.sort(all)
  for i = #all, n + 1, -1 do
    all[i] = nil
  end
  return all
end

function H.claim(a)
  local kinds = nil
  if a.kinds and #a.kinds > 0 then
    kinds = {}
    for _, k in ipairs(a.kinds) do
      kinds[k] = true
    end
  end
  local out = {}
  for _, name in ipairs(a.queues or {}) do
    if #out >= a.limit then
      break
    end
    if call('SISMEMBER', P .. 'paused', name) == 0 then
      for _, m in ipairs(pick(name, kinds, a.limit - #out)) do
        local j = S.job(pmid(m))
        if j and j.st == 'enqueued' then
          set(j, 'att', j.att + 1)
          set(j, 'claim', j.claim + 1)
          set(j, 'srv', a.server)
          set(j, 'at', NOW)
          set(j, 'cancel', 0)
          S.move(j, 'processing')
          out[#out + 1] = S.jobview(j)
        elseif not j then
          zrem(P .. 's:enqueued', m)
          for _, k in ipairs(call('SMEMBERS', P .. 'qk:' .. name)) do
            zrem(qkey(name, k), m)
          end
        end
      end
    end
  end
  if #out > 0 then
    call('SADD', P .. 'ps', a.server)
  end
  return L(out)
end

function S.finish(o)
  local j = S.job(o.id)
  if not j or j.st ~= 'processing' or j.claim ~= o.claim then
    return 2
  end
  local st = o.st
  if st ~= 'succeeded' and st ~= 'failed' and st ~= 'deleted' and st ~= 'scheduled' and st ~= 'enqueued' or o.bad then
    return 4
  end
  local to = st
  local delay = o.delay or 0
  if to == 'scheduled' and delay <= 0 then
    to = 'enqueued'
  end
  if to == 'enqueued' and j.lim ~= '' then
    to = 'throttled'
  end
  if j.cancel == 1 and st ~= 'succeeded' then
    to = 'deleted'
  end
  if o.reason and o.reason ~= '' then
    S.log(j, {st = to, reason = o.reason, err = o.err, trace = o.trace, srv = j.srv})
  end
  if o.refund then
    set(j, 'att', math.max(j.att - 1, 0))
  end
  if o.out and o.out ~= '' and ARCH[to] then
    set(j, 'out', o.out)
  end
  if to == 'scheduled' then
    set(j, 'run', NOW + delay)
    S.move(j, to)
  elseif to == 'enqueued' or to == 'throttled' then
    set(j, 'run', NOW)
    S.enqueue(j)
  else
    S.move(j, to)
  end
  if st == 'scheduled' and to ~= 'deleted' and not o.refund then
    S.bump('scheduled')
  end
  return 1
end

function H.finish(a)
  local res = {}
  for i, o in ipairs(a.outs or {}) do
    res[i] = S.finish(o)
  end
  S.settle()
  return L(res)
end

function H.heartbeat(a)
  local key = P .. 'sv:' .. a.id
  local started = a.started or 0
  if started == 0 then
    started = tonumber(call('HGET', key, 'started') or NOW)
  end
  call('HSET', key, 'info', a.info, 'started', I(started), 'hb', I(NOW))
  call('ZADD', P .. 'svh', I(NOW), a.id)
  local leases = {}
  for i, m in ipairs(call('ZRANGE', P .. 'p:' .. a.id, 0, -1)) do
    local id = tonumber(m)
    local v = call('HMGET', jkey(id), 'claim', 'cancel', 'at')
    leases[i] = {id = I(id), claim = tonumber(v[1] or 0), cancel = v[2] == '1' or nil, age = I(NOW - tonumber(v[3] or NOW))}
  end
  return {leases = L(leases), paused = L(call('SMEMBERS', P .. 'paused'))}
end

function H.unregister(a)
  call('DEL', P .. 'sv:' .. a.id)
  call('ZREM', P .. 'svh', a.id)
end

function H.set_meta(a)
  local j = S.job(a.id)
  if not j or j.st ~= 'processing' or j.claim ~= a.claim then
    fail('lost', '')
  end
  local cur = j.meta and cjson.decode(j.meta) or {}
  for k, v in pairs(cjson.decode(a.meta)) do
    cur[k] = v
  end
  set(j, 'meta', next(cur) and cjson.encode(cur) or nil)
end

function H.lead(a)
  local key = P .. 'ls:' .. a.name
  local v = call('HMGET', key, 'holder', 'exp', 'acq')
  if not v[1] or tonumber(v[2]) <= NOW then
    call('HSET', key, 'holder', a.holder, 'exp', I(NOW + a.ttl), 'acq', I(NOW))
    return {ok = true, held = '0'}
  end
  if v[1] == a.holder then
    call('HSET', key, 'exp', I(NOW + a.ttl))
    return {ok = true, held = I(NOW - tonumber(v[3]))}
  end
  return {ok = false, held = '0'}
end

function H.resign(a)
  local key = P .. 'ls:' .. a.name
  if call('HGET', key, 'holder') == a.holder then
    call('DEL', key)
  end
end

function H.promote(a)
  local n = 0
  local key = P .. 's:scheduled'
  for _, m in ipairs(call('ZRANGEBYSCORE', key, '-inf', I(NOW), 'LIMIT', 0, a.limit)) do
    local j = S.job(tonumber(m))
    if j and j.st == 'scheduled' then
      S.enqueue(j)
      n = n + 1
    else
      zrem(key, m)
    end
  end
  S.settle()
  S.flushz()
  local nxt = call('ZRANGE', key, 0, 0, 'WITHSCORES')
  return {n = n, queues = L(ready), next = nxt[2] and I(math.max(tonumber(nxt[2]) - NOW, 1)) or nil}
end

function H.orphans(a)
  local cutoff = NOW - a.dead
  local ms = {}
  for _, srv in ipairs(call('SMEMBERS', P .. 'ps')) do
    local hb = call('ZSCORE', P .. 'svh', srv)
    if call('EXISTS', P .. 'p:' .. srv) == 0 then
      call('SREM', P .. 'ps', srv)
    elseif not hb or tonumber(hb) < cutoff then
      for _, m in ipairs(call('ZRANGE', P .. 'p:' .. srv, 0, a.limit - 1)) do
        ms[#ms + 1] = m
      end
    end
  end
  table.sort(ms)
  local out = {}
  for i = 1, math.min(#ms, a.limit) do
    local id = tonumber(ms[i])
    local v = call('HMGET', jkey(id), 'claim', 'k', 'q', 'att', 'max', 'srv', 'cancel', 'lr')
    out[i] = {id = I(id), claim = tonumber(v[1] or 0), k = v[2] or '', q = v[3] or '', att = tonumber(v[4] or 0),
      max = tonumber(v[5] or 0), srv = v[6] or '', cancel = v[7] == '1' or nil, lr = v[8] or nil}
  end
  return L(out)
end

function H.sweep(a)
  local limit = a.limit
  local n = 0
  for _, m in ipairs(scan(P .. 's:awaiting', P .. 'c:aw', limit)) do
    local c = S.job(tonumber(m))
    if c and c.st == 'awaiting' and S.repair(c) then
      n = n + 1
    end
  end
  for _, m in ipairs(scan(P .. 'bu', P .. 'c:bu', limit)) do
    local b = S.batch(tonumber(m))
    if n < limit and b and S.idle(b) then
      S.complete(b)
      n = n + 1
    end
  end
  S.settle()
  for _, key in ipairs(scanlex(P .. 'lk', P .. 'c:lk', limit)) do
    if n < limit then
      local ak = P .. 'la:' .. key
      if call('SCARD', ak) <= 1000 then
        for _, id in ipairs(call('SMEMBERS', ak)) do
          local j = S.job(tonumber(id))
          if not j or not BUSY[j.st] or j.lim ~= key then
            call('SREM', ak, id)
            n = n + 1
          end
        end
      end
      n = n + S.admit(key, limit - n)
    end
  end
  S.settle()
  return n
end

function S.drop(j)
  S.unindex(j)
  local id = I(j.id)
  if j.lim ~= '' then
    local l = S.limit(j.lim)
    l.refs = l.refs - 1
    l.d = true
  end
  local ck = P .. 'ch:' .. id
  if j.kids == 1 then
    for _, c in ipairs(call('SMEMBERS', ck)) do
      S.follow(S.job(tonumber(c)), j.id, 'deleted', 'pruned')
    end
  end
  local ds = S.deps(j)
  for _, d in ipairs(ds) do
    if d.b then
      call('SREM', P .. 'bd:' .. I(d.p), id)
    else
      call('SREM', P .. 'ch:' .. I(d.p), id)
    end
  end
  call('DEL', jkey(j.id), ck, P .. 'h:' .. id, P .. 'lg:' .. id)
  jobs[j.id] = false
  if j.b ~= 0 then
    S.complete(S.batch(j.b))
  end
  return 1 + #ds
end

local function expired(list, st, keep, limit)
  if keep < 0 then
    return
  end
  local r = call('ZRANGEBYSCORE', P .. 's:' .. st, '-inf', '(' .. I(NOW - keep), 'LIMIT', 0, limit, 'WITHSCORES')
  for i = 1, #r, 2 do
    list[#list + 1] = fmt('%020.0f', tonumber(r[i + 1])) .. r[i]
  end
end

function H.prune(a)
  local limit = a.limit
  local n = 0
  local archived, failed = {}, {}
  expired(archived, 'succeeded', a.succ, limit)
  expired(archived, 'deleted', a.del, limit)
  expired(failed, 'failed', a.failed, limit)
  for _, list in ipairs({archived, failed}) do
    table.sort(list)
    for i = 1, math.min(#list, limit) do
      local j = S.job(tonumber(string.sub(list[i], 21)))
      if j then
        n = n + S.drop(j)
      end
    end
  end
  S.settle()
  for _, id in ipairs(call('ZRANGEBYSCORE', P .. 'svh', '-inf', '(' .. I(NOW - a.servers), 'LIMIT', 0, limit)) do
    call('DEL', P .. 'sv:' .. id)
    call('ZREM', P .. 'svh', id)
    n = n + 1
  end
  local oldest = I(floor((NOW - a.stats) / 1000000))
  for _, m in ipairs(call('ZRANGEBYSCORE', P .. 'stm', '-inf', '(' .. oldest, 'LIMIT', 0, limit)) do
    call('DEL', P .. 'st:' .. m)
    call('ZREM', P .. 'stm', m)
    n = n + 1
  end
  for _, key in ipairs(call('ZRANGEBYSCORE', P .. 'ux', '-inf', I(NOW), 'LIMIT', 0, limit)) do
    local u = S.rawu(key)
    if not u then
      call('ZREM', P .. 'ux', key)
    elseif u.dl ~= 0 and u.dl <= NOW or u.dl == 0 and u.exp ~= 0 and u.exp <= NOW then
      S.delu(key)
      n = n + 1
    else
      S.setu(key, u.job, u.exp, u.dl)
    end
  end
  for _, key in ipairs(scanlex(P .. 'lk', P .. 'c:plk', limit)) do
    local l = S.limit(key)
    if l.refs <= 0 and math.max(l.tat, l.atat) <= NOW then
      call('DEL', P .. 'l:' .. key, P .. 't:' .. key, P .. 'la:' .. key)
      call('ZREM', P .. 'lk', key)
      l.gone = true
      n = n + 1
    end
  end
  for _, m in ipairs(scan(P .. 'bf', P .. 'c:bf', limit)) do
    local b = S.batch(tonumber(m))
    if not b then
      call('ZREM', P .. 'bf', m)
    elseif b.fin ~= 0 and b.nest == 0 then
      local members = 0
      for _, c in pairs(b.c) do
        members = members + c
      end
      if members == 0 then
        local id = I(b.id)
        local p = b.par ~= 0 and S.batch(b.par) or nil
        if p then
          p.nest = p.nest - 1
          p.d = true
        end
        if b.par ~= 0 then
          call('ZREM', P .. 'bn:' .. I(b.par), m)
        end
        call('DEL', P .. 'b:' .. id, P .. 'bd:' .. id, P .. 'bn:' .. id)
        call('ZREM', P .. 'bs', m)
        call('ZREM', P .. 'bf', m)
        b.gone = true
        n = n + 1
      end
    end
  end
  return n
end

local function recindex(id, paused, nxt)
  if not paused and nxt ~= 0 then
    call('ZADD', P .. 'rd', I(nxt), id)
  else
    call('ZREM', P .. 'rd', id)
  end
end

local function recview(id)
  local v = call('HMGET', P .. 'r:' .. id, 'data', 'ver', 'next', 'last', 'lastjob', 'cr', 'up')
  if not v[1] then
    return nil
  end
  return {id = id, data = v[1], ver = v[2] or '0', next = v[3] or '0', last = v[4] or '0', lastjob = v[5] or '0',
    cr = v[6] or '0', up = v[7] or '0'}
end

function H.due(a)
  local out = {}
  for _, id in ipairs(call('ZRANGEBYSCORE', P .. 'rd', '-inf', I(NOW), 'LIMIT', 0, a.limit)) do
    out[#out + 1] = recview(id)
  end
  return {now = I(NOW), list = L(out)}
end

function H.fire(a)
  local key = P .. 'r:' .. a.id
  local cur = call('HMGET', key, 'ver', 'paused')
  if not cur[1] then
    fail('notfound', 'recurring "' .. a.id .. '"')
  end
  if tonumber(cur[1]) ~= a.ver then
    fail('conflict', 'recurring "' .. a.id .. '" is at version ' .. cur[1])
  end
  if a.bad then
    fail('bad', '')
  end
  local res = S.insert(a.jobs or {})
  call('HSET', key, 'next', I(a.next), 'last', I(a.last), 'ver', I(a.ver + 1), 'up', I(NOW))
  if res then
    call('HSET', key, 'lastjob', res[#res].id)
  end
  recindex(a.id, cur[2] == '1', a.next)
  return res
end

function H.recurring(a)
  local r = recview(a.id)
  if not r then
    fail('notfound', 'recurring "' .. a.id .. '"')
  end
  return r
end

function H.put_recurring(a)
  local key = P .. 'r:' .. a.id
  local cur = call('HMGET', key, 'ver', 'cr')
  local cr
  if not cur[1] and a.ver == 0 then
    cr = NOW
  elseif cur[1] and tonumber(cur[1]) == a.ver then
    cr = tonumber(cur[2])
  else
    fail('conflict', 'recurring "' .. a.id .. '" version ' .. I(a.ver))
  end
  call('HSET', key, 'data', a.data, 'paused', a.paused and '1' or '0', 'ver', I(a.ver + 1), 'next', I(a.next),
    'last', I(a.last), 'lastjob', I(a.lastjob), 'cr', I(cr), 'up', I(NOW))
  call('ZADD', P .. 'rs', 0, a.id)
  recindex(a.id, a.paused, a.next)
end

function H.remove_recurring(a)
  if call('DEL', P .. 'r:' .. a.id) == 0 then
    fail('notfound', 'recurring "' .. a.id .. '"')
  end
  call('ZREM', P .. 'rs', a.id)
  call('ZREM', P .. 'rd', a.id)
end

function H.recurrings()
  local out = {}
  for _, id in ipairs(call('ZRANGE', P .. 'rs', 0, -1)) do
    out[#out + 1] = recview(id)
  end
  return L(out)
end

local function matches(f, j)
  return (not f.st or j.st == f.st) and (not f.q or j.q == f.q) and (not f.k or j.k == f.k)
    and (not f.b or j.b == f.b) and (not f.rec or j.rec == f.rec)
end

local function candidates(f)
  if f.ids then
    return f.ids
  end
  local ids = {}
  for i, m in ipairs(call('ZRANGE', P .. 's:' .. f.st, f.off, f.off + f.n - 1)) do
    ids[i] = LEX[f.st] and pmid(m) or tonumber(m)
  end
  return ids
end

function H.delete(a)
  local ids = candidates(a)
  local n, left = 0, 0
  for _, id in ipairs(ids) do
    local j = S.job(id)
    if j and matches(a, j) then
      if j.st == 'processing' then
        if j.cancel == 0 then
          set(j, 'cancel', 1)
          events[#events + 1] = {3, j.q, I(j.id)}
          n = n + 1
        end
      elseif not ARCH[j.st] then
        S.log(j, {st = 'deleted', reason = 'deleted'})
        S.move(j, 'deleted')
        n = n + 1
        left = left + 1
      end
    end
  end
  S.settle()
  return {n = n, off = (a.off or 0) + #ids - left, more = not a.ids and #ids == a.n}
end

function H.requeue(a)
  local ids = candidates(a)
  local n = 0
  for _, id in ipairs(ids) do
    local j = S.job(id)
    local st = j and j.st
    if j and matches(a, j) and (st == 'failed' or st == 'scheduled' or st == 'succeeded' or st == 'deleted') then
      local free = true
      if j.uk ~= '' and j.ufor == 0 then
        local u = S.holder(j.uk)
        if u and u.job ~= j.id then
          free = false
        else
          S.setu(j.uk, j.id, 0, 0)
        end
      end
      if free then
        local to = j.lim ~= '' and 'throttled' or 'enqueued'
        if st ~= 'scheduled' then
          set(j, 'att', 0)
        end
        set(j, 'max', math.max(j.max, j.att + 1))
        set(j, 'cancel', 0)
        set(j, 'out', nil)
        set(j, 'run', NOW)
        S.log(j, {st = to, reason = 'requeued'})
        S.enqueue(j)
        set(j, 'gr', 0)
        n = n + 1
      end
    end
  end
  S.settle()
  return {n = n, off = (a.off or 0) + #ids - n, more = not a.ids and #ids == a.n}
end

function H.pause(a)
  if a.paused then
    call('SADD', P .. 'paused', a.name)
  else
    call('SREM', P .. 'paused', a.name)
  end
  call('SADD', P .. 'qrow', a.name)
  call('SADD', P .. 'qs', a.name)
  events[#events + 1] = {4, a.name, '0'}
end

function H.job(a)
  local j = S.job(a.id)
  if not j then
    fail('notfound', 'job ' .. I(a.id))
  end
  return S.record(j, true)
end

local function bound(key, cs, cm, strict)
  local lo = call('ZCOUNT', key, '-inf', '(' .. cs)
  local hi = call('ZCOUNT', key, '-inf', cs)
  while lo < hi do
    local mid = floor((lo + hi) / 2)
    local m = call('ZRANGE', key, mid, mid)[1]
    if m < cm or strict and m == cm then
      lo = mid + 1
    else
      hi = mid
    end
  end
  return lo
end

local function tagged(j, tag)
  if not j.tags then
    return false
  end
  for _, t in ipairs(cjson.decode(j.tags)) do
    if t == tag then
      return true
    end
  end
  return false
end

function H.jobs(a)
  local st = a.st
  local key = P .. 's:' .. st
  local lex, desc = LEX[st], DESC[st]
  local rank = 0
  if a.cur then
    if lex then
      if not string.match(a.cur, '^%d+$') then
        fail('invalid', 'cursor "' .. a.cur .. '"')
      end
      rank = call('ZLEXCOUNT', key, '-', '[' .. a.cur)
    else
      local cs, cm = string.match(a.cur, '^(%d+)%.(%d+)$')
      if not cs then
        fail('invalid', 'cursor "' .. a.cur .. '"')
      end
      if desc then
        rank = call('ZCARD', key) - bound(key, cs, cm, false)
      else
        rank = bound(key, cs, cm, true)
      end
    end
  end
  local filtered = a.q or a.k or a.b or a.tag
  local step = filtered and 200 or a.limit + 1
  local budget = filtered and 5000 or a.limit + 1
  local found, seen, last, ended = {}, 0, nil, false
  while #found <= a.limit and seen < budget and not ended do
    local ms
    if lex then
      ms = call('ZRANGE', key, rank, rank + step - 1)
    elseif desc then
      ms = call('ZREVRANGE', key, rank, rank + step - 1, 'WITHSCORES')
    else
      ms = call('ZRANGE', key, rank, rank + step - 1, 'WITHSCORES')
    end
    local width = lex and 1 or 2
    ended = #ms < step * width
    for i = 1, #ms, width do
      if #found <= a.limit then
        local m = ms[i]
        rank = rank + 1
        seen = seen + 1
        last = lex and m or I(tonumber(ms[i + 1])) .. '.' .. m
        local j = S.job(lex and pmid(m) or tonumber(m))
        if j and j.st == st and (not a.q or j.q == a.q) and (not a.k or j.k == a.k) and (not a.b or j.b == a.b)
          and (not a.tag or tagged(j, a.tag)) then
          found[#found + 1] = {j, last}
        end
      else
        ended = false
      end
    end
  end
  local recs = {}
  for i = 1, math.min(#found, a.limit) do
    recs[i] = S.record(found[i][1], false)
  end
  local nxt = nil
  if #found > a.limit then
    nxt = found[a.limit][2]
  elseif not ended then
    nxt = last
  end
  return {recs = L(recs), next = nxt}
end

function H.counts()
  local c = {}
  for _, st in ipairs(STATES) do
    c[st] = call('ZCARD', P .. 's:' .. st)
  end
  local tot = call('HMGET', P .. 'tot', 'succeeded', 'deleted')
  return {c = c, retries = tonumber(call('GET', P .. 'retries') or 0), succ = tot[1] or '0', del = tot[2] or '0'}
end

function H.series(a)
  local out = {}
  for _, m in ipairs(call('ZRANGEBYSCORE', P .. 'stm', I(a.lo), '(' .. fmt('%.6f', a.to / 1000000))) do
    local v = call('HMGET', P .. 'st:' .. m, 'succeeded', 'failed', 'deleted', 'retried')
    out[#out + 1] = {at = m, s = v[1] or '0', f = v[2] or '0', d = v[3] or '0', r = v[4] or '0'}
  end
  return L(out)
end

function H.servers()
  local out = {}
  for _, id in ipairs(call('ZRANGE', P .. 'svh', 0, -1)) do
    local v = call('HMGET', P .. 'sv:' .. id, 'info', 'started', 'hb')
    if v[1] then
      out[#out + 1] = {id = id, info = v[1], started = v[2], hb = v[3]}
    end
  end
  return L(out)
end

function H.queues()
  local out = {}
  for _, name in ipairs(call('SMEMBERS', P .. 'qs')) do
    local c = {}
    local h = call('HGETALL', P .. 'qc:' .. name)
    for i = 1, #h, 2 do
      c[h[i]] = h[i + 1]
    end
    local first = call('ZRANGE', P .. 'qr:' .. name, 0, 0, 'WITHSCORES')
    out[#out + 1] = {
      name = name, c = L(c), paused = call('SISMEMBER', P .. 'paused', name) == 1 or nil,
      row = call('SISMEMBER', P .. 'qrow', name) == 1 or nil,
      lat = first[2] and I(math.max(NOW - tonumber(first[2]), 0)) or nil,
    }
  end
  return L(out)
end

local function binfo(b)
  local c = {}
  for st, n in pairs(b.c) do
    if n > 0 then
      c[st] = I(n)
    end
  end
  return {id = I(b.id), desc = b.desc, meta = b.meta, tot = I(b.tot), seal = b.seal == 1 or nil, c = L(c),
    cr = I(b.cr), fin = I(b.fin), par = I(b.par), nest = I(b.nest), nestfin = I(b.nest - b.unf)}
end

function H.batch(a)
  local b = S.batch(a.id)
  if not b then
    fail('notfound', 'batch ' .. I(a.id))
  end
  return binfo(b)
end

function H.batches(a)
  local key = (a.parent or 0) > 0 and P .. 'bn:' .. I(a.parent) or P .. 'bs'
  local ms = call('ZREVRANGEBYSCORE', key, a.after and '(' .. I(a.after) or '+inf', '-inf', 'LIMIT', 0, a.limit + 1)
  local out = {}
  for i = 1, math.min(#ms, a.limit) do
    local b = S.batch(tonumber(ms[i]))
    if b then
      out[#out + 1] = binfo(b)
    end
  end
  return {list = L(out), next = #ms > a.limit and I(tonumber(ms[a.limit])) or nil}
end

function H.limits(a)
  local out = {}
  local min = a.after and a.after ~= '' and '(' .. a.after or '-'
  for _, key in ipairs(call('ZRANGEBYLEX', P .. 'lk', min, '+', 'LIMIT', 0, a.limit)) do
    local l = S.limit(key)
    local nxt = l.rate > 0 and S.slot(l) or 0
    out[#out + 1] = {key = key, max = l.max, rate = l.rate, per = I(l.per), burst = l.burst,
      active = call('SCARD', P .. 'la:' .. key), thr = call('ZCARD', P .. 't:' .. key), rsv = l.rsv,
      next = nxt > NOW and I(nxt) or nil}
  end
  return L(out)
end

function H.write_console(a)
  local j = S.job(a.id)
  if not j or j.st ~= 'processing' or j.claim ~= a.claim then
    fail('lost', '')
  end
  local key = P .. 'lg:' .. I(j.id)
  for _, text in ipairs(a.lines or {}) do
    set(j, 'lseq', j.lseq + 1)
    call('RPUSH', key, cjson.encode({seq = j.lseq, att = j.att, at = I(NOW), text = text}))
  end
  if a.prog >= 0 then
    set(j, 'prog', a.prog)
  end
end

function H.logs(a)
  if call('EXISTS', jkey(a.id)) == 0 then
    return nil
  end
  local start = math.max(a.after, 0)
  return L(call('LRANGE', P .. 'lg:' .. I(a.id), start, start + a.limit - 1))
end

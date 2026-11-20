local function norm(ps)
  ps.meta = ps.meta ~= '' and ps.meta or nil
  ps.tags = ps.tags ~= '' and ps.tags or nil
  ps.title = ps.title or ''
  ps.pri = ps.pri or 0
  ps.delay = ps.delay or 0
  ps.uk = ps.uk or ''
  ps.ufor = ps.ufor or 0
  ps.udeb = ps.udeb or 0
  ps.lk = ps.lk or ''
  ps.lmax = ps.lmax or 0
  ps.lrate = ps.lrate or 0
  ps.lper = ps.lper or 0
  ps.lburst = ps.lburst or 0
  ps.b = ps.b or 0
  ps.ab = ps.ab or 0
  ps.par = ps.par or {}
  ps.rec = ps.rec or ''
end

function S.runat(ps)
  if ps.udeb > 0 then
    return NOW + ps.udeb
  end
  if ps.hr then
    return ps.run
  end
  return NOW + math.max(ps.delay, 0)
end

function S.replaces(j, ps)
  if ps.udeb > 0 then
    return j.st == 'scheduled' and j.gr == 0
  end
  return ps.urep == true and (j.st == 'awaiting' or j.st == 'scheduled' or j.st == 'throttled' or j.st == 'enqueued')
end

function S.replaced(j, at)
  if j.gr == 1 or j.st == 'enqueued' and j.lim ~= '' then
    return j.st, j.run
  end
  if j.st == 'awaiting' or j.st == 'scheduled' then
    return j.st, at
  end
  if at > NOW then
    return 'scheduled', at
  end
  return j.st, j.run
end

function S.replace(j, ps)
  local st, at = S.replaced(j, S.runat(ps))
  S.unindex(j)
  set(j, 'args', ps.args)
  set(j, 'meta', ps.meta)
  set(j, 'tags', ps.tags)
  set(j, 'title', ps.title)
  set(j, 'pri', ps.pri)
  set(j, 'st', st)
  set(j, 'run', at)
  S.index(j)
end

function S.state(id, ov)
  if ov then
    local st = ov.jobs[id]
    if st then
      return st
    end
  end
  local j = S.job(id)
  return j and j.st or nil
end

local function target(p, k)
  local a = p.items[k].alias
  if a > 0 then
    k = a
  end
  if p.res[k].dup then
    return 0
  end
  return k
end

local function sort(p)
  local marks = {}
  for i = 1, #p.items do
    if not p.res[i].dup and not marks[i] then
      local stack = {{i, 0}}
      marks[i] = 1
      while #stack > 0 do
        local top = stack[#stack]
        local pars = p.items[top[1]].p.par
        local pushed = false
        while top[2] < #pars and not pushed do
          top[2] = top[2] + 1
          local pr = pars[top[2]]
          if (pr.id or 0) == 0 then
            local k = target(p, pr.ix + 1)
            if k > 0 and not marks[k] then
              marks[k] = 1
              stack[#stack + 1] = {k, 0}
              pushed = true
            end
          end
        end
        if not pushed then
          marks[top[1]] = 2
          p.order[#p.order + 1] = top[1]
          stack[#stack] = nil
        end
      end
    end
  end
end

function S.prepare(list, ov)
  local p = {items = {}, res = {}, order = {}}
  local seen = {}
  local fresh = 0
  for i, ps in ipairs(list) do
    norm(ps)
    p.items[i] = {p = ps, alias = 0, holder = 0}
    p.res[i] = {id = 0, st = '', dup = false, rep = false}
    if ps.uk ~= '' then
      local first = seen[ps.uk]
      if first then
        p.items[i].alias = first
        p.res[i].dup = true
      else
        seen[ps.uk] = i
        local u = S.holder(ps.uk)
        if not u and ov and ov.keys[ps.uk] then
          u = {job = ov.keys[ps.uk]}
        end
        if u then
          p.res[i] = {id = u.job, st = S.state(u.job, ov) or '', dup = true, rep = false}
          local j = S.job(u.job)
          if j and S.replaces(j, ps) then
            p.items[i].holder = j.id
            p.res[i].st = S.replaced(j, S.runat(ps))
            p.res[i].rep = true
          end
        end
      end
    end
    if not p.res[i].dup then
      fresh = fresh + 1
    end
  end
  sort(p)
  local id = fresh > 0 and call('INCRBY', P .. 'id', fresh) - fresh or 0
  for i = 1, #p.items do
    local a = p.items[i].alias
    if a > 0 then
      p.res[i].id = p.res[a].id
    elseif not p.res[i].dup then
      id = id + 1
      p.res[i].id = id
    end
  end
  return p
end

local function parent(p, pr, ov)
  local id = pr.id or 0
  if id == 0 then
    local k = pr.ix + 1
    local a = p.items[k].alias
    if a > 0 then
      k = a
    end
    local r = p.res[k]
    if not r.dup then
      return r.id, r.st
    end
    id = r.id
  end
  local st = S.state(id, ov)
  if not st then
    fail('notfound', 'parent ' .. I(id))
  end
  return id, st
end

local function initial(it)
  if it.doom ~= '' then
    return 'deleted'
  end
  if it.pending > 0 then
    return 'awaiting'
  end
  if S.runat(it.p) > NOW then
    return 'scheduled'
  end
  if it.p.lk ~= '' then
    return 'throttled'
  end
  return 'enqueued'
end

function S.resolve(p, ov)
  for _, i in ipairs(p.order) do
    local it = p.items[i]
    local ps = it.p
    it.deps, it.pending, it.doom = {}, 0, ''
    for _, pr in ipairs(ps.par) do
      local id, st = parent(p, pr, ov)
      local known = false
      for _, d in ipairs(it.deps) do
        known = known or d.p == id
      end
      if not known then
        local d = {p = id, on = pr.on, b = false, r = has(pr.on, st)}
        if not d.r then
          it.pending = it.pending + 1
          if it.doom == '' and ARCH[st] then
            it.doom = 'parent ' .. I(id) .. ' ' .. st
          end
        end
        it.deps[#it.deps + 1] = d
      end
    end
    if ps.ab ~= 0 then
      local v = S.view(ps.ab, ov)
      if not v then
        fail('notfound', 'batch ' .. I(ps.ab))
      end
      if not v.finished then
        it.pending = it.pending + 1
      end
      it.deps[#it.deps + 1] = {p = ps.ab, on = 0, b = true, r = v.finished}
    end
    if ps.b ~= 0 then
      S.joinable(ps.b, ov)
    end
    p.res[i].st = initial(it)
  end
end

function S.apply(p, admits)
  for _, i in ipairs(p.order) do
    local it = p.items[i]
    local ps = it.p
    local j = setmetatable({
      id = p.res[i].id, st = p.res[i].st, q = ps.q, k = ps.k, args = ps.args, meta = ps.meta, tags = ps.tags,
      title = ps.title, pri = ps.pri, att = 0, max = ps.max, claim = 0, tmo = ps.tmo or '0', run = S.runat(ps),
      cr = NOW, at = 0, fin = 0, srv = '', cancel = 0, b = ps.b, ab = ps.ab, rec = ps.rec, uk = ps.uk,
      ufor = ps.ufor, lim = ps.lk, gr = 0, pend = it.pending, prog = 0, lseq = 0, lr = '',
      ds = it.deps, deps = depstr(it.deps), new = true, d = {},
    }, JOB)
    jobs[j.id] = j
    for _, d in ipairs(it.deps) do
      if not d.b then
        call('SADD', P .. 'ch:' .. I(d.p), I(j.id))
        local pj = jobs[d.p]
        if pj then
          set(pj, 'kids', 1)
        else
          call('HSET', jkey(d.p), 'kids', '1')
        end
      elseif not d.r then
        call('SADD', P .. 'bd:' .. I(d.p), I(j.id))
      end
    end
    if j.uk ~= '' then
      S.hold(j)
    end
    if j.lim ~= '' then
      local l = S.limit(j.lim)
      l.refs = l.refs + 1
      l.d = true
      if l.max ~= ps.lmax or l.rate ~= ps.lrate or l.per ~= ps.lper or l.burst ~= ps.lburst then
        l.max, l.rate, l.per, l.burst = ps.lmax, ps.lrate, ps.lper, ps.lburst
        if admits then
          S.admitting(j.lim)
        end
      end
    end
    if j.b ~= 0 then
      local b = S.batch(j.b)
      b.tot = b.tot + 1
      b.d = true
    end
    if j.st == 'deleted' then
      j.fin = NOW
      S.log(j, {st = 'deleted', reason = it.doom})
    end
    S.index(j)
    if j.st == 'throttled' and admits then
      S.admitting(j.lim)
    end
  end
  for i, it in ipairs(p.items) do
    if it.holder ~= 0 then
      local j = S.job(it.holder)
      if j and S.replaces(j, it.p) then
        S.replace(j, it.p)
        p.res[i].st = j.st
      end
    end
  end
  call('SADD', P .. 'qs', unpack(S.names(p)))
end

function S.names(p)
  local seen, out = {}, {}
  for _, it in ipairs(p.items) do
    if not seen[it.p.q] then
      seen[it.p.q] = true
      out[#out + 1] = it.p.q
    end
  end
  return out
end

function S.results(p)
  local out = {}
  for i, r in ipairs(p.res) do
    out[i] = {id = I(r.id), st = r.st, dup = r.dup or nil, rep = r.rep or nil}
  end
  return L(out)
end

function S.insert(list)
  if #list == 0 then
    return nil
  end
  local p = S.prepare(list, nil)
  S.resolve(p, nil)
  S.apply(p, true)
  S.settle()
  for i, r in ipairs(p.res) do
    local a = p.items[i].alias
    if a > 0 then
      r.st = p.res[a].st
    elseif not r.dup then
      r.st = jobs[r.id].st
    end
  end
  return S.results(p)
end

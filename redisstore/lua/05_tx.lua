local function overlay()
  return {jobs = {}, batches = {}}
end

local function finished(v, ov)
  if not v.sealed or v.finished or v.live > 0 or v.unf > 0 then
    return
  end
  v.finished = true
  local p = v.par ~= 0 and S.view(v.par, ov) or nil
  if p then
    p.unf = p.unf - 1
    finished(p, ov)
  end
end

local function replan(op)
  local pl = op.plan
  local p = {items = {}, res = {}, order = {}}
  for i, ps in ipairs(op.jobs) do
    norm(ps)
    p.items[i] = {p = ps, alias = pl.alias[i], holder = tonumber(pl.holder[i])}
    p.res[i] = {id = tonumber(pl.ids[i]), st = pl.st[i], dup = pl.dup[i], rep = pl.rep[i]}
  end
  for _, i in ipairs(pl.order) do
    local ps, r = p.items[i].p, p.res[i]
    local u = ps.uk ~= '' and S.holder(ps.uk) or nil
    if u and u.job ~= r.id then
      r.id, r.st, r.dup = u.job, S.state(u.job, nil) or '', true
    else
      p.order[#p.order + 1] = i
    end
  end
  for i, it in ipairs(p.items) do
    if it.alias > 0 then
      p.res[i].id = p.res[it.alias].id
    end
  end
  return p
end

local function check(op, ov)
  if op.open then
    local o = op.open
    local par = o.parent or 0
    if par > 0 then
      S.joinable(par, ov)
    end
    ov.batches[o.id] = {sealed = false, finished = false, live = 0, unf = 0, par = par}
    local pv = par > 0 and S.view(par, ov) or nil
    if pv then
      pv.unf = pv.unf + 1
    end
    return nil
  end
  if op.seal then
    local v = S.view(op.seal, ov)
    if not v then
      fail('notfound', 'batch ' .. I(op.seal))
    end
    v.sealed = true
    finished(v, ov)
    return nil
  end
  local p = replan(op)
  S.resolve(p, ov)
  for _, i in ipairs(p.order) do
    local r, b = p.res[i], p.items[i].p.b
    ov.jobs[r.id] = r.st
    if b ~= 0 and not ARCH[r.st] then
      local v = S.view(b, ov)
      v.live = v.live + 1
    end
  end
  return p
end

local function release(ops)
  for _, op in ipairs(ops or {}) do
    if op.plan then
      for i, ps in ipairs(op.jobs) do
        local uk = ps.uk or ''
        if uk ~= '' and not op.plan.dup[i] then
          local u = S.rawu(uk)
          if u and u.dl ~= 0 and u.job == tonumber(op.plan.ids[i]) then
            S.delu(uk)
          end
        end
      end
    end
  end
end

function H.stage(a)
  local ov = overlay()
  for _, op in ipairs(a.ops or {}) do
    check(op, ov)
  end
  if a.open then
    if (a.open.parent or 0) > 0 then
      S.joinable(a.open.parent, ov)
    end
    return {id = I(call('INCR', P .. 'bid')), cr = I(NOW)}
  end
  if a.seal then
    if not S.view(a.seal, ov) then
      fail('notfound', 'batch ' .. I(a.seal))
    end
    return nil
  end
  local p = S.prepare(a.jobs, ov)
  S.resolve(p, ov)
  local pl = {ids = {}, st = {}, dup = {}, rep = {}, alias = {}, holder = {}, order = p.order}
  for i, r in ipairs(p.res) do
    local it = p.items[i]
    local ps = it.p
    if not r.dup and ps.uk ~= '' then
      S.setu(ps.uk, r.id, ps.ufor > 0 and NOW + ps.ufor or 0, NOW + a.ttl)
    end
    if it.alias > 0 then
      r.st = p.res[it.alias].st
    end
    pl.ids[i], pl.st[i], pl.dup[i], pl.rep[i] = I(r.id), r.st, r.dup, r.rep
    pl.alias[i], pl.holder[i] = it.alias, I(it.holder)
  end
  return {plan = pl, res = S.results(p)}
end

function H.commit(a)
  if a.key and call('EXISTS', P .. 'ap:' .. a.key) == 1 then
    return nil
  end
  local ov = overlay()
  local plans = {}
  local ok, err = pcall(function()
    for i, op in ipairs(a.ops or {}) do
      plans[i] = check(op, ov) or false
    end
  end)
  if not ok then
    release(a.ops)
    error(err, 0)
  end
  for i, op in ipairs(a.ops or {}) do
    if op.open then
      local o = op.open
      S.addbatch({id = o.id, par = o.parent or 0, desc = o.desc or '', meta = o.meta, tot = 0, seal = 0, live = 0,
        nest = 0, unf = 0, cr = o.cr, fin = 0, c = {}, d = true})
    elseif op.seal then
      S.seal(S.batch(op.seal))
    else
      S.apply(plans[i], true)
    end
  end
  S.settle()
  if a.key then
    call('SET', P .. 'ap:' .. a.key, '1', 'PX', a.keep)
  end
end

function H.rollback(a)
  release(a.ops)
end

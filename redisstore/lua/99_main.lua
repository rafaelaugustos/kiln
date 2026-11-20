local function run(_, args)
  reset(args)
  local handler = H[OP]
  if not handler then
    return redis.error_reply('kiln: unknown operation ' .. OP)
  end
  local ok, res = pcall(handler, A)
  if not ok then
    if type(res) == 'table' and res.e then
      return cjson.encode({e = res.e, m = res.m})
    end
    error(res, 0)
  end
  S.flush()
  S.publish()
  return cjson.encode({r = res})
end

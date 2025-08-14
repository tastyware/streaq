local stream_key = KEYS[1]
local group_name = KEYS[2]

for i=1, #ARGV do
  local stream = stream_key .. ARGV[i]
  -- create group if it doesn't exist
  local ok, groups = pcall(redis.call, 'xinfo', 'groups', stream)
  if not ok or #groups == 0 then
    redis.call('xgroup', 'create', stream, group_name, '0', 'mkstream')
  end
end

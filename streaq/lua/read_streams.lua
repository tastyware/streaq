local stream_key = KEYS[1]
local group_name = KEYS[2]
local consumer_name = KEYS[3]

local count = tonumber(ARGV[1])
local idle = ARGV[2]

local entries = {}

-- additional arguments are the names of custom priorities
for i = 3, #ARGV do
  local stream = stream_key .. ARGV[i]
  local entry_table = {}
  -- first, check for idle messages to reclaim
  local reclaimed = redis.call('xautoclaim', stream, group_name, consumer_name, idle, '0-0', 'count', count)[2]
  -- output format should match XREADGROUP
  if #reclaimed > 0 then
    for j=1, #reclaimed do entry_table[j] = reclaimed[j] end
    count = count - #reclaimed
  end
  -- next, check for new messages
  if count > 0 then
    local res = redis.call('xreadgroup', 'group', group_name, consumer_name, 'count', count, 'streams', stream, '>')
    local read = res and res[1][2]
    if read then
      -- this is the table we just created
      local len = #entry_table
      for j = 1, #read do entry_table[len + j] = read[j] end
      count = count - #read
    end
  end

  if #entry_table > 0 then
    table.insert(entries, {stream, entry_table})
  end
  if count <= 0 then break end
end

return entries

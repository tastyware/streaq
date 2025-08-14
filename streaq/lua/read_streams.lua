local stream_key = KEYS[1]
local group_name = KEYS[2]
local consumer_name = KEYS[3]

local count = ARGV[1]
local idle = ARGV[2]

local entries = {}

-- additional arguments are the names of custom priorities
for i = 3, #ARGV do
  local stream = stream_key .. ARGV[i]
  -- first, check for idle messages to reclaim
  local reclaimed = redis.call("xautoclaim", stream, group_name, consumer_name, idle, "0-0", "count", count)[2]
  -- output format should match XREADGROUP
  entries[i - 2] = { stream, reclaimed }
  if #reclaimed > 0 then
    count = count - #reclaimed
    if count <= 0 then break end
  end
  -- next, check for new messages
  local res = redis.call("xreadgroup", "group", group_name, consumer_name, "count", count, "streams", stream, ">")
  local read = res and res[1][2]
  if read then
    -- this is the table we just created
    local entry_table = entries[i - 2][2]
    local len = #entry_table
    for j = 1, #read do entry_table[len + j] = read[j] end
    count = count - #read
    if count <= 0 then break end
  end
end

return entries

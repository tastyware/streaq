local queue_key = KEYS[1]
local stream_key = KEYS[2]

local current_time = ARGV[1]

for i=2, #ARGV do
  local priority = ARGV[i]
  local queue = queue_key .. priority
  -- get and delete tasks ready to run from delayed queue
  local tids = redis.call('zrange', queue, 0, current_time, 'byscore')
  if #tids > 0 then
    redis.call('zremrangebyscore', queue, 0, current_time)

    local stream = stream_key .. priority
    -- add ready tasks to live queue
    for j=1, #tids do
      redis.call('xadd', stream, '*', 'task_id', tids[j])
    end
  end
end

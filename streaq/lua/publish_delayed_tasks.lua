local queue_key = KEYS[1]
local stream_key = KEYS[2]

local current_time = ARGV[1]

for i=2, #ARGV do
  local priority = ARGV[i]
  local queue = queue_key .. priority
  -- get and delete tasks ready to run from delayed queue
  local tids = redis.call('zrange', queue, 0, current_time, 'byscore')
  redis.call('zremrangebyscore', queue, 0, current_time)

  -- add ready tasks to live queue
  for _, task_id in ipairs(tids) do
    redis.call('xadd', stream_key .. priority, '*', 'task_id', task_id)
  end
end

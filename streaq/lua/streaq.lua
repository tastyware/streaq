#!lua name=streaq

redis.register_function('create_groups', function(keys, argv)
  local stream_key = keys[1]
  local group_name = argv[1]

  for i = 2, #argv do
    local stream = stream_key .. argv[i]
    -- create group if it doesn't exist
    local ok, groups = pcall(redis.call, 'xinfo', 'groups', stream)
    if not ok or #groups == 0 then
      redis.call('xgroup', 'create', stream, group_name, '0', 'mkstream')
    end
  end
end)

redis.register_function('publish_delayed_tasks', function(keys, argv)
  local queue_key = keys[1]
  local stream_key = keys[2]

  local current_time = argv[1]

  -- get and delete tasks ready to run from delayed queue (with scores)
  local tids = redis.call('zrange', queue_key, 0, current_time, 'byscore', 'withscores')
  if #tids > 0 then
    redis.call('zremrangebyscore', queue_key, 0, current_time)
    -- add ready tasks to live queue, using scheduled fire time as enqueue_time
    for j = 1, #tids, 2 do
      redis.call('xadd', stream_key, '*', 'task_id', tids[j], 'enqueue_time', tids[j + 1])
    end
  end
end)

redis.register_function('publish_task', function(keys, argv)
  local stream_key = keys[1]
  local queue_key = keys[2]
  local task_key = keys[3]
  local results_key = keys[4]

  local task_id = argv[1]
  local task_data = argv[2]
  local score = argv[3]
  local expire = argv[4]
  local current_time = argv[5]

  -- idempotency if result exists
  if redis.call('exists', results_key) == 1 then return end
  -- idempotency if payload exists
  local res
  if expire ~= '0' then
    res = redis.call('set', task_key, task_data, 'nx', 'px', expire)
  else
    res = redis.call('set', task_key, task_data, 'nx')
  end
  if not res then return end
  -- delayed queue
  if score ~= '0' then
    redis.call('zadd', queue_key, score, task_id)
  -- live queue
  else
    redis.call('xadd', stream_key, '*', 'task_id', task_id, 'enqueue_time', current_time)
  end
end)

redis.register_function('read_streams', function(keys, argv)
  local stream_key = keys[1]

  local group_name = argv[1]
  local consumer_name = argv[2]
  local count = tonumber(argv[3])
  local idle = argv[4]

  local entries = {}

  -- additional arguments are the names of custom priorities
  for i = 5, #argv do
    local stream = stream_key .. argv[i]
    local entry_table = {}
    -- first, check for idle messages to reclaim
    local reclaimed = redis.call('xautoclaim', stream, group_name, consumer_name, idle, '0-0', 'count', count)[2]
    -- output format should match XREADGROUP
    if #reclaimed > 0 then
      for j = 1, #reclaimed do entry_table[j] = reclaimed[j] end
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
      table.insert(entries, { stream, entry_table })
    end
    if count <= 0 then break end
  end

  return entries
end)

redis.register_function('refresh_timeout', function(keys, argv)
  local stream_key = keys[1]
  local running_set = keys[2]

  local group_name = argv[1]
  local consumer = argv[2]
  local message_id = argv[3]
  local task_id = argv[4]

  if #redis.call('xpending', stream_key, group_name, message_id, message_id, 1, consumer) > 0 then
    redis.call('xclaim', stream_key, group_name, consumer, 0, message_id, 'justid')
    redis.call('sadd', running_set, task_id)
    return 1
  end
  return 0
end)

redis.register_function('schedule_cron_job', function(keys, argv)
  local cron_key = keys[1]
  local queue_key = keys[2]
  local data_key = keys[3]
  local task_key = keys[4]

  local task_id = argv[1]
  local score = argv[2]
  local member = argv[3]
  local expire = argv[4]

  -- check if another worker already handled this
  if redis.call('zadd', cron_key, 'gt', 'ch', score, member) ~= 0 then
    redis.call('zadd', queue_key, score, task_id)
    redis.call('copy', data_key, task_key)
    if expire ~= '0' then
      redis.call('pexpire', task_key, expire)
    end
  end
end)

redis.register_function('requeue_task', function(keys, argv)
  local dead_key = keys[1]
  local stream_key = keys[2]
  local task_key = keys[3]
  local previous_key = keys[4]
  local results_key = keys[5]
  local retry_key = keys[6]
  local results_set = keys[7]

  local task_id = argv[1]
  local current_time = argv[2]

  if redis.call('zrem', dead_key, task_id) == 0 then return 0 end
  if redis.call('exists', task_key) == 0 then return 0 end
  redis.call('del', results_key, retry_key)
  redis.call('zrem', results_set, task_id)
  -- don't let the payload expire while it waits in the queue
  redis.call('persist', task_key)
  redis.call('persist', previous_key)
  redis.call('xadd', stream_key, '*', 'task_id', task_id, 'enqueue_time', current_time)
  return 1
end)

redis.register_function('purge_task', function(keys, argv)
  local dead_key = keys[1]
  local stream_key = keys[2]
  local depends_set = keys[3]

  local task_id = argv[1]
  local child_id = argv[2]
  local current_time = argv[3]

  if redis.call('zrem', dead_key, task_id) == 0 then return 0 end
  -- a parked task holds back its child, so fail it now
  if redis.call('srem', depends_set, child_id) == 1 then
    redis.call('xadd', stream_key, '*', 'task_id', child_id, 'enqueue_time', current_time, 'success', 0)
  end
  return 1
end)

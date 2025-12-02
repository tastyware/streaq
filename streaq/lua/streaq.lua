#!lua name=streaq

redis.register_function('create_groups', function(keys, argv)
  local stream_key = keys[1]
  local group_name = keys[2]

  for i=1, #argv do
    local stream = stream_key .. argv[i]
    -- create group if it doesn't exist
    local ok, groups = pcall(redis.call, 'xinfo', 'groups', stream)
    if not ok or #groups == 0 then
      redis.call('xgroup', 'create', stream, group_name, '0', 'mkstream')
    end
  end
end)

redis.register_function('fail_dependents', function(keys, argv)
  local dependents_key = keys[1]
  local dependencies_key = keys[2]
  local task_id = keys[3]

  local visited = {}
  local failed = {}
  local stack = { task_id }

  -- iterative DFS to traverse DAG
  while #stack > 0 do
    -- pop off last element
    local tid = stack[#stack]
    stack[#stack] = nil
    if not visited[tid] then
      visited[tid] = true
      -- push dependents onto the stack
      local deps = redis.call('smembers', dependents_key .. tid)
      for _, dep_id in ipairs(deps) do
        stack[#stack + 1] = dep_id
        redis.call('srem', dependencies_key .. dep_id, tid)
      end
      -- remove dependents set
      redis.call('del', dependents_key .. tid)
      -- add to failed list
      if tid ~= task_id then
        failed[#failed + 1] = tid
      end
    end
  end

  return failed
end)

redis.register_function('publish_delayed_tasks', function(keys, argv)
  local queue_key = keys[1]
  local stream_key = keys[2]

  local current_time = argv[1]

  for i=2, #argv do
    local priority = argv[i]
    local queue = queue_key .. priority
    -- get and delete tasks ready to run from delayed queue
    local tids = redis.call('zrange', queue, 0, current_time, 'byscore')
    if #tids > 0 then
      redis.call('zremrangebyscore', queue, 0, current_time)

      local stream = stream_key .. priority
      -- add ready tasks to live queue
      for j=1, #tids do
        redis.call('xadd', stream, '*', 'task_id', tids[j], 'enqueue_time', current_time)
      end
    end
  end
end)

redis.register_function('publish_task', function(keys, argv)
  local stream_key = keys[1]
  local queue_key = keys[2]
  local task_key = keys[3]
  local dependents_key = keys[4]
  local dependencies_key = keys[5]
  local results_key = keys[6]

  local task_id = argv[1]
  local task_data = argv[2]
  local priority = argv[3]
  local score = argv[4]
  local expire = argv[5]
  local current_time = argv[6]

  local args
  if expire ~= '0' then
    args = {'set', task_key, task_data, 'nx', 'px', expire}
  else
    args = {'set', task_key, task_data, 'nx'}
  end

  if not redis.call(unpack(args)) then return 0 end

  local modified = 0
  -- additional args are dependencies for task
  for i=7, #argv do
    local dep_id = argv[i]
    -- update dependency DAG if dependency exists
    if redis.call('exists', results_key .. dep_id) ~= 1 then
      modified = modified + 1
      redis.call('sadd', dependencies_key .. task_id, dep_id)
      redis.call('sadd', dependents_key .. dep_id, task_id)
    end
  end

  -- if there are dependencies don't queue yet
  if modified == 0 then
    -- delayed queue
    if score ~= '0' then
      redis.call('zadd', queue_key .. priority, score, task_id)
    -- live queue
    else
      return redis.call('xadd', stream_key .. priority, '*', 'task_id', task_id, 'enqueue_time', current_time)
    end
  end

  return 1
end)

redis.register_function('read_streams', function(keys, argv)
  local stream_key = keys[1]
  local group_name = keys[2]
  local consumer_name = keys[3]

  local count = tonumber(argv[1])
  local idle = argv[2]

  local entries = {}

  -- additional arguments are the names of custom priorities
  for i = 3, #argv do
    local stream = stream_key .. argv[i]
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
end)

redis.register_function('update_dependents', function(keys, argv)
  local dependents_key = keys[1]
  local dependencies_key = keys[2]
  local task_id = keys[3]

  local runnable = {}

  local deps = redis.call('smembers', dependents_key .. task_id)
  for i = 1, #deps do
    local dep = deps[i]
    redis.call('srem', dependencies_key .. dep, task_id)
    -- if no more dependencies are left, it's time to enqueue!
    if redis.call('scard', dependencies_key .. dep) == 0 then
      table.insert(runnable, dep)
    end
  end

  redis.call('del', dependents_key .. task_id, dependencies_key .. task_id)

  return runnable
end)

redis.register_function('refresh_timeout', function(keys, argv)
  local stream_key = keys[1]
  local group_name = keys[2]

  local consumer = argv[1]
  local message_id = argv[2]

  if #redis.call('xpending', stream_key, group_name, message_id, message_id, 1, consumer) > 0 then
    redis.call('xclaim', stream_key, group_name, consumer, 0, message_id, 'justid')
    return true
  end
  return false
end)

redis.register_function('schedule_cron_job', function(keys, argv)
  local cron_key = keys[1]
  local queue_key = keys[2]
  local data_key = keys[3]
  local task_key = keys[4]

  local task_id = argv[1]
  local score = argv[2]
  local member = argv[3]

  -- check if another worker already handled this
  if redis.call('zadd', cron_key, 'gt', 'ch', score, member) ~= 0 then
    redis.call('zadd', queue_key, score, task_id)
    redis.call('copy', data_key, task_key)
  end
end)

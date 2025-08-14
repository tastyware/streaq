local stream_key = KEYS[1]
local queue_key = KEYS[2]
local task_key = KEYS[3]
local dependents_key = KEYS[4]
local dependencies_key = KEYS[5]
local results_key = KEYS[6]

local task_id = ARGV[1]
local task_data = ARGV[2]
local priority = ARGV[3]
local score = ARGV[4]
local expire = ARGV[5]

local args
if expire ~= '0' then
  args = {'set', task_key, task_data, 'nx', 'px', expire}
else
  args = {'set', task_key, task_data, 'nx'}
end

if not redis.call(unpack(args)) then return 0 end

local modified = 0
-- additional args are dependencies for task
for i=6, #ARGV do
  local dep_id = ARGV[i]
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
    return redis.call('xadd', stream_key .. priority, '*', 'task_id', task_id)
  end
end

return 1

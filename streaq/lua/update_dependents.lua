local dependents_key = KEYS[1]
local dependencies_key = KEYS[2]
local task_id = KEYS[3]

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

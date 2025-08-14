local dependents_key = KEYS[1]
local dependencies_key = KEYS[2]
local task_id = KEYS[3]

local runnable = {}

local deps = redis.call('smembers', dependents_key .. task_id)
for i = 1, #deps do
  redis.call('srem', dependencies_key .. deps[i], task_id)
  -- if no more dependencies are left, it's time to enqueue!
  if #redis.call('smembers', dependencies_key .. deps[i]) == 0 then
    table.insert(runnable, deps[i])
  end
end

redis.call('del', dependents_key .. task_id, dependencies_key .. task_id)

return runnable

local dependents_key = KEYS[1]
local dependencies_key = KEYS[2]
local task_id = KEYS[3]

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

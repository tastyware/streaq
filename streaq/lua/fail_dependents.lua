local dependents_key = KEYS[1]
local dependencies_key = KEYS[2]
local task_id = KEYS[3]

-- traverse DAG and fail all dependents of failed task
local function traverse(tid, failed)
  if not failed[tid] then
    failed[tid] = true
    for _, dep_id in ipairs(redis.call('smembers', dependents_key .. tid)) do
      traverse(dep_id, failed)
      redis.call('srem', dependencies_key .. dep_id, tid)
    end
  redis.call('del', dependents_key .. tid)
  end
end

local visited = {}
traverse(task_id, visited)
local failed = {}
-- mark visited nodes, traverse with DFS
for tid, _ in pairs(visited) do
  if tid ~= task_id then table.insert(failed, tid) end
end

return failed

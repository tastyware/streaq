from typing import Any

from coredis import Redis
from coredis.commands import Script

ADD_DEPENDENCIES = """
local task_key = KEYS[1]
local task_id = KEYS[2]
local dependents_key = KEYS[3]
local dependencies_key = KEYS[4]
local prefix = KEYS[5]

local task_data = ARGV[1]
local ttl = ARGV[2]

if not redis.call('set', task_key, task_data, 'nx', 'px', ttl) then
  return false
end

local modified = 0
for i=3, #ARGV do
  local dep_id = ARGV[i]
  if redis.call('exists', prefix .. dep_id) ~= 1 then
    modified = modified + 1
    redis.call('sadd', dependencies_key .. task_id, dep_id)
    redis.call('sadd', dependents_key .. dep_id, task_id)
  end
end

return modified == 0
"""

PUBLISH_TASK = """
local stream_key = KEYS[1]
local task_key = KEYS[2]
local queue_key = KEYS[3]

local task_id = ARGV[1]
local ttl = ARGV[2]
local task_data = ARGV[3]
local priority = ARGV[4]
local score = ARGV[5]

if not redis.call('set', task_key, task_data, 'nx', 'px', ttl) then
  return 0
end
if score ~= '0' then
  redis.call('zadd', queue_key, score, task_id)
  return 1
else
  return redis.call('xadd', stream_key .. priority, '*', 'task_id', task_id)
end
"""

FAIL_DEPENDENTS = """
local dependents_key = KEYS[1]
local dependencies_key = KEYS[2]
local task_id = KEYS[3]

local function traverse(task_id, failed)
  if not failed[task_id] then
    failed[task_id] = true
    for _, dep_id in ipairs(redis.call('smembers', dependents_key .. task_id)) do
      traverse(dep_id, failed)
      redis.call('srem', dependencies_key .. dep_id, task_id)
    end
  redis.call('del', dependents_key .. task_id)
  end
end

local visited = {}
traverse(task_id, visited)
local failed = {}
for tid, _ in pairs(visited) do
  if tid ~= task_id then
    table.insert(failed, tid)
  end
end

return failed
"""

UPDATE_DEPENDENTS = """
local dependents_key = KEYS[1]
local dependencies_key = KEYS[2]
local task_id = KEYS[3]

local runnable = {}

local deps = redis.call('smembers', dependents_key .. task_id)
for i = 1, #deps do
  redis.call('srem', dependencies_key .. deps[i], task_id)
  if #redis.call('smembers', dependencies_key .. deps[i]) == 0 then
    table.insert(runnable, deps[i])
  end
end

redis.call('del', dependents_key .. task_id, dependencies_key .. task_id)

return runnable
"""

PUBLISH_DELAYED_TASK = """
local queue_key = KEYS[1]
local stream_key = KEYS[2]

local task_id = ARGV[1]
local priority = ARGV[2]

if not redis.call('zscore', queue_key, task_id) then
  return 0
end

redis.call('xadd', stream_key .. priority, '*', 'task_id', task_id)
redis.call('zrem', queue_key, task_id)
return 1
"""

RECLAIM_IDLE_TASKS = """
local timeout_key = KEYS[1]
local stream_key = KEYS[2]
local group_name = KEYS[3]
local consumer_name = KEYS[4]

local current_time = ARGV[1]
local count = ARGV[2]

local messages = {}

for i=3, #ARGV do
  local priority = ARGV[i]
  local timed_out = redis.call(
    'zrangebyscore',
    timeout_key .. priority,
    0,
    current_time,
    'limit',
    0,
    count
  )

  if #timed_out > 0 then
    messages[priority] = redis.call(
      'xclaim',
      stream_key .. priority,
      group_name,
      consumer_name,
      0,
      unpack(timed_out)
    )
    redis.call('zrem', timeout_key .. priority, unpack(timed_out))
    count = count - #messages[priority]
    if count <= 0 then break end
  end
end

return cjson.encode(messages)
"""

CREATE_GROUPS = """
local stream_key = KEYS[1]
local group_name = KEYS[2]

for i=1, #ARGV do
  local stream = stream_key .. ARGV[i]
  local ok, groups = pcall(redis.call, 'xinfo', 'groups', stream)
  if not ok or #groups == 0 then
    redis.call('xgroup', 'create', stream, group_name, '0', 'mkstream')
  end
end
"""


def register_scripts(redis: Redis[Any]) -> dict[str, Script[str]]:
    return {
        "add_dependencies": redis.register_script(ADD_DEPENDENCIES),
        "create_groups": redis.register_script(CREATE_GROUPS),
        "publish_task": redis.register_script(PUBLISH_TASK),
        "publish_delayed_task": redis.register_script(PUBLISH_DELAYED_TASK),
        "fail_dependents": redis.register_script(FAIL_DEPENDENTS),
        "update_dependents": redis.register_script(UPDATE_DEPENDENTS),
        "reclaim_idle_tasks": redis.register_script(RECLAIM_IDLE_TASKS),
    }

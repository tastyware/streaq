from redis.asyncio import Redis
from redis.commands.core import AsyncScript


ADD_DEPENDENCIES = """
local task_key = KEYS[1]
local task_id = KEYS[2]
local dependents_key = KEYS[3]
local dependencies_key = KEYS[4]
local prefix = KEYS[5]


local task_data = ARGV[1]
local ttl = ARGV[2]

if redis.call('set', task_key, task_data, 'nx', 'px', ttl) == nil then
  return
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

if modified == 0 then
  return 1
end

return
"""

PUBLISH_TASK = """
local stream_key = KEYS[1]
local message_key = KEYS[2]
local task_key = KEYS[3]
local queue_key = KEYS[4]

local task_id = ARGV[1]
local ttl = ARGV[2]
local task_data = ARGV[3]
local priority = ARGV[4]
local score = ARGV[5]

local is_new = redis.call('set', task_key, task_data, 'nx', 'px', ttl)
if score then
  redis.call('zadd', queue_key, score, task_id)
  return 1
elseif is_new then
  local message_id = redis.call('xadd', stream_key .. priority, '*', 'task_id', task_id)
  redis.call('set', message_key, message_id, 'px', ttl)
  return message_id
end

return 0
"""

PUBLISH_DELAYED_TASK = """
local delayed_queue_key = KEYS[1]
local stream_key = KEYS[2]
local task_message_id_key = KEYS[3]

local task_id = ARGV[1]
local task_message_id_expire_ms = ARGV[2]
local priority = ARGV[3]

local score = redis.call('zscore', delayed_queue_key, task_id)
if score == nil or score == false then
  return 0
end

local message_id = redis.call('xadd', stream_key .. priority, '*', 'task_id', task_id)
redis.call('set', task_message_id_key, message_id, 'px', task_message_id_expire_ms)
redis.call('zrem', delayed_queue_key, task_id)
return 1
"""

RETRY_TASK = """
local stream_key = KEYS[1]
local message_key = KEYS[2]

local task_id = ARGV[1]
local expire_ms = ARGV[2]

local message_id = redis.call('xadd', stream_key, '*', 'task_id', task_id)
redis.call('set', message_key, message_id, 'px', expire_ms)
return message_id
"""

PUBLISH_DEPENDENT = """
local stream_key = KEYS[1]
local message_key = KEYS[2]
local dep_id = KEYS[3]

local ttl = ARGV[1]

local message_id = redis.call('xadd', stream_key, '*', 'task_id', dep_id)
redis.call('set', message_key, message_id, 'px', ttl)
"""

UNPUBLISH_DEPENDENT = """
local task_key = KEYS[1]
local result_key = KEYS[2]
local channel_key = KEYS[3]
local dep_id = KEYS[4]

local result_data = ARGV[1]

redis.call('del', task_key)
redis.call('set', result_key, result_data, 'ex', 300)
redis.call('publish', channel_key, dep_id)
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

UNCLAIM_IDLE_TASKS = """
local sorted_set_key = KEYS[1]
local stream_key = KEYS[2]
local group_name = KEYS[3]
local consumer_name = KEYS[4]
local message_key = KEYS[5]

local current_time = ARGV[1]
local ttl = ARGV[2]

local timed_out = redis.call('zrangebyscore', sorted_set_key, '-inf', current_time)
if #timed_out > 0 then
  local claimed = redis.call('xclaim', stream_key, group_name, consumer_name, 0, unpack(timed_out))
  for _, message in ipairs(claimed) do
    redis.call('xack', stream_key, group_name, message[1])
    redis.call('xdel', stream_key, message[1])
    local message_id = redis.call('xadd', stream_key, '*', unpack(message[2]))
    redis.call('set', message_key .. message[2][2], message_id, 'px', ttl)
  end
  redis.call('zrem', sorted_set_key, unpack(timed_out))
end

return timed_out
"""


def register_scripts(redis: Redis) -> dict[str, AsyncScript]:
    return {
        "add_dependencies": redis.register_script(ADD_DEPENDENCIES),
        "publish_task": redis.register_script(PUBLISH_TASK),
        "publish_delayed_task": redis.register_script(PUBLISH_DELAYED_TASK),
        "retry_task": redis.register_script(RETRY_TASK),
        "fail_dependents": redis.register_script(FAIL_DEPENDENTS),
        "publish_dependent": redis.register_script(PUBLISH_DEPENDENT),
        "unpublish_dependent": redis.register_script(UNPUBLISH_DEPENDENT),
        "update_dependents": redis.register_script(UPDATE_DEPENDENTS),
        "unclaim_idle_tasks": redis.register_script(UNCLAIM_IDLE_TASKS),
    }

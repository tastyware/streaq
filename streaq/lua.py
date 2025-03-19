from redis.asyncio import Redis
from redis.commands.core import AsyncScript


ADD_DEPENDENCIES = """
local task_key = KEYS[1]
local task_id = KEYS[2]
local graph_key = KEYS[3]
local prefix = KEYS[4]

local task_data = ARGV[1]
local ttl = ARGV[2]

if redis.call('set', task_key, task_data, 'nx', 'px', ttl) == nil then
  return
end

local graph_str = redis.call('get', graph_key)
local dag
if not graph_str then
  dag = { dependents = {}, dependencies = {} }
else
  dag = cjson.decode(graph_str)
end

if not dag.dependents[task_id] then
  dag.dependents[task_id] = {}
end

dag.dependencies[task_id] = {}

local modified = 0
for i=3, #ARGV do
  local dep_id = ARGV[i]
  if redis.call('exists', prefix .. dep_id) ~= 1 then
    modified = modified + 1
    dag.dependencies[task_id][dep_id] = true
    if not dag.dependents[dep_id] then
      dag.dependents[dep_id] = {}
    end
    dag.dependents[dep_id][task_id] = true
  end
end

if modified == 0 then
  return 1
end

graph_str = cjson.encode(dag)
redis.call('set', graph_key, graph_str)

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

if not redis.call('set', task_key, task_data, 'nx', 'px', ttl) then
  return 0
end

if not score then
  local message_id = redis.call('xadd', stream_key .. priority, '*', 'task_id', task_id)
  redis.call('set', message_key, message_id, 'px', ttl)
  return message_id
else
  redis.call('zadd', queue_key, score, task_id)
  return 1
end
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
local task_message_id_key = KEYS[2]

local task_id = ARGV[1]
local task_message_id_expire_ms = ARGV[3]

local message_id = redis.call('xadd', stream_key, '*', 'task_id', task_id)
redis.call('set', task_message_id_key, message_id, 'px', task_message_id_expire_ms)
return message_id
"""

FAIL_DEPENDENTS = """
local graph_key = KEYS[1]
local task_id = KEYS[2]

local graph_str = redis.call('get', graph_key)
local dag
if not graph_str then
  dag = { dependents = {}, dependencies = {} }
else
  dag = cjson.decode(graph_str)
end

if not dag.dependents[task_id] then
  return {}
end

local function traverse(task_id, failed)
  if not failed[task_id] then
    failed[task_id] = true
    for dep_id, _ in pairs(dag.dependents[task_id] or {}) do
      traverse(dep_id, failed)
      dag.dependencies[dep_id][task_id] = nil
    end
  dag.dependents[task_id] = nil
  end
end

local visited = {}
traverse(task_id, visited)
local failed = {}
for tid, _ in pairs(visited) do
  if tid ~= task_id then
    table.insert(failed, tid)
  end
  if next(dag.dependencies[tid] or {}) == nil then
    dag.dependencies[tid] = nil
  end
end

graph_str = cjson.encode(dag)
redis.call('set', graph_key, graph_str)

return failed
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

UPDATE_DEPENDENTS = """
local graph_key = KEYS[1]
local task_id = KEYS[2]

local graph_str = redis.call('get', graph_key)
local dag
if not graph_str then
  dag = { dependents = {}, dependencies = {} }
else
  dag = cjson.decode(graph_str)
end

local runnable = {}

if dag.dependents[task_id] then
  for dependent_id, _ in pairs(dag.dependents[task_id] or {}) do
    dag.dependencies[dependent_id][task_id] = nil
    if next(dag.dependencies[dependent_id]) == nil then
      table.insert(runnable, dependent_id)
      dag.dependencies[dependent_id] = nil
    end
  end
  dag.dependents[task_id] = nil
  dag.dependencies[task_id] = nil

  graph_str = cjson.encode(dag)
  redis.call('set', graph_key, graph_str)
end

return runnable
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
    }

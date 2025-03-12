from redis.asyncio import Redis
from redis.commands.core import AsyncScript


PUBLISH_TASK = """
local stream_key = KEYS[1]
local message_key = KEYS[2]
local task_key = KEYS[3]
local queue_key = KEYS[4]

local task_id = ARGV[1]
local enqueue_time = ARGV[2]
local ttl = ARGV[3]
local task_data = ARGV[4]
local score = ARGV[5]

if redis.call('set', task_key, task_data, 'NX', 'PX', ttl) == nil then
    return 0
end

if score == nil then
    local message_id = redis.call('xadd', stream_key, '*', 'task_id', task_id, 'score', enqueue_time)
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

local score = redis.call('zscore', delayed_queue_key, task_id)
if score == nil or score == false then
    return 0
end

local message_id = redis.call('xadd', stream_key, '*', 'task_id', task_id, 'score', score)
redis.call('set', task_message_id_key, message_id, 'px', task_message_id_expire_ms)
redis.call('zrem', delayed_queue_key, task_id)
return 1
"""

PUBLISH_DEPENDENT_TASK = """
local stream_key = KEYS[1]
local message_key = KEYS[2]
local deps_key = KEYS[3]

local dep_id = ARGV[1]
local enqueue_time = ARGV[2]
local ttl = ARGV[3]

if next(redis.call('smembers', deps_key)) == nil then
    local message_id = redis.call('xadd', stream_key, '*', 'task_id', dep_id, 'score', enqueue_time)
    redis.call('set', message_key, message_id, 'px', ttl)
end
"""

RETRY_TASK = """
local stream_key = KEYS[1]
local task_message_id_key = KEYS[2]

local task_id = ARGV[1]
local score = ARGV[2]
local task_message_id_expire_ms = ARGV[3]

local message_id = redis.call('xadd', stream_key, '*', 'task_id', task_id, 'score', score)
redis.call('set', task_message_id_key, message_id, 'px', task_message_id_expire_ms)
return message_id
"""

UPDATE_DEPENDENCIES = """
local dependents_set = KEYS[1]
local dependencies_set = KEYS[2]
local dependent_key = KEYS[3]
local dependency_key = KEYS[4]
local dependent_id = KEYS[5]
local dependency_id = KEYS[6]

local task_data = ARGV[1]
local ttl = ARGV[2]

if redis.call('exists', dependent_key) == 0 then
    return
end
if redis.call('set', dependency_key, task_data, 'NX', 'PX', ttl) == nil then
    return
end

redis.call('sadd', dependents_set, dependency_id)
redis.call('sadd', dependencies_set, dependent_id)
return 1
"""


def register_scripts(redis: Redis) -> dict[str, AsyncScript]:
    return {
        "publish_task": redis.register_script(PUBLISH_TASK),
        "publish_delayed_task": redis.register_script(PUBLISH_DELAYED_TASK),
        "publish_dependant_task": redis.register_script(PUBLISH_DEPENDENT_TASK),
        "retry_task": redis.register_script(RETRY_TASK),
        "update_dependencies": redis.register_script(UPDATE_DEPENDENCIES),
    }

from redis.asyncio import Redis
from redis.commands.core import AsyncScript


PUBLISH_TASK_LUA = """
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
    return 0  -- Key exists, task not enqueued
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

PUBLISH_DELAYED_TASK_LUA = """
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

RETRY_TASK_LUA = """
local stream_key = KEYS[1]
local task_message_id_key = KEYS[2]

local task_id = ARGV[1]
local score = ARGV[2]
local task_message_id_expire_ms = ARGV[3]

local message_id = redis.call('xadd', stream_key, '*', 'task_id', task_id, 'score', score)
redis.call('set', task_message_id_key, message_id, 'px', task_message_id_expire_ms)
return message_id
"""


def register_scripts(redis: Redis) -> dict[str, AsyncScript]:
    return {
        "publish_task": redis.register_script(PUBLISH_TASK_LUA),
        "publish_delayed_task": redis.register_script(PUBLISH_DELAYED_TASK_LUA),
        "retry_task": redis.register_script(RETRY_TASK_LUA),
    }

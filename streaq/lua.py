PUBLISH_TASK_LUA = """
local stream_key = KEYS[1]
local task_message_id_key = KEYS[2]

local task_id = ARGV[1]
local score = ARGV[2]
local task_message_id_expire_ms = ARGV[3]
local fn_name = ARGV[4]

local message_id = redis.call('xadd', stream_key, '*', 'task_id', task_id, 'score', score, 'fn_name', fn_name)
redis.call('set', task_message_id_key, message_id, 'px', task_message_id_expire_ms)
return message_id
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

FETCH_TASK_LUA = """
local stream_key = KEYS[1]
local task_message_id_key = KEYS[2]

local message_id = redis.call('get', task_message_id_key)
if message_id == false then
    return nil
end

local task = redis.call('xrange', stream_key, message_id, message_id)
if task == nil then
    return nil
end

return task[1]
"""

DEFAULT_QUEUE_NAME = "default"
DEFAULT_TTL = 86_400_000  # 1 day in ms
REDIS_ABORT = ":aborted"
REDIS_CHANNEL = ":channel"
REDIS_GRAPH = ":dag"
REDIS_GROUP = "workers"
REDIS_HEALTH = ":health"
REDIS_LOCK = ":lock"
REDIS_MESSAGE = ":task:message:"
REDIS_PREFIX = "streaq:"
REDIS_RESULT = ":task:results:"
REDIS_RETRY = ":task:retry:"
REDIS_RUNNING = ":task:running:"
REDIS_QUEUE = ":queues:delayed"
REDIS_STREAM = ":queues:stream"
REDIS_TASK = ":task:data:"

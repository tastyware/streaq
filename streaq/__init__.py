import logging

VERSION = "0.1.0"
__version__ = VERSION

DEFAULT_TTL = 86_400_000  # 1 day in ms
REDIS_CHANNEL = "streaq:channel"
REDIS_GROUP = "streaq:workers"
REDIS_HEALTH = "streaq:health"
REDIS_QUEUE = "streaq:queue"
REDIS_MESSAGE = "streaq:messages"
REDIS_STREAM = "streaq:stream"


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# ruff: noqa: E402

from .task import Task, TaskStatus
from .types import WorkerContext
from .utils import StreaqError, StreaqRetry
from .worker import Worker

__all__ = [
    "StreaqError",
    "StreaqRetry",
    "Task",
    "TaskStatus",
    "Worker",
    "WorkerContext",
]

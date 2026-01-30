import logging

import coredis

VERSION = "6.0.0b2"
__version__ = VERSION

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

# disable runtime type checks
coredis.Config.optimized = True

# ruff: noqa: E402

from .task import TaskStatus
from .types import StreaqError, StreaqRetry, TaskContext, TaskDepends, WorkerDepends
from .worker import Worker

__all__ = [
    "StreaqError",
    "StreaqRetry",
    "TaskContext",
    "TaskDepends",
    "TaskStatus",
    "Worker",
    "WorkerDepends",
]

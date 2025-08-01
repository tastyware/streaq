import logging

import coredis

VERSION = "5.1.0"
__version__ = VERSION

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

# disable some runtime checks
coredis.Config.optimized = True

# ruff: noqa: E402

from .task import StreaqRetry, TaskStatus
from .types import TaskContext
from .utils import StreaqError
from .worker import Worker

__all__ = [
    "StreaqError",
    "StreaqRetry",
    "TaskContext",
    "TaskStatus",
    "Worker",
]

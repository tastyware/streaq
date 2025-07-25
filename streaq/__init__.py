import logging

import uvloop

VERSION = "5.0.0"
__version__ = VERSION

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

uvloop.install()

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

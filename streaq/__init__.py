import logging

VERSION = "6.5.0"
__version__ = VERSION

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

# ruff: noqa: E402

from .task import TaskStatus
from .types import (
    StreaqError,
    StreaqRetry,
    TaskContext,
    TaskDepends,  # pyright: ignore[reportDeprecated]
    WorkerDepends,  # pyright: ignore[reportDeprecated]
)
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

import logging

VERSION = "0.1.0"
__version__ = VERSION

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# ruff: noqa: E402

from .task import TaskStatus
from .types import WrappedContext
from .utils import StreaqError
from .worker import StreaqRetry, Worker

__all__ = [
    "StreaqError",
    "StreaqRetry",
    "TaskStatus",
    "Worker",
    "WrappedContext",
]

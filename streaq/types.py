from dataclasses import dataclass
from datetime import timedelta
from typing import Generic, ParamSpec, TypeVar

from redis.asyncio import Redis

P = ParamSpec("P")
POther = ParamSpec("POther")
R = TypeVar("R")
ROther = TypeVar("ROther")
WD = TypeVar("WD")


@dataclass
class StreamMessage:
    """
    Dataclass wrapping data stored in the Redis stream.
    """

    message_id: str
    task_id: str
    priority: str


@dataclass
class WrappedContext(Generic[WD]):
    """
    Dataclass wrapping the user-defined context (contained in `deps`)
    with additional information such as try, the Redis connection, and
    other task-specific data.
    """

    deps: WD
    fn_name: str
    redis: Redis
    task_id: str
    timeout: timedelta | int | None
    tries: int
    ttl: timedelta | int | None
    worker_id: str

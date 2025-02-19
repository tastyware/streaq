from dataclasses import dataclass
from datetime import timedelta
from typing import Generic, ParamSpec, TypeVar

from redis.asyncio import Redis

P = ParamSpec("P")
R = TypeVar("R")
WD = TypeVar("WD")


@dataclass
class StreamMessage:
    message_id: str
    task_id: str
    score: int


@dataclass
class WrappedContext(Generic[WD]):
    deps: WD
    redis: Redis
    task_id: str
    timeout: timedelta | int | None
    tries: int
    ttl: timedelta | int | None
    worker_id: str

from dataclasses import dataclass
from typing import Generic, ParamSpec, TypeVar

from redis.asyncio import Redis
from redis.commands.core import AsyncScript

P = ParamSpec("P")
PNext = ParamSpec("PNext")
R = TypeVar("R")
RNext = TypeVar("RNext")
WD = TypeVar("WD")


@dataclass
class StreamMessage:
    fn_name: str
    message_id: str
    task_id: str
    score: int


@dataclass
class WorkerContext(Generic[WD]):
    """
    Context provided to worker functions, contains deps but also a connection, retry count etc.
    """

    deps: WD
    id: str
    redis: Redis
    scripts: dict[str, AsyncScript]

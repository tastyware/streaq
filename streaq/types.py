from dataclasses import dataclass
from datetime import timedelta
from typing import (
    Any,
    Callable,
    Concatenate,
    Coroutine,
    Generic,
    Optional,
    ParamSpec,
    TypeAlias,
    TypeVar,
)

from coredis import Redis

P = ParamSpec("P")
POther = ParamSpec("POther")
R = TypeVar("R", bound=Optional[object])
ROther = TypeVar("ROther")
WD = TypeVar("WD", bound=Optional[object])


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
    redis: Redis[str]
    task_id: str
    timeout: timedelta | int | None
    tries: int
    ttl: timedelta | int | None
    worker_id: str


AnyCoroutine: TypeAlias = Coroutine[Any, Any, Any]
ReturnCoroutine: TypeAlias = Callable[..., AnyCoroutine]
TypedCoroutine: TypeAlias = Coroutine[Any, Any, R]

Middleware: TypeAlias = Callable[[WrappedContext[WD], ReturnCoroutine], ReturnCoroutine]

AsyncTask: TypeAlias = Callable[Concatenate[WrappedContext[WD], P], TypedCoroutine[R]]
SyncTask: TypeAlias = Callable[Concatenate[WrappedContext[WD], P], R]
AsyncCron: TypeAlias = Callable[[WrappedContext[WD]], TypedCoroutine[R]]
SyncCron: TypeAlias = Callable[[WrappedContext[WD]], R]

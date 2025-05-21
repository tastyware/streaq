from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Concatenate,
    Coroutine,
    Generic,
    Optional,
    ParamSpec,
    Protocol,
    TypeAlias,
    TypeVar,
    overload,
)

from coredis import Redis

if TYPE_CHECKING:
    from streaq.task import RegisteredCron, RegisteredTask

P = ParamSpec("P")
POther = ParamSpec("POther")
R = TypeVar("R", bound=Optional[object])
ROther = TypeVar("ROther", bound=Optional[object])
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

AsyncCron: TypeAlias = Callable[[WrappedContext[WD]], TypedCoroutine[R]]
SyncCron: TypeAlias = Callable[[WrappedContext[WD]], R]
AsyncTask: TypeAlias = Callable[Concatenate[WrappedContext[WD], P], TypedCoroutine[R]]
SyncTask: TypeAlias = Callable[Concatenate[WrappedContext[WD], P], R]


class CronDefinition(Protocol, Generic[WD]):
    @overload
    def __call__(self, fn: AsyncCron[WD, R]) -> RegisteredCron[WD, R]: ...

    @overload
    def __call__(self, fn: SyncCron[WD, R]) -> RegisteredCron[WD, R]: ...  # type: ignore


class TaskDefinition(Protocol, Generic[WD]):
    @overload
    def __call__(self, fn: AsyncTask[WD, P, R]) -> RegisteredTask[WD, P, R]: ...

    @overload
    def __call__(self, fn: SyncTask[WD, P, R]) -> RegisteredTask[WD, P, R]: ...  # type: ignore

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Coroutine,
    Generic,
    Optional,
    ParamSpec,
    Protocol,
    TypeAlias,
    TypeVar,
    overload,
)

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
class TaskContext:
    """
    Dataclass containing task-specific information like the try count.
    """

    fn_name: str
    task_id: str
    timeout: timedelta | int | None
    tries: int
    ttl: timedelta | int | None


AnyCoroutine: TypeAlias = Coroutine[Any, Any, Any]
ReturnCoroutine: TypeAlias = Callable[..., AnyCoroutine]
TypedCoroutine: TypeAlias = Coroutine[Any, Any, R]

Middleware: TypeAlias = Callable[[ReturnCoroutine], ReturnCoroutine]

AsyncCron: TypeAlias = Callable[[], TypedCoroutine[R]]
SyncCron: TypeAlias = Callable[[], R]
AsyncTask: TypeAlias = Callable[P, TypedCoroutine[R]]
SyncTask: TypeAlias = Callable[P, R]


class CronDefinition(Protocol, Generic[WD]):
    @overload
    def __call__(self, fn: AsyncCron[R]) -> RegisteredCron[WD, R]: ...

    @overload
    def __call__(self, fn: SyncCron[R]) -> RegisteredCron[WD, R]: ...  # type: ignore


class TaskDefinition(Protocol, Generic[WD]):
    @overload
    def __call__(self, fn: AsyncTask[P, R]) -> RegisteredTask[WD, P, R]: ...

    @overload
    def __call__(self, fn: SyncTask[P, R]) -> RegisteredTask[WD, P, R]: ...  # type: ignore

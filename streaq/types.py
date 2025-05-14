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
R = TypeVar("R", covariant=True)
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
    redis: Redis[str]
    task_id: str
    timeout: timedelta | int | None
    tries: int
    ttl: timedelta | int | None
    worker_id: str


Middleware: TypeAlias = Callable[
    [WrappedContext[WD], Callable[..., Coroutine]], Callable[..., Coroutine]
]

CronTaskFn: TypeAlias = Callable[[WrappedContext[WD]], Coroutine[Any, Any, R] | R]

TaskFn: TypeAlias = Callable[Concatenate[WrappedContext[WD], P], R]

AsyncTaskFn: TypeAlias = Callable[
    Concatenate[WrappedContext[WD], P], Coroutine[Any, Any, R]
]


class CronTaskDefinitionWrapper(Protocol, Generic[WD]):
    def __call__(self, fn: CronTaskFn[WD, R]) -> RegisteredCron[WD, R]: ...


class NamedTaskFunc(Protocol, Generic[WD, P, R]):
    def __call__(
        self, ctx: WrappedContext[WD], *args: P.args, **kwds: P.kwargs
    ) -> R: ...


class AsyncNamedTaskFunc(Protocol, Generic[WD, P, R]):
    async def __call__(
        self, ctx: WrappedContext[WD], *args: P.args, **kwds: P.kwargs
    ) -> R: ...


class TaskDefinitionWrapper(Protocol, Generic[WD]):
    @overload
    def __call__(self, fn: TaskFn[WD, P, R]) -> RegisteredTask[WD, P, R]: ...  # type: ignore
    @overload
    def __call__(self, fn: AsyncTaskFn[WD, P, R]) -> RegisteredTask[WD, P, R]: ...  # type: ignore

    @overload
    def __call__(self, fn: NamedTaskFunc[WD, P, R]) -> RegisteredTask[WD, P, R]: ...  # type: ignore

    @overload
    def __call__(  # type: ignore
        self, fn: AsyncNamedTaskFunc[WD, P, R]
    ) -> RegisteredTask[WD, P, R]: ...

    def __call__(
        self,
        fn: AsyncNamedTaskFunc[WD, P, R]
        | NamedTaskFunc[WD, P, R]
        | AsyncTaskFn[WD, P, R]
        | TaskFn[WD, P, R],
    ) -> RegisteredTask[WD, P, R]: ...

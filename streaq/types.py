from __future__ import annotations

from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime, timedelta
from inspect import iscoroutinefunction
from typing import (
    Any,
    Awaitable,
    Callable,
    Coroutine,
    Optional,
    ParamSpec,
    TypeAlias,
    TypeVar,
    TypeVarTuple,
    cast,
)

from coredis.commands.function import Library, wraps
from coredis.commands.request import CommandRequest
from coredis.response._callbacks.streams import MultiStreamRangeCallback
from coredis.response._utils import flat_pairs_to_ordered_dict
from coredis.response.types import StreamEntry
from coredis.typing import KeyT, ResponseType
from typing_extensions import TypeIs

C = TypeVar("C", bound=Optional[object])
P = ParamSpec("P")
R = TypeVar("R", bound=Optional[object])
ROther = TypeVar("ROther", bound=Optional[object])
Ts = TypeVarTuple("Ts")


class StreaqError(Exception):
    """
    Base class for all task queuing errors.
    """

    pass


class StreaqCancelled(StreaqError):
    """
    Similar to ``asyncio.CancelledError`` and ``trio.Cancelled``, but can be raised
    manually.
    """

    pass


class StreaqRetry(StreaqError):
    """
    An exception you can manually raise in your tasks to make sure the task
    is retried.

    :param delay:
        amount of time to wait before retrying the task; if None and schedule
        is not passed either, will be the number of tries squared, in seconds
    :param schedule: specific datetime to retry the task at
    """

    def __init__(
        self,
        *args: Any,
        delay: timedelta | int | None = None,
        schedule: datetime | None = None,
    ):
        super().__init__(*args)
        self.delay = delay
        self.schedule = schedule


task_context: ContextVar[TaskContext] = ContextVar("_task_context")
worker_context: ContextVar[Any] = ContextVar("_worker_context")


class _TaskDepends:
    def __getattr__(self, name: str) -> Any:
        ctx = task_context.get(None)
        if not ctx:
            raise StreaqError(
                "Task context can only be accessed inside a running worker!"
            )
        return getattr(ctx, name)


class _WorkerDepends:
    def __getattr__(self, name: str) -> Any:
        try:
            ctx = worker_context.get()
        except LookupError as e:
            raise StreaqError(
                "Worker context can only be accessed inside a running worker!"
            ) from e
        return getattr(ctx, name)


def TaskDepends() -> TaskContext:
    """
    Simple dependency injection wrapper for task dependencies.
    """
    return _TaskDepends()  # type: ignore


def WorkerDepends() -> Any:
    """
    Simple dependency injection wrapper for worker dependencies.
    """
    return _WorkerDepends()


@dataclass(frozen=True)
class StreamMessage:
    """
    Dataclass wrapping data stored in the Redis stream.
    """

    message_id: str
    task_id: str
    priority: str
    enqueue_time: int


@dataclass(frozen=True)
class TaskContext:
    """
    Dataclass containing task-specific information like the try count.
    """

    fn_name: str
    task_id: str
    timeout: timedelta | int | None
    tries: int
    ttl: timedelta | int | None


ReturnCoroutine: TypeAlias = Callable[..., Coroutine[Any, Any, Any]]
TypedCoroutine: TypeAlias = Coroutine[Any, Any, R]
Middleware: TypeAlias = Callable[[ReturnCoroutine], ReturnCoroutine]

AsyncCron: TypeAlias = Callable[[], TypedCoroutine[R]]
SyncCron: TypeAlias = Callable[[], R]
AsyncTask: TypeAlias = Callable[P, TypedCoroutine[R]]
SyncTask: TypeAlias = Callable[P, R]


def is_async_task(
    fn: Callable[P, Awaitable[R]] | Callable[P, R],
) -> TypeIs[Callable[P, Awaitable[R]]]:
    return iscoroutinefunction(fn)


class ReadStreamsCallback(MultiStreamRangeCallback[str]):
    def transform(
        self,
        response: ResponseType,
    ) -> dict[str, tuple[StreamEntry, ...]] | None:
        if not response:
            return None
        return {
            stream_id: tuple(
                StreamEntry(r[0], flat_pairs_to_ordered_dict(r[1])) for r in entries
            )
            for stream_id, entries in cast(list[Any], response)
        }


class Streaq(Library[str]):
    """
    FFI stubs for Lua functions in streaq.lua
    """

    NAME = "streaq"

    @wraps(verify_existence=False)
    def create_groups(
        self, stream_key: KeyT, group_name: KeyT, *priorities: str
    ) -> CommandRequest[None]: ...

    @wraps(verify_existence=False)
    def fail_dependents(
        self, dependents_key: KeyT, dependencies_key: KeyT, task_id: KeyT
    ) -> CommandRequest[list[str]]: ...

    @wraps(verify_existence=False)
    def publish_delayed_tasks(
        self, queue_key: KeyT, stream_key: KeyT, current_time: int, *priorities: str
    ) -> CommandRequest[None]: ...

    @wraps(verify_existence=False)
    def publish_task(
        self,
        stream_key: KeyT,
        queue_key: KeyT,
        task_key: KeyT,
        dependents_key: KeyT,
        dependencies_key: KeyT,
        results_key: KeyT,
        task_id: str,
        task_data: Any,
        priority: str,
        score: int,
        expire: int,
        current_time: int,
        *dependencies: str,
    ) -> CommandRequest[None]: ...

    @wraps(callback=ReadStreamsCallback())
    def read_streams(
        self,
        stream_key: KeyT,
        group_name: KeyT,
        consumer_name: KeyT,
        count: int,
        idle: int,
        *priorities: str,
    ) -> CommandRequest[Any]: ...

    @wraps(verify_existence=False)
    def update_dependents(
        self, dependents_key: KeyT, dependencies_key: KeyT, task_id: KeyT
    ) -> CommandRequest[list[str]]: ...

    @wraps(verify_existence=False)
    def refresh_timeout(
        self, stream_key: KeyT, group_name: KeyT, consumer: str, message_id: str
    ) -> CommandRequest[bool]: ...

    @wraps(verify_existence=False)
    def schedule_cron_job(
        self,
        cron_key: KeyT,
        queue_key: KeyT,
        data_key: KeyT,
        task_key: KeyT,
        task_id: str,
        score: int,
        member: str,
    ) -> CommandRequest[None]: ...

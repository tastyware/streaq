from __future__ import annotations

from collections.abc import Awaitable, Callable, Coroutine
from dataclasses import dataclass
from datetime import datetime, timedelta
from inspect import iscoroutinefunction
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Concatenate,
    NotRequired,
    ParamSpec,
    Protocol,
    TypeAlias,
    TypedDict,
    cast,
    overload,
)
from uuid import NAMESPACE_OID, uuid5

from coredis.commands.function import Library, LibraryT, wraps
from coredis.commands.request import CommandRequest
from coredis.response._callbacks import ResponseCallback
from coredis.response._utils import flat_pairs_to_ordered_dict
from coredis.response.types import StreamEntry
from coredis.typing import KeyT
from typing_extensions import TypeIs, TypeVar, TypeVarTuple

if TYPE_CHECKING:
    from streaq.task import AsyncRegisteredTask, SyncRegisteredTask

C = TypeVar("C", bound=object | None, default=None)
P = ParamSpec("P")
POther = ParamSpec("POther")
R_co = TypeVar("R_co", bound=object | None, covariant=True)
ROther = TypeVar("ROther", bound=object | None)
T = TypeVar("T")
Ts = TypeVarTuple("Ts")


class StreaqError(Exception):
    """
    Base class for all task queuing errors.
    """


class StreaqCancelled(StreaqError):
    """
    Raised when tasks are aborted.
    """


class StreaqRetry(StreaqError):
    """
    An exception you can raise in your tasks to retry a task.

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
    ) -> None:
        super().__init__(*args)
        self.delay = delay
        self.schedule = schedule


class StreamMessage(TypedDict):
    """
    Schema for data stored in the Redis stream.
    """

    message_id: str
    task_id: str
    priority: str
    enqueue_time: str
    success: NotRequired[str]


class TaskData(TypedDict):
    """
    Schema for task blob stored in Redis.
    """

    #: fn_name
    f: str
    #: args
    a: tuple[Any, ...]
    #: kwargs
    k: dict[str, Any]
    #: enqueue_time
    t: int
    #: fallback
    F: NotRequired[str]
    #: child
    C: NotRequired[str]
    #: child is a fallback
    O: NotRequired[int]
    #: ignore parent output
    I: NotRequired[int]


@dataclass(frozen=True, slots=True)
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
TypedCoroutine: TypeAlias = Coroutine[Any, Any, R_co]
Middleware: TypeAlias = Callable[[ReturnCoroutine], ReturnCoroutine]
Entries: TypeAlias = dict[str, tuple[StreamEntry, ...]] | None

AsyncCron: TypeAlias = Callable[[], TypedCoroutine[R_co]]
SyncCron: TypeAlias = Callable[[], R_co]
AsyncTask: TypeAlias = Callable[P, TypedCoroutine[R_co]]
SyncTask: TypeAlias = Callable[P, R_co]


def is_async_task(
    fn: Callable[P, Awaitable[R_co]] | Callable[P, R_co],
) -> TypeIs[Callable[P, Awaitable[R_co]]]:
    return iscoroutinefunction(fn)


class CronDecorator(Protocol):
    @overload
    def __call__(self, fn: AsyncCron[R_co], /) -> AsyncRegisteredTask[[], R_co]: ...  # type: ignore[overload-overlap]

    @overload
    def __call__(self, fn: SyncCron[R_co], /) -> SyncRegisteredTask[[], R_co]: ...

    def __call__(
        self, fn: AsyncCron[R_co] | SyncCron[R_co], /
    ) -> AsyncRegisteredTask[[], R_co] | SyncRegisteredTask[[], R_co]: ...


class TaskDecorator(Protocol):
    @overload
    def __call__(self, fn: AsyncTask[P, R_co], /) -> AsyncRegisteredTask[P, R_co]: ...  # type: ignore[overload-overlap]

    @overload
    def __call__(self, fn: SyncTask[P, R_co], /) -> SyncRegisteredTask[P, R_co]: ...

    def __call__(
        self, fn: AsyncTask[P, R_co] | SyncTask[P, R_co], /
    ) -> AsyncRegisteredTask[P, R_co] | SyncRegisteredTask[P, R_co]: ...


class ReadStreamsCallback(
    ResponseCallback[dict[str, list[list[list[str] | str]]] | None, Entries]
):  # pragma: gated cover[xreadgroup-no-max-count]
    """
    Transform Lua function output to same format as XREAD.
    """

    def transform(
        self, response: dict[str, list[list[list[str] | str]]] | None
    ) -> dict[str, tuple[StreamEntry, ...]] | None:
        if response:
            return {
                stream_id: tuple(
                    StreamEntry(r[0], flat_pairs_to_ordered_dict(r[1])) for r in entries
                )
                for stream_id, entries in cast(list[Any], response)
            }
        return None


_code = (Path(__file__).parent / "lua/streaq.lua").read_text()
_postfix = "_" + uuid5(NAMESPACE_OID, _code).hex[:8]
_name = "streaq" + _postfix
_code = _code.replace("name=streaq", f"name={_name}")
_code = _code.replace("', function(keys", f"{_postfix}', function(keys")


def ffi(
    func: Callable[Concatenate[LibraryT, P], CommandRequest[T]],
) -> Callable[Concatenate[LibraryT, P], CommandRequest[T]]:
    """
    Like coredis' ``wraps``, but maps to the versioned Lua function name.
    """

    return wraps(function_name=func.__name__ + _postfix, verify_existence=False)(func)


class Streaq(Library[str]):
    """
    FFI stubs for Lua functions in streaq.lua.
    """

    CODE = _code
    NAME = _name

    @ffi
    def create_groups(
        self, stream_key: KeyT, group_name: str, *priorities: str
    ) -> CommandRequest[None]: ...

    @ffi
    def publish_delayed_tasks(
        self, queue_key: KeyT, stream_key: KeyT, current_time: int
    ) -> CommandRequest[None]: ...

    @ffi
    def publish_task(
        self,
        stream_key: KeyT,
        queue_key: KeyT,
        task_key: KeyT,
        results_key: KeyT,
        task_id: str,
        task_data: Any,
        score: int,
        expire: int,
        current_time: int,
    ) -> CommandRequest[None]: ...

    @ffi
    def read_streams(
        self,
        stream_key: KeyT,
        group_name: str,
        consumer_name: str,
        count: int,
        idle: int,
        *priorities: str,
    ) -> CommandRequest[Any]: ...

    @ffi
    def claim_task(
        self,
        stream_key: KeyT,
        running_set: KeyT,
        retry_key: KeyT,
        abort_set: KeyT,
        group_name: str,
        consumer: str,
        message_id: str,
        task_id: str,
    ) -> CommandRequest[list[int] | None]: ...

    @ffi
    def schedule_cron_job(
        self,
        cron_key: KeyT,
        queue_key: KeyT,
        data_key: KeyT,
        task_key: KeyT,
        task_id: str,
        score: int,
        member: str,
        expire: int,
    ) -> CommandRequest[None]: ...

    @ffi
    def requeue_task(
        self,
        dead_key: KeyT,
        stream_key: KeyT,
        task_key: KeyT,
        previous_key: KeyT,
        results_key: KeyT,
        retry_key: KeyT,
        results_set: KeyT,
        task_id: str,
        current_time: int,
    ) -> CommandRequest[int]: ...

    @ffi
    def release_child(
        self,
        depends_set: KeyT,
        stream_key: KeyT,
        child_id: str,
        current_time: int,
    ) -> CommandRequest[None]: ...

    @ffi
    def purge_task(
        self,
        dead_key: KeyT,
        stream_key: KeyT,
        depends_set: KeyT,
        task_id: str,
        child_id: str,
        current_time: int,
    ) -> CommandRequest[int]: ...

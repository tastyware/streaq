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
    cast,
    overload,
)

from coredis.commands import Library, wraps
from coredis.response._callbacks.streams import MultiStreamRangeCallback
from coredis.response._utils import flat_pairs_to_ordered_dict
from coredis.response.types import StreamEntry
from coredis.typing import KeyT, ResponseType

if TYPE_CHECKING:  # pragma: no cover
    from streaq.task import RegisteredTask

C = TypeVar("C", bound=Optional[object])
P = ParamSpec("P")
POther = ParamSpec("POther")
R = TypeVar("R", bound=Optional[object])
ROther = TypeVar("ROther", bound=Optional[object])


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


AnyCoroutine: TypeAlias = Coroutine[Any, Any, Any]
ReturnCoroutine: TypeAlias = Callable[..., AnyCoroutine]
TypedCoroutine: TypeAlias = Coroutine[Any, Any, R]

Middleware: TypeAlias = Callable[[ReturnCoroutine], ReturnCoroutine]

AsyncCron: TypeAlias = Callable[[], TypedCoroutine[R]]
SyncCron: TypeAlias = Callable[[], R]
AsyncTask: TypeAlias = Callable[P, TypedCoroutine[R]]
SyncTask: TypeAlias = Callable[P, R]


class CronDefinition(Protocol, Generic[C]):
    @overload
    def __call__(self, fn: AsyncCron[R]) -> RegisteredTask[C, [], R]: ...

    @overload
    def __call__(self, fn: SyncCron[R]) -> RegisteredTask[C, [], R]: ...  # type: ignore


class TaskDefinition(Protocol, Generic[C]):
    @overload
    def __call__(self, fn: AsyncTask[P, R]) -> RegisteredTask[C, P, R]: ...

    @overload
    def __call__(self, fn: SyncTask[P, R]) -> RegisteredTask[C, P, R]: ...  # type: ignore


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

    @wraps()
    def create_groups(
        self, stream_key: KeyT, group_name: KeyT, *priorities: str
    ) -> None: ...

    @wraps()
    def fail_dependents(
        self, dependents_key: KeyT, dependencies_key: KeyT, task_id: KeyT
    ) -> list[str]: ...

    @wraps()
    def publish_delayed_tasks(
        self, queue_key: KeyT, stream_key: KeyT, current_time: int, *priorities: str
    ) -> None: ...

    @wraps()
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
    ) -> None: ...

    @wraps(callback=ReadStreamsCallback())
    def read_streams(
        self,
        stream_key: KeyT,
        group_name: KeyT,
        consumer_name: KeyT,
        count: int,
        idle: int,
        *priorities: str,
    ) -> Any: ...

    @wraps()
    def update_dependents(
        self, dependents_key: KeyT, dependencies_key: KeyT, task_id: KeyT
    ) -> list[str]: ...

    @wraps()
    def refresh_timeout(
        self, stream_key: KeyT, group_name: KeyT, consumer: str, message_id: str
    ) -> bool: ...

    @wraps()
    def schedule_cron_job(
        self,
        cron_key: KeyT,
        queue_key: KeyT,
        data_key: KeyT,
        task_key: KeyT,
        task_id: str,
        score: int,
        member: str,
    ) -> None: ...

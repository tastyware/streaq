from __future__ import annotations

from collections.abc import Callable, Generator, Iterable
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import StrEnum
from functools import cached_property
from typing import (
    TYPE_CHECKING,
    Any,
    Concatenate,
    Generic,
    overload,
)
from uuid import uuid4

from coredis.client import Client

from streaq.types import (
    AsyncTask,
    P,
    POther,
    R,
    ReturnCoroutine,
    ROther,
    Streaq,
    StreaqError,
    SyncTask,
    TaskContext,
    Ts,
    TypedCoroutine,
)
from streaq.utils import asyncify, now_ms

if TYPE_CHECKING:  # pragma: no cover
    from streaq.worker import Worker

_task_context = ContextVar[TaskContext]("_task_context")


class TaskStatus(StrEnum):
    """
    Enum of possible task statuses:
    """

    #: task doesn't exist in Redis
    NOT_FOUND = "missing"
    #: task is in the live queue
    QUEUED = "queued"
    #: task is running on a worker
    RUNNING = "running"
    #: task is in the delayed queue
    SCHEDULED = "scheduled"
    #: task was completed
    DONE = "done"


@dataclass(frozen=True)
class TaskInfo:
    """
    Dataclass containing information about an unfinished task (running or enqueued).
    """

    task_id: str
    fn_name: str
    created_time: int
    args: tuple[Any, ...] = ()
    kwargs: dict[str, Any] = field(default_factory=lambda: dict())
    tries: int = 0
    scheduled: datetime | None = None
    dependencies: set[str] = field(default_factory=lambda: set())
    dependents: set[str] = field(default_factory=lambda: set())
    status: TaskStatus = TaskStatus.NOT_FOUND


@dataclass(frozen=True)
class TaskResult(Generic[R]):
    """
    Dataclass wrapping the result of a task with additional information
    like run time and whether execution terminated successfully.
    """

    task_id: str
    fn_name: str
    created_time: int
    enqueue_time: int
    success: bool
    start_time: int
    finish_time: int
    tries: int
    worker_id: str
    _result: R | BaseException

    @property
    def result(self) -> R:
        if not self.success:
            raise StreaqError(
                "Can't access result for a failed task, use TaskResult.exception "
                "instead!"
            )
        return self._result  # type: ignore

    @property
    def exception(self) -> BaseException:
        if self.success:
            raise StreaqError(
                "Can't access exception for a successful task, use TaskResult.result "
                "instead!"
            )
        return self._result  # type: ignore


@dataclass(slots=True)
class Task(Generic[P, R]):
    """
    Represents a task that has been enqueued or scheduled.

    Awaiting the object directly will enqueue it.
    """

    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    parent: RegisteredTask
    worker: Worker[Any]
    id: str = field(default_factory=lambda: uuid4().hex)
    _after: Task[Any, Any] | None = None
    after: list[str] = field(default_factory=lambda: [])
    delay: timedelta | int | None = None
    schedule: datetime | str | None = None
    priority: str | None = None
    _triggers: Task[Any, Any] | None = None
    _fails_over: bool = False
    _prev_source: str | None = None

    def start(
        self,
        after: str | Iterable[str] | None = None,
        delay: timedelta | int | None = None,
        schedule: datetime | str | None = None,
        priority: str | None = None,
    ) -> Task[P, R]:
        """
        Configure the task to modify schedule, queue, or dependencies.

        :param after: task ID(s) to wait for before running this task
        :param delay: duration to wait before running the task
        :param schedule:
            datetime at which to run the task, or crontab for repeated scheduling,
            follows the specification
            `here <https://github.com/josiahcarlson/parse-crontab?tab=readme-ov-file#description>`_.
        :param priority: priority queue to insert the task

        :return: self
        """
        # merge _after and after into a single list[str]
        if isinstance(after, str):
            self.after.append(after)
        elif after:
            self.after.extend(after)
        self.delay = delay
        self.schedule = schedule
        self.priority = priority
        if (delay and schedule) or (delay and after) or (schedule and after):
            raise StreaqError(
                "Use one of 'delay', 'schedule', or 'after' when enqueuing tasks, not "
                "multiple!"
            )
        return self

    @overload
    def then(
        self: Task[Any, R],
        task: AsyncRegisteredTask[Concatenate[R, POther], ROther]
        | SyncRegisteredTask[Concatenate[R, POther], ROther],
        *_: POther.args,  # for some reason we have to define this but it's not used
        **kwargs: POther.kwargs,
    ) -> Task[Concatenate[R, POther], ROther]: ...

    @overload
    def then(
        self: Task[Any, tuple[*Ts]],
        task: Callable[[*Ts], TypedCoroutine[ROther]] | Callable[[*Ts], ROther],
        **kwargs: Any,
    ) -> Task[Any, ROther]: ...

    def then(self: Task[Any, Any], task: Any, **kwargs: Any) -> Task[Any, Any]:
        """
        Enqueues the given task as a dependent of this one. Positional arguments must
        come from the previous task's output (tuple outputs will be unpacked), and any
        additional arguments can be passed as kwargs.

        :param task: task to feed output to

        :return: task object for newly created, dependent task
        """
        self._triggers = Task((), kwargs, task, self.worker)
        self._triggers._after = self
        return self._triggers

    def otherwise(
        self, task: AsyncRegisteredTask[P, R] | SyncRegisteredTask[P, R]
    ) -> Task[P, R]:
        """
        Enqueues the given task as a fallback of this one. If this task fails, the
        other task will be run with the same arguments. If this task succeeds, the
        other task will be skipped and return the same result.

        :param task: task to fall back to

        :return: task object for newly created, dependent task
        """
        self._triggers = Task(self.args, self.kwargs, task, self.worker)
        self._triggers._after = self
        self._fails_over = True
        return self._triggers

    async def _recurse(self, lib: Streaq, pipe: Client[str], enqueue_time: int) -> None:
        if self._after:
            await self._after._recurse(lib, pipe, enqueue_time)
            self.after.append(self._after.id)
            if self._after._fails_over:
                self._prev_source = self._after._prev_source
            else:
                self._prev_source = self._after.id
        data = await self.serialize(enqueue_time)
        self.worker.publish_task(pipe, self, data, enqueue_time, lib=lib)

    async def _chain(self) -> Task[P, R]:
        now = now_ms()
        # iterate over chain
        if self._after:
            async with self.worker.redis.pipeline(transaction=True) as pipe:
                lib = Streaq(pipe)
                await self._recurse(lib, pipe, now)
        else:
            await self.worker.publish_task(
                self.worker.redis, self, await self.serialize(now), now
            )
        return self

    def __hash__(self) -> int:
        return hash(self.id)

    def __await__(self) -> Generator[Any, None, Task[P, R]]:
        return self._chain().__await__()

    @overload
    def __or__(
        self: Task[Any, R],
        other: AsyncRegisteredTask[[R], ROther] | SyncRegisteredTask[[R], ROther],
    ) -> Task[[R], ROther]: ...

    @overload
    def __or__(
        self: Task[Any, tuple[*Ts]],
        other: Callable[[*Ts], TypedCoroutine[ROther]] | Callable[[*Ts], ROther],
    ) -> Task[Any, ROther]: ...

    def __or__(self: Task[Any, Any], other: Any) -> Task[Any, Any]:
        self._triggers = Task((), {}, other, self.worker)
        self._triggers._after = self
        return self._triggers

    def __xor__(
        self, other: AsyncRegisteredTask[P, R] | SyncRegisteredTask[P, R]
    ) -> Task[P, R]:
        return self.otherwise(other)

    async def serialize(self, enqueue_time: int) -> Any:
        """
        Serializes the task data for sending to the queue.

        :param enqueue_time: the time at which the task was enqueued

        :return: serialized task data
        """
        try:
            data = {
                "f": self.parent.fn_name,
                "a": self.args,
                "k": self.kwargs,
                "t": enqueue_time,
            }
            if self._prev_source:
                data["A"] = self._prev_source
            if self._after and self._after._fails_over:
                data["F"] = self._after.id
            if self._triggers:
                if self._fails_over:
                    data["O"] = self._triggers.id
                else:
                    data["T"] = self._triggers.id
            return await self.worker.serialize(data)
        except Exception as e:
            raise StreaqError(f"Unable to serialize task {self.parent.fn_name}!") from e

    async def status(self) -> TaskStatus:
        """
        Fetch the current status of the task.

        :return: current task status
        """
        return await self.worker.status_by_id(self.id)

    async def result(self, timeout: timedelta | int | None = None) -> TaskResult[R]:
        """
        Wait for and return the task's result, optionally with a timeout.

        :param timeout: amount of time to wait before raising a `TimeoutError`

        :return: wrapped result object
        """
        return await self.worker.result_by_id(self.id, timeout=timeout)

    async def abort(self, timeout: timedelta | int = 5) -> bool:
        """
        Notify workers that the task should be aborted.

        :param timeout: how long to wait to confirm abortion was successful

        :return: whether the task was aborted successfully
        """
        return await self.worker.abort_by_id(self.id, timeout=timeout)

    async def info(self) -> TaskInfo | None:
        """
        Fetch info about a previously enqueued task.

        :return: task info, unless task has finished or doesn't exist
        """
        return await self.worker.info_by_id(self.id)

    async def unschedule(self) -> bool:
        """
        Stop scheduling the repeating task if registered.

        :return: whether the task was unscheduled successfully
        """
        return await self.worker.unschedule_by_id(self.id)


@dataclass(kw_only=True)
class RegisteredTask:
    """
    Base task registry definition containing task properties from the decorator.
    """

    expire: timedelta | int | None
    max_schedule_drift: timedelta | int | None
    max_tries: int | None
    silent: bool
    timeout: timedelta | int | None
    ttl: timedelta | int | None
    unique: bool
    fn_name: str
    crontab: str | None
    worker: Worker[Any]

    def build_context(self, id: str, tries: int = 1) -> TaskContext:
        """
        Creates the context for a task to be run given task metadata
        """
        return TaskContext(
            fn_name=self.fn_name,
            task_id=id,
            timeout=self.timeout,
            tries=tries,
            ttl=self.ttl,
        )

    @property
    def context(self) -> TaskContext:
        """
        Get the current task's unique context. Only available in running tasks.
        """
        try:
            return _task_context.get()
        except LookupError as e:
            raise StreaqError("Context is only available in running tasks!") from e


@dataclass(kw_only=True)
class AsyncRegisteredTask(RegisteredTask, Generic[P, R]):
    """
    Registered task definition for an async function that allows spawning new tasks to
    be enqueued.
    """

    fn: AsyncTask[P, R]

    @cached_property
    def runner(self) -> AsyncTask[P, R]:
        return self.fn

    def enqueue(
        self,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Task[P, R]:
        """
        Serialize the task and send it to the queue for later execution by an
        active worker. Though this isn't async, it should be awaited as it
        returns an object that should be.
        """
        return Task(args, kwargs, self, self.worker)

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> TypedCoroutine[R]:
        return self.fn(*args, **kwargs)


@dataclass(kw_only=True)
class SyncRegisteredTask(RegisteredTask, Generic[P, R]):
    """
    Registered task definition for a sync function that allows spawning new tasks to be
    enqueued.
    """

    fn: SyncTask[P, R]

    @cached_property
    def runner(self) -> AsyncTask[P, R]:
        return asyncify(self.fn, self.worker._limiter)  # pyright: ignore[reportPrivateUsage]

    def enqueue(
        self,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Task[P, R]:
        """
        Serialize the task and send it to the queue for later execution by an
        active worker. Though this isn't async, it should be awaited as it
        returns an object that should be.
        """
        return Task(args, kwargs, self, self.worker)

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        return self.fn(*args, **kwargs)


@dataclass
class RegisteredMiddleware:
    """
    Registered middleware definition, allows for accessing task context.
    """

    _wrapped: Callable[[ReturnCoroutine], ReturnCoroutine]

    def __call__(self, fn: ReturnCoroutine) -> ReturnCoroutine:
        return self._wrapped(fn)

    @property
    def context(self) -> TaskContext:
        """
        Get the current task's unique context. Only available in running middlewares.
        """
        try:
            return _task_context.get()
        except LookupError as e:
            raise StreaqError(
                "Context is only available in running middlewares!"
            ) from e

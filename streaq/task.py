from __future__ import annotations

from collections.abc import Callable, Collection, Generator, Iterable
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import StrEnum
from functools import cached_property
from typing import TYPE_CHECKING, Any, Concatenate, Generic, Literal, cast, overload
from uuid import NAMESPACE_OID, uuid4, uuid5

from coredis import PureToken
from coredis.client import Client

from streaq.types import (
    AsyncTask,
    P,
    POther,
    R_co,
    ReturnCoroutine,
    ROther,
    Streaq,
    StreaqError,
    SyncTask,
    T,
    TaskContext,
    TaskData,
    TypedCoroutine,
)
from streaq.utils import asyncify, now_ms

if TYPE_CHECKING:
    from streaq.worker import Worker

_task_context = ContextVar[TaskContext]("_task_context")


class TaskStatus(StrEnum):
    """
    Enum of possible task statuses.
    """

    #: task is in the live queue
    QUEUED = "queued"
    #: task is running on a worker
    RUNNING = "running"
    #: task is in the delayed queue
    SCHEDULED = "scheduled"
    #: task was completed
    DONE = "done"
    #: task is in dead letter queue
    DEAD = "dead"


@dataclass(frozen=True)
class TaskInfo:
    """
    Dataclass with information about an unfinished task (running or enqueued).
    """

    task_id: str
    fn_name: str
    created_time: int
    status: TaskStatus
    args: tuple[Any, ...] = ()
    kwargs: dict[str, Any] = field(default_factory=dict[str, Any])
    tries: int = 0
    scheduled: datetime | None = None
    child: str | None = None


@dataclass(frozen=True)
class TaskResult(Generic[R_co]):
    """
    Dataclass wrapping the result of a task with additional info.
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
    _result: R_co | BaseException

    @property
    def result(self) -> R_co:
        if not self.success:
            raise StreaqError(
                "Can't access result for a failed task, use TaskResult.exception "
                "instead!"
            )
        return cast(R_co, self._result)

    @property
    def exception(self) -> BaseException:
        if self.success:
            raise StreaqError(
                "Can't access exception for a successful task, use TaskResult.result "
                "instead!"
            )
        return cast(BaseException, self._result)


@dataclass(slots=True)
class Task(Generic[P, R_co]):
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
    delay: timedelta | int | None = None
    schedule: datetime | str | None = None
    priority: str | None = None
    _triggers: Task[Any, Any] | None = None
    _fails_over: bool = False
    _ignores_output: bool = False

    def start(
        self,
        delay: timedelta | int | None = None,
        schedule: datetime | str | None = None,
        priority: str | None = None,
        id: str | None = None,
    ) -> Task[P, R_co]:
        """
        Configure the task to modify schedule, queue, or ID.

        :param delay: duration to wait before running the task
        :param schedule:
            datetime at which to run the task, or crontab for repeated scheduling,
            follows the specification
            `here <https://github.com/josiahcarlson/parse-crontab?tab=readme-ov-file#description>`_.
        :param priority: priority queue to insert the task
        :param id: custom ID for the task

        :return: self
        """
        if delay and schedule:
            raise StreaqError(
                "Use one of 'delay' or 'schedule' when enqueuing tasks, not both!"
            )
        if self._after:
            raise StreaqError("Only call `start()` on the first task in a chain!")
        if id:
            self.id = id
        self.delay = delay
        self.schedule = schedule
        self.priority = priority
        return self

    @overload
    def then(
        self: Task[Any, R_co],
        task: AsyncRegisteredTask[Concatenate[R_co, POther], ROther]
        | SyncRegisteredTask[Concatenate[R_co, POther], ROther],
        *,
        forward: Literal[True] = True,
    ) -> Callable[POther, Task[Concatenate[R_co, POther], ROther]]: ...

    @overload
    def then(
        self,
        task: AsyncRegisteredTask[POther, ROther] | SyncRegisteredTask[POther, ROther],
        *,
        forward: Literal[False],
    ) -> Callable[POther, Task[POther, ROther]]: ...

    def then(
        self,
        task: AsyncRegisteredTask[Any, ROther] | SyncRegisteredTask[Any, ROther],
        *,
        forward: bool = True,
    ) -> Callable[..., Task[Any, ROther]]:
        """
        Enqueues the given task as a dependent of this one.

        :param task: task to run afterwards
        :param forward:
            whether to forward task's result, prepending it to the next one's args

        :return: callable for creating a new, dependent task object
        """

        def bind(*args: POther.args, **kwargs: POther.kwargs) -> Task[POther, ROther]:
            child: Task[POther, ROther] = Task(args, kwargs, task, self.worker)
            child._after = self
            child._ignores_output = not forward
            self._triggers = child
            return child

        return bind

    @overload
    def otherwise(
        self,
        task: AsyncRegisteredTask[P, R_co] | SyncRegisteredTask[P, R_co],
        *,
        forward: Literal[True] = True,
    ) -> Callable[[], Task[P, R_co]]: ...

    @overload
    def otherwise(
        self,
        task: AsyncRegisteredTask[POther, R_co] | SyncRegisteredTask[POther, R_co],
        *,
        forward: Literal[False],
    ) -> Callable[POther, Task[POther, R_co]]: ...

    def otherwise(
        self,
        task: AsyncRegisteredTask[Any, ROther] | SyncRegisteredTask[Any, ROther],
        *,
        forward: bool = True,
    ) -> Callable[..., Task[Any, R_co]]:
        """
        Enqueues the given task as a fallback of this one.

        If this task fails, the other task will be run, with the same arguments if
        forwarding, or provided arguments otherwise. If this task succeeds, the other
        task will be skipped and return the same result.

        :param task: task to fall back to
        :param forward:
            whether to replay task's args/kwargs, otherwise uses provided ones

        :return: callable for creating a new, fallback task object
        """

        def bind(*args: Any, **kwargs: Any) -> Task[Any, R_co]:
            child: Task[Any, R_co] = (
                Task((), self.kwargs, task, self.worker)
                if forward
                else Task(args, kwargs, task, self.worker)
            )
            child._after = self
            child._ignores_output = not forward
            self._fails_over = True
            self._triggers = child
            return child

        return bind

    def map(
        self: Task[P, Collection[T]],
        task: AsyncRegisteredTask[Concatenate[T, POther], ROther]
        | SyncRegisteredTask[Concatenate[T, POther], ROther],
    ) -> Callable[POther, Task[[Collection[T]], list[ROther]]]:
        """
        Run the given task on each item of this task's output in parallel.

        :param task: task to run for each item

        :return:
            callable for creating N new, dependent task objects and aggregating the
            results
        """

        def bind(
            *args: POther.args, **kwargs: POther.kwargs
        ) -> Task[[Collection[T]], list[ROther]]:
            return self.then(self.worker._map)(
                to=task.fn_name, args=args, kwargs=kwargs
            )

        return bind

    def filter(
        self: Task[P, Collection[T]],
        task: AsyncRegisteredTask[Concatenate[T, POther], bool]
        | SyncRegisteredTask[Concatenate[T, POther], bool],
    ) -> Callable[POther, Task[[Collection[T]], list[T]]]:
        """
        Run the given task on each item of this task's output in parallel, filtering
        out any results that evaluate to False.

        :param task: task to filter each item by

        :return:
            callable for creating N new, dependent task objects, aggregating the
            results, and filtering out any that evaluated to False
        """

        def bind(
            *args: POther.args, **kwargs: POther.kwargs
        ) -> Task[[Collection[T]], list[T]]:
            return self.then(self.worker._filter)(
                by=task.fn_name, args=args, kwargs=kwargs
            )

        return bind

    async def _recurse(self, lib: Streaq, pipe: Client[str], enqueue_time: int) -> None:
        if self._after:
            await self._after._recurse(lib, pipe, enqueue_time)
        if self._triggers:
            self._triggers.id = uuid5(NAMESPACE_OID, self.id).hex
        data = await self.serialize(enqueue_time)
        if self._after:
            pipe.set(self.worker._task_key + self.id, data, condition=PureToken.NX)
        else:
            if isinstance(self.schedule, str):
                raise StreaqError("Cron tasks can't be chained!")
            self.worker._publish_task(pipe, self, data, enqueue_time, lib=lib)

    async def _chain(self) -> Task[P, R_co]:
        now = now_ms()
        # iterate over chain
        if self._after:
            async with self.worker.redis.pipeline(transaction=True) as pipe:
                lib = Streaq(pipe)
                await self._recurse(lib, pipe, now)
        else:
            await self.worker._publish_task(
                self.worker.redis, self, await self.serialize(now), now
            )
        return self

    def __hash__(self) -> int:
        return hash(self.id)

    def __await__(self) -> Generator[Any, None, Task[P, R_co]]:
        return self._chain().__await__()

    async def serialize(self, enqueue_time: int) -> Any:
        """
        Serializes the task data for sending to the queue.

        :param enqueue_time: the time at which the task was enqueued

        :return: serialized task data
        """
        try:
            data: TaskData = {
                "f": self.parent.fn_name,
                "a": self.args,
                "k": self.kwargs,
                "t": enqueue_time,
            }
            if self._after and self._after._fails_over:
                data["F"] = self._after.id
            if self._triggers:
                data["C"] = self._triggers.id
            if self._ignores_output:
                data["I"] = 1
            return await self.worker.serialize(data)
        except Exception as e:
            raise StreaqError(f"Unable to serialize task {self.parent.fn_name}!") from e

    async def status(self) -> TaskStatus | None:
        """
        Fetch the current status of the task.

        :return: current task status
        """
        return await self.worker.status_by_id(self.id)

    async def result(self, timeout: timedelta | int | None = None) -> TaskResult[R_co]:
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

    expire: timedelta | int | None = None
    max_tries: int | None = None
    retry_timeouts: bool = False
    silent: bool = False
    timeout: timedelta | int | None = None
    ttl: timedelta | int | None = None
    fn_name: str
    crontab: str | None = None
    worker: Worker[Any]

    def build_context(self, id: str, tries: int = 1) -> TaskContext:
        """
        Creates the context for a task to be run given task metadata.
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


class PipelineMixin(RegisteredTask, Generic[P, R_co]):
    """
    Fan-out helpers shared by sync and async registered tasks.
    """

    def map(
        self: PipelineMixin[Concatenate[T, POther], R_co], data: Iterable[T]
    ) -> Callable[POther, Task[[Collection[T]], list[R_co]]]:
        """
        Run this task on each item of this task's output in parallel.

        :param data: items to run tasks on and aggregate

        :return:
            callable for creating N new, dependent task objects and aggregating the
            results
        """

        def bind(
            *args: POther.args, **kwargs: POther.kwargs
        ) -> Task[[Collection[T]], list[R_co]]:
            return self.worker._map.enqueue(
                list(data), to=self.fn_name, args=args, kwargs=kwargs
            )

        return bind

    def filter(
        self: PipelineMixin[Concatenate[T, POther], bool], data: Iterable[T]
    ) -> Callable[POther, Task[[Collection[T]], list[T]]]:
        """
        Run this task on each item given in parallel, filtering out any results that
        evaluate to False.

        :param data: items to run tasks on and filter by

        :return:
            callable for creating N new, dependent task objects, aggregating the
            results, and filtering out any that evaluated to False
        """

        def bind(
            *args: POther.args, **kwargs: POther.kwargs
        ) -> Task[[Collection[T]], list[T]]:
            return self.worker._filter.enqueue(
                list(data), by=self.fn_name, args=args, kwargs=kwargs
            )

        return bind


@dataclass(kw_only=True)
class AsyncRegisteredTask(PipelineMixin[P, R_co]):
    """
    Definition for an async function that can be enqueued.
    """

    fn: AsyncTask[P, R_co]

    @cached_property
    def runner(self) -> AsyncTask[P, R_co]:
        return self.fn

    def enqueue(
        self,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Task[P, R_co]:
        """
        Serialize the task and send it to the queue for execution by a worker.

        Though this isn't async, it should be awaited as it returns an object that
        should be.
        """
        return Task(args, kwargs, self, self.worker)

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> TypedCoroutine[R_co]:
        return self.fn(*args, **kwargs)


@dataclass(kw_only=True)
class SyncRegisteredTask(PipelineMixin[P, R_co]):
    """
    Definition for a sync function that can be enqueued.
    """

    fn: SyncTask[P, R_co]

    @cached_property
    def runner(self) -> AsyncTask[P, R_co]:
        return asyncify(self.fn, self.worker._limiter)

    def enqueue(
        self,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Task[P, R_co]:
        """
        Serialize the task and send it to the queue for execution by a worker.

        Though this isn't async, it should be awaited as it
        returns an object that should be.
        """
        return Task(args, kwargs, self, self.worker)

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R_co:
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
        Get the current task's unique context.

        Only available in running middlewares.
        """
        try:
            return _task_context.get()
        except LookupError as e:
            raise StreaqError(
                "Context is only available in running middlewares!"
            ) from e

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Concatenate,
    Generator,
    Generic,
    Iterable,
    overload,
)
from uuid import uuid4

from streaq.constants import REDIS_TASK
from streaq.types import (
    AsyncTask,
    P,
    R,
    ROther,
    Streaq,
    StreaqError,
    SyncTask,
    Ts,
    TypedCoroutine,
)
from streaq.utils import datetime_ms, now_ms, to_ms

if TYPE_CHECKING:  # pragma: no cover
    from streaq.worker import Worker


class TaskStatus(str, Enum):
    """
    Enum of possible task statuses:
    """

    NOT_FOUND = "missing"
    QUEUED = "queued"
    RUNNING = "running"
    SCHEDULED = "scheduled"
    DONE = "done"


@dataclass(frozen=True)
class TaskInfo:
    """
    Dataclass containing information about a running or enqueued task.
    """

    fn_name: str
    created_time: int
    tries: int
    scheduled: datetime | None
    dependencies: set[str]
    dependents: set[str]


@dataclass(frozen=True)
class TaskResult(Generic[R]):
    """
    Dataclass wrapping the result of a task with additional information
    like run time and whether execution terminated successfully.
    """

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


@dataclass
class Task(Generic[R]):
    """
    Represents a task that has been enqueued or scheduled.

    Awaiting the object directly will enqueue it.
    """

    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    parent: RegisteredTask
    worker: Worker[Any]
    id: str = field(default_factory=lambda: uuid4().hex)
    _after: Task[Any] | None = None
    after: list[str] = field(default_factory=lambda: [])
    delay: timedelta | int | None = None
    schedule: datetime | str | None = None
    priority: str | None = None
    _triggers: Task[Any] | None = None

    def start(
        self,
        after: str | Iterable[str] | None = None,
        delay: timedelta | int | None = None,
        schedule: datetime | str | None = None,
        priority: str | None = None,
    ) -> Task[R]:
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

    async def _enqueue(self) -> Task[R]:
        """
        This is called when the task is awaited.
        """
        if self._after:
            self.after.append(self._after.id)
        enqueue_time = now_ms()
        data = self.serialize(enqueue_time)
        self.priority = self.priority or self.worker.priorities[-1]
        expire = to_ms(self.parent.expire or 0)
        if self.schedule:
            if isinstance(self.schedule, str):
                score = self.worker.next_run(self.schedule)
                # add to cron registry
                async with self.worker.redis.pipeline(transaction=False) as pipe:
                    pipe.set(self.worker.cron_data_key + self.id, data)
                    pipe.hset(self.worker.cron_registry_key, {self.id: self.schedule})
                    pipe.zadd(self.worker.cron_schedule_key, {self.id: score})
                    Streaq(pipe).publish_task(
                        self.worker.stream_key,
                        self.worker.queue_key,
                        self.task_key(REDIS_TASK),
                        self.worker.dependents_key,
                        self.worker.dependencies_key,
                        self.worker.results_key,
                        self.id,
                        data,
                        self.priority,
                        score,
                        expire,
                        enqueue_time,
                        *self.after,
                    )
                return self
            score = datetime_ms(self.schedule)
        elif self.delay is not None:
            score = enqueue_time + to_ms(self.delay)
        else:
            score = 0
        await self.worker.lib.publish_task(
            self.worker.stream_key,
            self.worker.queue_key,
            self.task_key(REDIS_TASK),
            self.worker.dependents_key,
            self.worker.dependencies_key,
            self.worker.results_key,
            self.id,
            data,
            self.priority,
            score,
            expire,
            enqueue_time,
            *self.after,
        )
        return self

    @overload
    def then(
        self: Task[R],
        task: AsyncRegisteredTask[Concatenate[R, P], ROther]
        | SyncRegisteredTask[Concatenate[R, P], ROther],
        *_: P.args,  # for some reason we have to define this although it's not used
        **kwargs: P.kwargs,
    ) -> Task[ROther]: ...

    @overload
    def then(
        self: Task[tuple[*Ts]],
        task: Callable[[*Ts], TypedCoroutine[ROther]] | Callable[[*Ts], ROther],
        **kwargs: Any,
    ) -> Task[ROther]: ...

    def then(self: Task[Any], task: Any, **kwargs: Any) -> Task[Any]:
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

    async def _chain(self) -> Task[R]:
        # traverse backwards
        if self._after:
            await self._after
        return await self._enqueue()

    def __hash__(self) -> int:
        return hash(self.id)

    def __await__(self) -> Generator[Any, None, Task[R]]:
        return self._chain().__await__()

    @overload
    def __or__(
        self: Task[R],
        other: AsyncRegisteredTask[[R], ROther] | SyncRegisteredTask[[R], ROther],
    ) -> Task[ROther]: ...

    @overload
    def __or__(
        self: Task[tuple[*Ts]],
        other: Callable[[*Ts], TypedCoroutine[ROther]] | Callable[[*Ts], ROther],
    ) -> Task[ROther]: ...

    def __or__(self: Task[Any], other: Any) -> Task[Any]:
        self._triggers = Task((), {}, other, self.worker)
        self._triggers._after = self
        return self._triggers

    def task_key(self, mid: str) -> str:
        return self.worker.prefix + mid + self.id

    def serialize(self, enqueue_time: int) -> Any:
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
            if self._after:
                data["A"] = self._after.id
            if self._triggers:
                data["T"] = self._triggers.id
            return self.worker.serialize(data)
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

    async def unschedule(self) -> None:
        """
        Stop scheduling the repeating task if registered.
        """
        await self.worker.unschedule_by_id(self.id)


@dataclass(kw_only=True)
class RegisteredTask:
    expire: timedelta | int | None
    max_tries: int | None
    silent: bool
    timeout: timedelta | int | None
    ttl: timedelta | int | None
    unique: bool
    fn_name: str
    crontab: str | None
    worker: Worker[Any]


@dataclass(kw_only=True)
class AsyncRegisteredTask(RegisteredTask, Generic[P, R]):
    fn: AsyncTask[P, R]

    def enqueue(
        self,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Task[R]:
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
    fn: SyncTask[P, R]

    def enqueue(
        self,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Task[R]:
        """
        Serialize the task and send it to the queue for later execution by an
        active worker. Though this isn't async, it should be awaited as it
        returns an object that should be.
        """
        return Task(args, kwargs, self, self.worker)

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        return self.fn(*args, **kwargs)

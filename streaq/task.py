from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from hashlib import sha256
from time import time
from typing import TYPE_CHECKING, Any, Generator, Generic
from uuid import UUID, uuid4

from anyio import fail_after
from crontab import CronTab

from streaq import logger
from streaq.constants import REDIS_TASK
from streaq.types import AsyncCron, AsyncTask, C, P, POther, R, ROther
from streaq.utils import StreaqError, datetime_ms, now_ms, to_ms, to_seconds

if TYPE_CHECKING:  # pragma: no cover
    from streaq.worker import Worker


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


class TaskStatus(str, Enum):
    """
    Enum of possible task statuses:
    """

    NOT_FOUND = "missing"
    QUEUED = "queued"
    RUNNING = "running"
    SCHEDULED = "scheduled"
    DONE = "done"


@dataclass
class TaskInfo:
    """
    Dataclass containing information about a running or enqueued task.
    """

    fn_name: str
    enqueue_time: int
    tries: int
    scheduled: datetime | None
    dependencies: set[str]
    dependents: set[str]


@dataclass
class TaskResult(Generic[R]):
    """
    Dataclass wrapping the result of a task with additional information
    like run time and whether execution terminated successfully.
    """

    fn_name: str
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
    parent: RegisteredCron[Any, R] | RegisteredTask[Any, Any, R]
    id: str = field(default_factory=lambda: uuid4().hex)
    _after: Task[Any] | None = None
    after: list[str] = field(default_factory=lambda: [])
    delay: timedelta | int | None = None
    schedule: datetime | None = None
    priority: str | None = None
    _triggers: Task[Any] | None = None

    def start(
        self,
        after: str | list[str] | None = None,
        delay: timedelta | int | None = None,
        schedule: datetime | None = None,
        priority: str | None = None,
    ) -> Task[R]:
        """
        Configure the task to modify schedule, queue, or dependencies.

        :param after: task ID(s) to wait for before running this task
        :param delay: duration to wait before running the task
        :param schedule: datetime at which to run the task
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
        if self.schedule:
            score = datetime_ms(self.schedule)
        elif self.delay is not None:
            score = enqueue_time + to_ms(self.delay)
        else:
            score = 0
        data = self.serialize(enqueue_time)
        self.priority = self.priority or self.parent.worker.priorities[-1]
        expire = to_ms(self.parent.expire or 0)
        if not await self.parent.worker.redis.fcall(
            "publish_task",
            keys=[
                self.parent.worker.stream_key,
                self.parent.worker.queue_key,
                self.task_key(REDIS_TASK),
                self.parent.worker.dependents_key,
                self.parent.worker.dependencies_key,
                self.parent.worker.results_key,
            ],
            args=[self.id, data, self.priority, score, expire] + self.after,
        ):
            logger.debug("Task is unique and already exists, not enqueuing!")
        return self

    def then(
        self, task: RegisteredTask[C, POther, ROther], **kwargs: Any
    ) -> Task[ROther]:
        """
        Enqueues the given task as a dependent of this one. Positional arguments will
        come from the previous task's output (tuple outputs will be unpacked), and any
        additional arguments can be passed as kwargs.

        :param task: task to feed output to

        :return: task object for newly created, dependent task
        """
        self._triggers = Task((), kwargs, task)
        self._triggers._after = self
        return self._triggers

    async def _chain(self) -> Task[R]:
        # traverse backwards
        if self._after:
            await self._after
        return await self._enqueue()

    def __await__(self) -> Generator[Any, None, Task[R]]:
        return self._chain().__await__()

    def __or__(self, other: RegisteredTask[C, POther, ROther]) -> Task[ROther]:
        self._triggers = Task((), {}, other)
        self._triggers._after = self
        return self._triggers

    def task_key(self, mid: str) -> str:
        return self.parent.worker.prefix + mid + self.id

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
            return self.parent.worker.serialize(data)
        except Exception as e:
            raise StreaqError(f"Unable to serialize task {self.parent.fn_name}!") from e

    async def status(self) -> TaskStatus:
        """
        Fetch the current status of the task.

        :return: current task status
        """
        return await self.parent.worker.status_by_id(self.id)

    async def result(self, timeout: timedelta | int | None = None) -> TaskResult[R]:
        """
        Wait for and return the task's result, optionally with a timeout.

        :param timeout: amount of time to wait before raising a `TimeoutError`

        :return: wrapped result object
        """
        return await self.parent.worker.result_by_id(self.id, timeout=timeout)

    async def abort(self, timeout: timedelta | int = 5) -> bool:
        """
        Notify workers that the task should be aborted.

        :param timeout: how long to wait to confirm abortion was successful

        :return: whether the task was aborted successfully
        """
        return await self.parent.worker.abort_by_id(self.id, timeout=timeout)

    async def info(self) -> TaskInfo | None:
        """
        Fetch info about a previously enqueued task.

        :return: task info, unless task has finished or doesn't exist
        """
        return await self.parent.worker.info_by_id(self.id)


@dataclass
class RegisteredTask(Generic[C, P, R]):
    fn: AsyncTask[P, R]
    expire: timedelta | int | None
    max_tries: int | None
    silent: bool
    timeout: timedelta | int | None
    ttl: timedelta | int | None
    unique: bool
    worker: Worker[C]
    _fn_name: str | None = None

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
        return Task(args, kwargs, self)

    async def run(self, *args: P.args, **kwargs: P.kwargs) -> R:
        """
        Run the task in the local event loop with the given params and return the
        result. This skips enqueuing and result storing in Redis.
        """
        with fail_after(to_seconds(self.timeout)):
            return await self.fn(*args, **kwargs)

    @property
    def fn_name(self) -> str:
        return self._fn_name or self.fn.__qualname__


@dataclass
class RegisteredCron(Generic[C, R]):
    fn: AsyncCron[R]
    crontab: CronTab
    max_tries: int | None
    silent: bool
    timeout: timedelta | int | None
    ttl: timedelta | int | None
    unique: bool
    worker: Worker[C]
    expire: timedelta | int | None = None
    _fn_name: str | None = None

    def enqueue(self) -> Task[R]:
        """
        Serialize the task and send it to the queue for later execution by an
        active worker. Though this isn't async, it should be awaited as it
        returns an object that should be.
        """
        task = Task((), {}, self)
        uid_bytes = f"{self.fn_name}@{datetime_ms(self.schedule())}".encode()
        deterministic_hash = sha256(uid_bytes).hexdigest()
        task.id = UUID(bytes=bytes.fromhex(deterministic_hash[:32]), version=4).hex
        return task

    async def run(self) -> R:
        """
        Run the task in the local event loop and return the result.
        This skips enqueuing and result storing in Redis.
        """
        with fail_after(to_seconds(self.timeout)):
            return await self.fn()

    @property
    def fn_name(self) -> str:
        return self._fn_name or self.fn.__qualname__

    def schedule(self) -> datetime:
        """
        Datetime of next run.
        """
        return datetime.fromtimestamp(self.next() / 1000, tz=self.worker.tz)

    def next(self) -> int:
        """
        Timestamp in milliseconds of next run.
        """
        return round((time() + self.delay) * 1000)

    @property
    def delay(self) -> float:
        return self.crontab.next(now=datetime.now(self.worker.tz))  # type: ignore

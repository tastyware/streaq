import asyncio
import hashlib
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from time import time
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Concatenate,
    Coroutine,
    Generic,
)
from uuid import UUID, uuid4

from crontab import CronTab
from redis.asyncio import Redis

from streaq import logger
from streaq.constants import (
    DEFAULT_TTL,
    REDIS_DEPENDENCIES,
    REDIS_DEPENDENTS,
    REDIS_MESSAGE,
    REDIS_PREFIX,
    REDIS_RESULT,
    REDIS_TASK,
)
from streaq.types import P, POther, R, ROther, WD, WrappedContext
from streaq.utils import StreaqError, datetime_ms, now_ms, to_seconds, to_ms

if TYPE_CHECKING:
    from streaq.worker import Worker


class StreaqRetry(StreaqError):
    """
    An exception you can manually raise in your tasks to make sure the task
    is retried.

    :param msg: error message to show
    :param delay:
        amount of time to wait before retrying the task; if none, will be the
        square of the number of attempts in seconds
    """

    def __init__(self, msg: str, delay: timedelta | int | None = None):
        super().__init__(msg)
        self.delay = delay


class TaskPriority(str, Enum):
    """
    Enum of possible task priority levels.
    """

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class TaskStatus(str, Enum):
    """
    Enum of possible task statuses.
    """

    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    SCHEDULED = "scheduled"
    DONE = "done"


@dataclass
class TaskData:
    """
    Dataclass containing additional task information not stored locally,
    such as try and time enqueued.
    """

    fn_name: str
    enqueue_time: int
    task_try: int | None = None
    scheduled: datetime | None = None


@dataclass
class TaskResult(Generic[R]):
    """
    Dataclass wrapping the result of a task with additional information
    like run time and whether execution terminated successfully.
    """

    success: bool
    result: R | Exception
    start_time: int
    finish_time: int
    queue_name: str


@dataclass
class Task(Generic[R]):
    """
    Represents a task that has been enqueued or scheduled.
    """

    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    parent: "RegisteredCron | RegisteredTask"
    _after: "Task | None" = None
    _triggers: "Task | None" = None
    id: str = ""

    def __post_init__(self):
        if self.parent.unique:
            deterministic_hash = hashlib.sha256(
                self.parent.fn_name.encode()
            ).hexdigest()
            self.id = UUID(bytes=bytes.fromhex(deterministic_hash[:32]), version=4).hex
        else:
            self.id = uuid4().hex

    async def start(
        self,
        after: str | list[str] | None = None,
        delay: timedelta | int | None = None,
        schedule: datetime | None = None,
        priority: TaskPriority = TaskPriority.LOW,
    ) -> "Task[R]":
        """
        Enqueues a task immediately, for running after a delay, or for running
        at a specified time.

        :param after: task ID(s) to wait for before running this task
        :param delay: duration to wait before running the task
        :param schedule: datetime at which to run the task
        :param priority: priority queue to insert the task

        :return: self
        """
        after = self._after.id if self._after else after
        if (delay and schedule) or (delay and after) or (schedule and after):
            raise StreaqError(
                "Use one of 'delay', 'schedule', or 'after' when enqueuing tasks, not "
                "multiple!"
            )
        if not self.parent.worker.scripts:
            raise StreaqError(
                "Worker did not initialize correctly, are you using the async context "
                "manager?"
            )
        enqueue_time = now_ms()
        if schedule:
            score = datetime_ms(schedule)
        elif delay is not None:
            score = enqueue_time + to_ms(delay)
        else:
            score = None
        ttl = (score or enqueue_time) - enqueue_time + DEFAULT_TTL
        data = self.serialize(enqueue_time)
        run_now = False
        if after:
            if isinstance(after, str):
                after = [after]
            run_now = await self.parent.worker.scripts["add_dependencies"](
                keys=[
                    self._task_key(REDIS_TASK),
                    self.id,
                    REDIS_PREFIX + self.queue + REDIS_DEPENDENTS,
                    REDIS_PREFIX + self.queue + REDIS_DEPENDENCIES,
                    REDIS_PREFIX + self.queue + REDIS_RESULT,
                ],
                args=[data, ttl] + after,
            )
        if not after or run_now:
            keys = [
                self.parent.worker._stream_key,
                self._task_key(REDIS_MESSAGE),
                self._task_key(REDIS_TASK),
                self.parent.worker._queue_key,
            ]
            args = [self.id, ttl, data, priority]
            if score:
                args.append(score)
            res = await self.parent.worker.scripts["publish_task"](keys=keys, args=args)
            if res == 0:
                logger.debug("Task is unique and already exists, not enqueuing!")
                return self

        return self

    def then(
        self, task: "RegisteredTask[WD, POther, ROther]", **kwargs
    ) -> "Task[ROther]":
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

    async def _chain(self) -> "Task[R]":
        # traverse backwards
        if self._after:
            await self._after
        return await self.start()

    def __await__(self):
        return self._chain().__await__()

    def _task_key(self, mid: str) -> str:
        return REDIS_PREFIX + self.queue + mid + self.id

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
            return self.parent.worker.serializer(data)
        except Exception as e:
            raise StreaqError(f"Unable to serialize task {self.parent.fn_name}:") from e

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

    async def info(self) -> TaskData:
        """
        Fetch info about a previously enqueued task.

        :return: task info object
        """
        return await self.parent.worker.info_by_id(self.id)

    @property
    def redis(self) -> Redis:
        return self.parent.worker.redis

    @property
    def queue(self) -> str:
        return self.parent.worker.queue_name


@dataclass
class RegisteredTask(Generic[WD, P, R]):
    fn: Callable[Concatenate[WrappedContext[WD], P], Coroutine[Any, Any, R]]
    max_tries: int | None
    timeout: timedelta | int | None
    ttl: timedelta | int | None
    unique: bool
    worker: "Worker"
    _fn_name: str | None = None

    def enqueue(
        self,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Task[R]:
        """
        Serialize the task and send it to the queue for later execution by an active worker.
        Though this isn't async, it should be awaited as it returns an object that should be.
        """
        return Task(args, kwargs, self)

    async def run(self, *args: P.args, **kwargs: P.kwargs) -> R:
        """
        Run the task in the local event loop with the given params and return the result.
        This skips enqueuing and result storing in Redis.
        """
        deps = WrappedContext(
            deps=self.worker.deps,
            fn_name=self.fn_name,
            redis=self.worker.redis,
            task_id=uuid4().hex,
            timeout=self.timeout,
            tries=1,
            ttl=self.ttl,
            worker_id=self.worker.id,
        )
        try:
            return await asyncio.wait_for(
                self.fn(deps, *args, **kwargs),
                to_seconds(self.timeout) if self.timeout else None,
            )
        except asyncio.TimeoutError as e:
            raise e

    @property
    def fn_name(self) -> str:
        return self._fn_name or self.fn.__qualname__

    def __repr__(self):
        return f"<Task fn={self.fn_name} timeout={self.timeout} ttl={self.ttl}>"


@dataclass
class RegisteredCron(Generic[WD, R]):
    fn: Callable[[WrappedContext[WD]], Coroutine[Any, Any, R]]
    max_tries: int | None
    crontab: CronTab
    timeout: timedelta | int | None
    ttl: timedelta | int | None
    unique: bool
    worker: "Worker"

    def enqueue(self) -> Task[R]:
        """
        Serialize the task and send it to the queue for later execution by an active worker.
        Though this isn't async, it should be awaited as it returns an object that should be.
        """
        return Task((), {}, self)

    async def run(self) -> R:
        """
        Run the task in the local event loop and return the result.
        This skips enqueuing and result storing in Redis.
        """
        deps = WrappedContext(
            deps=self.worker.deps,
            fn_name=self.fn_name,
            redis=self.worker.redis,
            task_id=uuid4().hex,
            timeout=self.timeout,
            tries=1,
            ttl=self.ttl,
            worker_id=self.worker.id,
        )
        try:
            return await asyncio.wait_for(
                self.fn(deps),
                to_seconds(self.timeout) if self.timeout else None,
            )
        except asyncio.TimeoutError as e:
            raise e

    @property
    def fn_name(self) -> str:
        return self.fn.__qualname__

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

    def __repr__(self):
        return (
            f"<Cron fn={self.fn_name} timeout={self.timeout} "
            f"schedule={self.schedule()}>"
        )

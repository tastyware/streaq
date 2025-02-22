import asyncio
import hashlib
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from time import time
from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Callable,
    Concatenate,
    Generic,
)
from uuid import UUID, uuid4

from crontab import CronTab
from redis import WatchError
from redis.asyncio import Redis
from redis.typing import EncodableT

from streaq import logger
from streaq.constants import (
    DEFAULT_TTL,
    REDIS_ABORT,
    REDIS_CHANNEL,
    REDIS_MESSAGE,
    REDIS_QUEUE,
    REDIS_RESULT,
    REDIS_RETRY,
    REDIS_RUNNING,
    REDIS_STREAM,
    REDIS_TASK,
)
from streaq.types import P, R, WD, WrappedContext
from streaq.utils import StreaqError, datetime_ms, now_ms, to_seconds, to_ms

if TYPE_CHECKING:
    from streaq.worker import Worker


class TaskStatus(str, Enum):
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
    id: str = ""

    def __post_init__(self):
        if self.id:
            return
        if self.parent.unique:
            deterministic_hash = hashlib.sha256(
                self.parent.fn_name.encode()
            ).hexdigest()
            self.id = UUID(bytes=bytes.fromhex(deterministic_hash[:32]), version=4).hex
        else:
            self.id = uuid4().hex

    async def start(
        self, delay: timedelta | int | None = None, schedule: datetime | None = None
    ) -> "Task[R]":
        """
        Enqueues a task immediately, for running after a delay, or for running
        at a specified time.

        :param delay: duration to wait before running the task
        :param schedule: datetime at which to run the task

        :return: self
        """
        if delay and schedule:
            raise StreaqError(
                "Use either 'delay' or 'schedule' when enqueuing tasks, not both!"
            )
        ttl_ms = to_ms(self.parent.ttl) if self.parent.ttl is not None else None
        async with self.redis.pipeline(transaction=True) as pipe:
            key = self._task_key(REDIS_TASK)
            await pipe.watch(key)
            if await pipe.exists(key, self._task_key(REDIS_RESULT)):
                await pipe.reset()
                logger.debug("Task is unique and already exists, not enqueuing!")
                return self
            enqueue_time = now_ms()
            if schedule:
                score = datetime_ms(schedule)
            elif delay is not None:
                score = enqueue_time + to_ms(delay)
            else:
                score = None
            ttl = ttl_ms or (score or enqueue_time) - enqueue_time + DEFAULT_TTL
            data = self.serialize(enqueue_time)
            pipe.multi()
            pipe.psetex(key, ttl, data)
            if score is not None:
                pipe.zadd(self.queue + REDIS_QUEUE, {self.id: score})
            else:
                await self.parent.worker.scripts["publish_task"](
                    keys=[
                        self.queue + REDIS_STREAM,
                        self._task_key(REDIS_MESSAGE),
                    ],
                    args=[self.id, enqueue_time, ttl],
                    client=pipe,
                )
            try:
                await pipe.execute()
            except WatchError:
                logger.debug("Task is unique and already exists, not enqueuing!")

        return self

    def __await__(self):
        return self.start().__await__()

    async def status(self) -> TaskStatus:
        """
        Fetch the current status of the task.
        """
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.exists(self._task_key(REDIS_RESULT))
            pipe.exists(self._task_key(REDIS_RUNNING))
            pipe.zscore(self.queue + REDIS_QUEUE, self.id)
            pipe.exists(self._task_key(REDIS_MESSAGE))
            is_complete, is_in_progress, score, queued = await pipe.execute()

        if is_complete:
            return TaskStatus.DONE
        elif is_in_progress:
            return TaskStatus.RUNNING
        elif queued:
            return TaskStatus.QUEUED
        elif score:
            return TaskStatus.SCHEDULED
        return TaskStatus.PENDING

    def _task_key(self, mid: str) -> str:
        return self.queue + mid + self.id

    def serialize(self, enqueue_time: int) -> EncodableT:
        """
        Serializes the task data for sending to the queue.

        :param enqueue_time: the time at which the task was enqueued

        :return: serialized task data
        """
        try:
            return self.parent.worker.serializer(
                {
                    "f": self.parent.fn_name,
                    "a": self.args,
                    "k": self.kwargs,
                    "t": enqueue_time,
                }
            )
        except Exception as e:
            raise StreaqError(f"Unable to serialize task {self.parent.fn_name}:") from e

    async def result(self, timeout: timedelta | int | None = None) -> TaskResult[R]:
        """
        Wait for and return the task's result, optionally with a timeout.

        :param timeout: amount of time to wait before raising a `TimeoutError`

        :return: wrapped result object
        """
        raw = await self.redis.get(self._task_key(REDIS_RESULT))
        if raw is None:
            timeout_seconds = to_seconds(timeout) if timeout is not None else None
            await asyncio.wait_for(self._listen_for_result(), timeout_seconds)
            raw = await self.redis.get(self._task_key(REDIS_RESULT))
        try:
            data = self.parent.worker.deserializer(raw)
            return TaskResult(
                success=data["s"],
                result=data["r"],
                start_time=data["st"],
                finish_time=data["ft"],
                queue_name=self.queue,
            )
        except Exception as e:
            raise StreaqError(
                f"Unable to deserialize result for task {self.id}:"
            ) from e

    async def _listen_for_result(self) -> None:
        pubsub = self.redis.pubsub()
        await pubsub.subscribe(self.queue + REDIS_CHANNEL)
        encoded_id = self.id.encode()
        async for msg in pubsub.listen():
            if msg.get("data") == encoded_id:
                break

    async def abort(self, timeout: timedelta | int | None = None) -> bool:
        """
        Notify workers that the task should be aborted.

        :param timeout: how long to wait to confirm abortion was successful

        :return: whether the task was aborted successfully
        """
        await self.redis.sadd(self.queue + REDIS_ABORT, self.id)  # type: ignore
        try:
            result = await self.result(timeout=timeout)
            return not result.success and isinstance(
                result.result, asyncio.CancelledError
            )
        except asyncio.CancelledError:
            return False

    async def info(self) -> TaskData:
        """
        Fetch info about a previously enqueued task.

        :return: task info object
        """
        async with self.redis.pipeline(transaction=False) as pipe:
            pipe.get(self._task_key(REDIS_TASK))
            pipe.get(self._task_key(REDIS_RETRY))
            pipe.zscore(self.queue + REDIS_QUEUE, self.id)
            raw, task_try, score = await pipe.execute()
        data = self.parent.worker.deserializer(raw)
        dt = (
            datetime.fromtimestamp(score / 1000, tz=self.parent.worker.tz)
            if score
            else None
        )
        return TaskData(
            fn_name=self.parent.fn_name,
            enqueue_time=data["t"],
            task_try=task_try,
            scheduled=dt,
        )

    @property
    def redis(self) -> Redis:
        return self.parent.worker.redis

    @property
    def queue(self) -> str:
        return self.parent.worker.queue_name


@dataclass
class RegisteredTask(Generic[WD, P, R]):
    fn: Callable[Concatenate[WrappedContext[WD], P], Awaitable[R]]
    lifespan: Callable[[WrappedContext[WD]], AbstractAsyncContextManager[None]]
    max_tries: int | None
    timeout: timedelta | int | None
    ttl: timedelta | int | None
    unique: bool
    worker: "Worker"

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
            redis=self.worker.redis,
            task_id=uuid4().hex,
            timeout=self.timeout,
            tries=1,
            ttl=self.ttl,
            worker_id=self.worker.id,
        )
        async with self.lifespan(deps):
            try:
                return await asyncio.wait_for(
                    self.fn(deps, *args, **kwargs),
                    to_seconds(self.timeout) if self.timeout else None,
                )
            except asyncio.TimeoutError as e:
                raise e

    @property
    def fn_name(self) -> str:
        return self.fn.__qualname__

    def __repr__(self):
        return (
            f"<Task fn={self.fn_name} lifespan={self.lifespan} timeout={self.timeout} "
            f"ttl={self.ttl}>"
        )


@dataclass
class RegisteredCron(Generic[WD, R]):
    fn: Callable[[WrappedContext[WD]], Awaitable[R]]
    lifespan: Callable[[WrappedContext[WD]], AbstractAsyncContextManager[None]]
    max_tries: int | None
    run_at_startup: bool
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
            redis=self.worker.redis,
            task_id=uuid4().hex,
            timeout=self.timeout,
            tries=1,
            ttl=self.ttl,
            worker_id=self.worker.id,
        )
        async with self.lifespan(deps):
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

    def _upcoming(self) -> int:
        # Timestamp in ms for run after next run.
        diff = self.crontab.next(now=self.schedule())  # type: ignore
        return self.next() + round(diff * 1000)

    @property
    def delay(self) -> float:
        return self.crontab.next(now=datetime.now(self.worker.tz))  # type: ignore

    def __repr__(self):
        return (
            f"<Cron fn={self.fn_name} lifespan={self.lifespan} timeout={self.timeout} "
            f"ttl={self.ttl} schedule={self.schedule()}>"
        )

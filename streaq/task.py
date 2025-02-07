import asyncio
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, AsyncIterator, Awaitable, Callable, Concatenate, Generic
from uuid import uuid4

from redis.asyncio import Redis, WatchError
from redis.typing import EncodableT

from streaq import DEFAULT_TTL, REDIS_MESSAGE, REDIS_QUEUE, REDIS_STREAM, logger
from streaq.types import P, R, WD, WorkerContext
from streaq.utils import StreaqError, datetime_ms, now_ms, to_seconds, to_ms


class TaskStatus(str, Enum):
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    SCHEDULED = "scheduled"
    DONE = "done"


@asynccontextmanager
async def task_lifespan(ctx: WorkerContext[WD]) -> AsyncIterator[None]:
    logger.info("Job started.")
    yield
    logger.info("Job finished.")


@dataclass
class Task(Generic[WD, P, R]):
    """
    Represents a job that has been enqueued or scheduled.
    """

    deserializer: Callable[[EncodableT], Any]
    fn_name: str
    redis: Redis
    id: str = uuid4().hex

    async def status(self) -> TaskStatus:
        async with self.redis.pipeline(transaction=True) as pipe:
            await pipe.exists(self.result_key)
            await pipe.exists(in_progress_key_prefix + self.job_id)
            await pipe.zscore(REDIS_QUEUE, self.id)
            await pipe.exists(job_message_id_prefix + self.job_id)
            is_complete, is_in_progress, score, queued = await pipe.execute()

        if is_complete:
            return TaskStatus.DONE
        elif is_in_progress:
            return TaskStatus.RUNNING
        elif queued:
            return TaskStatus.QUEUED
        elif score:
            return TaskStatus.SCHEDULED
        raise StreaqError()

    async def result(self) -> R | None:
        result = await self.redis.get(self.result_key)
        return self.deserializer(result) if result else None

    async def abort(self) -> bool: ...

    @property
    def task_key(self) -> str:
        return f"streaq:task:{self.fn_name}:{self.id}"

    @property
    def result_key(self) -> str:
        return f"streaq:results:{self.fn_name}:{self.id}"


@dataclass
class RegisteredTask(Generic[WD, P, R]):
    deps: Callable[[], WorkerContext[WD]]
    fn: Callable[Concatenate[WorkerContext[WD], P], Awaitable[R]]
    serializer: Callable[[Any], EncodableT]
    deserializer: Callable[[EncodableT], Any]
    lifespan: Callable[[WorkerContext[WD]], AbstractAsyncContextManager[None]]
    timeout: timedelta | int | None
    ttl: timedelta | int | None
    unique: bool
    _id: str = uuid4().hex  # for unique jobs only

    # TODO: add exception handler
    # TODO: add dependency injection
    # TODO: add triggers next job/starts with
    # TODO: optimizations, __slots__?

    async def enqueue(
        self,
        _delay: timedelta | int | None = None,
        _schedule: datetime | None = None,
        _ttl: timedelta | int | None = None,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Task[WD, P, R]:
        """
        Serialize the job and send it to the queue for later execution by an active worker.

        :param _delay: duration to wait before running the job
        :param _schedule: datetime at which to run the job
        :param _ttl: max time to leave the job in the queue before it expires
        """
        if _delay and _schedule:
            raise StreaqError(
                "Use either '_delay' or '_schedule' when enqueuing tasks, not both!"
            )
        publish_task = self.deps().scripts["publish_task"]
        ttl_ms = to_ms(_ttl) if _ttl is not None else None
        async with self.redis.pipeline(transaction=True) as pipe:
            if self.unique:
                redis_key = self.task_key(self._id)
                await pipe.watch(redis_key)
                if await pipe.exists(
                    redis_key
                ):  # TODO: should result_key be checked here too?
                    await pipe.reset()
                    raise StreaqError("Task is unique and already exists!")
            enqueue_time = now_ms()
            if _schedule:
                score = datetime_ms(_schedule)
            elif _delay is not None:
                score = enqueue_time + to_ms(_delay)
            else:
                score = None
            ttl = ttl_ms or (score or enqueue_time) - enqueue_time + DEFAULT_TTL
            data = self.serializer(
                (
                    args,
                    kwargs,
                    enqueue_time,
                )
            )
            pipe.multi()
            if self.unique:
                task = Task(self.deserializer, self.fn_name, self.redis, id=self._id)
            else:
                task = Task(self.deserializer, self.fn_name, self.redis)
            await pipe.psetex(self.task_key(task.id), ttl, data)

            if score is not None:
                await pipe.zadd(REDIS_QUEUE, {task.id: score})
            else:
                await publish_task(
                    keys=[REDIS_STREAM, REDIS_MESSAGE],
                    args=[task.id, enqueue_time, ttl, self.fn_name],
                )

            try:
                await pipe.execute()
            except WatchError:
                raise StreaqError("Task is unique and already exists!")
        return task

    async def run(self, *args: P.args, **kwargs: P.kwargs) -> R:
        """
        Run the task with the given params and return the result.
        """
        deps = self.deps()
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
        return f"{self.fn.__module__}.{self.fn.__qualname__}"

    @property
    def redis(self) -> Redis:
        return self.deps().redis

    def task_key(self, id: str) -> str:
        return f"streaq:task:{self.fn_name}:{id}"

    def result_key(self, id: str) -> str:
        return f"streaq:results:{self.fn_name}:{id}"

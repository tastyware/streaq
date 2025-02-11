import asyncio
import hashlib
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Concatenate,
    Generic,
)
from uuid import UUID, uuid4

from redis.asyncio import Redis
from redis.typing import EncodableT

from streaq import logger
from streaq.constants import (
    DEFAULT_TTL,
    REDIS_MESSAGE,
    REDIS_QUEUE,
    REDIS_RESULT,
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


@asynccontextmanager
async def task_lifespan(ctx: WrappedContext[WD]) -> AsyncIterator[None]:
    logger.info(f"Task {ctx.task_id} started in worker {ctx.worker_id}.")
    yield
    logger.info(f"Task {ctx.task_id} finished.")


@dataclass
class Task(Generic[WD, P, R]):
    """
    Represents a job that has been enqueued or scheduled.
    """

    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    parent: "RegisteredTask"
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
        self, delay: timedelta | int | None = None, schedule: datetime | None = None
    ) -> "Task[WD, P, R]":
        """

        :param delay: duration to wait before running the job
        :param schedule: datetime at which to run the job
        """
        if delay and schedule:
            raise StreaqError(
                "Use either 'delay' or 'schedule' when enqueuing tasks, not both!"
            )
        publish_task = self.parent.worker.scripts["publish_task"]
        ttl_ms = to_ms(self.parent.ttl) if self.parent.ttl is not None else None
        async with self.redis.pipeline(transaction=True) as pipe:
            enqueue_time = now_ms()
            if schedule:
                score = datetime_ms(schedule)
            elif delay is not None:
                score = enqueue_time + to_ms(delay)
            else:
                score = None
            ttl = ttl_ms or (score or enqueue_time) - enqueue_time + DEFAULT_TTL
            data = self.parent.serializer(
                (
                    self.args,
                    self.kwargs,
                )
            )
            pipe.psetex(self.task_key(REDIS_TASK), ttl, data)
            if score is not None:
                pipe.zadd(self.queue + REDIS_QUEUE, {self.id: score})
            else:
                await publish_task(
                    keys=[
                        self.queue + REDIS_STREAM,
                        self.task_key(REDIS_MESSAGE),
                    ],
                    args=[
                        self.id,
                        enqueue_time,
                        ttl,
                        self.parent.fn_name,
                    ],
                    client=pipe,
                )
            await pipe.execute()

        return self

    def __await__(self):
        return self.start().__await__()

    async def status(self) -> TaskStatus:
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.exists(self.task_key(REDIS_RESULT))
            pipe.exists(self.task_key(REDIS_RUNNING))
            pipe.zscore(self.queue + REDIS_QUEUE, self.id)
            pipe.exists(self.task_key(REDIS_MESSAGE))
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

    def task_key(self, mid: str) -> str:
        return self.queue + mid + self.id

    async def result(self) -> R | None:
        result = await self.redis.get(self.task_key(REDIS_RESULT))
        if result is None:
            # pubsub
            pass
        return self.parent.deserializer(result) if result else None

    async def abort(self) -> bool: ...

    async def depends(self): ...

    async def then(self): ...

    @property
    def redis(self) -> Redis:
        return self.parent.worker.redis

    @property
    def queue(self) -> str:
        return self.parent.worker.queue_name


@dataclass
class RegisteredTask(Generic[WD, P, R]):
    fn: Callable[Concatenate[WrappedContext[WD], P], Awaitable[R]]
    serializer: Callable[[Any], EncodableT]
    deserializer: Callable[[EncodableT], Any]
    lifespan: Callable[[WrappedContext[WD]], AbstractAsyncContextManager[None]]
    timeout: timedelta | int | None
    ttl: timedelta | int | None
    unique: bool
    worker: "Worker"

    # TODO: add exception handler
    # TODO: add dependency injection
    # TODO: optimizations, __slots__?

    def enqueue(
        self,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Task[WD, P, R]:
        """
        Serialize the job and send it to the queue for later execution by an active worker.
        Though this isn't async, it should be awaited as it returns an object that should be.
        """
        return Task(args, kwargs, self)

    async def run(self, *args: P.args, **kwargs: P.kwargs) -> R:
        """
        Run the task directly with the given params and return the result.
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
            f"<Task fn={self.fn_name} serializer={self.serializer} deserializer="
            f"{self.deserializer} lifespan={self.lifespan} timeout={self.timeout} "
            f"ttl={self.ttl}>"
        )

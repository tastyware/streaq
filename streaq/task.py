import asyncio
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, AsyncIterator, Awaitable, Callable, Concatenate, Generic
from uuid import uuid4

from redis.asyncio import Redis
from redis.typing import EncodableT

from streaq import DEFAULT_TTL, REDIS_MESSAGE, REDIS_QUEUE, REDIS_STREAM, logger
from streaq.lua import PUBLISH_TASK_LUA
from streaq.types import P, R, WD, PNext, RNext, WorkerContext
from streaq.utils import StreaqError, datetime_ms, now_ms, to_seconds, to_ms


class TaskStatus(str, Enum):
    PENDING = "Pending"
    QUEUED = "Queued"
    RUNNING = "Running"
    DEFERRED = "Deferred"
    DONE = "Done"


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
        return TaskStatus.PENDING

    async def result(self) -> R:
        result = await self.redis.get(self.result_key)
        return self.deserializer(result)

    def triggers(
        self,
        *on_success: "RegisteredTask[WD, PNext, RNext]",
    ) -> "Task[WD, PNext, RNext]":
        """
        also takes all kwargs from `start()`

        TODO - AFAIK there's no way to enforce that `PNext` is `(R,)` - e.g. that the return value of the
        first function is the input to the second function.

        The even more complex case is where you have multiple jobs triggering a job, where I'm even more sure
        full type safety is impossible.

        I would therefore suggest that subsequent jobs are not allowed to take any arguments, and instead
        access the results of previous jobs via `FunctionContext`
        """
        ...

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

    # TODO: add exception handler
    # TODO: add dependency injection

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
        ttl_ms = to_ms(_ttl) if _ttl is not None else None
        async with self.redis.pipeline(transaction=True) as pipe:
            enqueue_time = now_ms()
            if _schedule:
                score = datetime_ms(_schedule)
            elif _delay is not None:
                score = enqueue_time + to_ms(_delay)
            else:
                score = None
            ttl = ttl_ms or (score or enqueue_time) - enqueue_time + DEFAULT_TTL
            data = self.serializer(
                {
                    "a": args,
                    "k": kwargs,
                    "t": enqueue_time,
                }
            )
            pipe.multi()
            task = Task(self.deserializer, self.fn_name, self.redis)
            pipe.psetex(self.task_key(task.id), ttl, data)

            if score is not None:
                pipe.zadd(REDIS_QUEUE, {task.id: score})
            else:
                pipe.eval(
                    PUBLISH_TASK_LUA,
                    2,
                    # keys
                    REDIS_STREAM,
                    REDIS_MESSAGE,
                    # args
                    task.id,
                    str(enqueue_time),
                    str(ttl),
                    self.fn_name,
                )

            await pipe.execute()
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

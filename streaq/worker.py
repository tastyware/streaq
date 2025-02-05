import asyncio
import pickle
from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from typing import AsyncIterator, Concatenate, Generic, ParamSpec, TypeVar
from uuid import uuid4

from croniter import croniter
from redis.asyncio import Redis

from streaq import logger
from streaq.utils import StreaqError, timeout_seconds

WD = TypeVar("WD")
P = ParamSpec("P")
R = TypeVar("R")

GROUP = "streaq:workers"
STREAM = "streaq:stream"


class TaskStatus(str, Enum):
    PENDING = "Pending"
    QUEUED = "Queued"
    RUNNING = "Running"
    DEFERRED = "Deferred"
    DONE = "Done"


@dataclass
class WorkerContext(Generic[WD]):
    """
    Context provided to worker functions, contains deps but also a connection, retry count etc.
    """

    deps: WD
    id: str
    redis: Redis


@dataclass
class PendingTask(Generic[WD, P, R]):
    """
    Represents a job that has not been enqueued yet.
    """

    fn: Callable[Concatenate[WorkerContext[WD], P], Awaitable[R]]
    key: str

    async def status(self) -> TaskStatus:
        return TaskStatus.PENDING

    async def result(self) -> R: ...


@dataclass
class Task(Generic[WD, P, R]):
    deps: Callable[[], WorkerContext[WD]]
    fn: Callable[Concatenate[WorkerContext[WD], P], Awaitable[R]]
    timeout: timedelta | int | None = None
    ttl: timedelta | int | None = None
    id: str = uuid4().hex

    async def enqueue(self, *args: P.args, **kwargs: P.kwargs) -> PendingTask[WD, P, R]:
        """
        Serialize the job and send it to the queue for later execution by an active worker.
        """
        data = pickle.dumps(
            {
                "fn_name": self.fn_name,
                "args": args,
                "kwargs": kwargs,
            }
        )
        async with self.redis.pipeline(transaction=True) as pipe:
            await pipe.watch(self.queue_key)
            if await pipe.exists(self.queue_key, self.result_key):
                await pipe.reset()
                return None

        return PendingTask(self.fn, self.queue_key)

    async def run(self, *args: P.args, **kwargs: P.kwargs) -> R:
        """
        Run the task directly, without enqueuing, and return the result.
        """
        return await self.fn(self.deps(), *args, **kwargs)

    @property
    def fn_name(self) -> str:
        return f"{self.fn.__module__}.{self.fn.__qualname__}"

    @property
    def queue_key(self) -> str:
        return f"streaq:job:{self.fn_name}:{self.id}"

    @property
    def result_key(self) -> str:
        return f"streaq:results:{self.fn_name}:{self.id}"

    @property
    def redis(self) -> Redis:
        return self.deps().redis


@asynccontextmanager
async def worker_lifespan() -> AsyncIterator[None]:
    logger.info("Worker started.")
    yield
    logger.info("Worker shutdown.")


@asynccontextmanager
async def job_lifespan(ctx: WorkerContext[WD]) -> AsyncIterator[None]:
    logger.info("Job started.")
    yield
    logger.info("Job finished.")


class Worker(Generic[WD]):
    def __init__(
        self,
        redis: Redis = Redis.from_url("redis://localhost:6379"),
        concurrency: int = 32,
        job_lifespan: Callable[
            [WorkerContext[WD]], AbstractAsyncContextManager[None]
        ] = job_lifespan,
        worker_lifespan: Callable[[], AbstractAsyncContextManager] = worker_lifespan,
    ):
        self.redis = redis
        self.concurrency = concurrency
        self.worker_lifespan = worker_lifespan()
        self.job_lifespan = job_lifespan
        self._deps: WD | None = None
        self.registry: dict[str, Task] = {}
        self.id = uuid4().hex

    def task(
        self,
        timeout: timedelta | int | None = None,
        ttl: timedelta | int | None = None,
    ):
        def wrapped(
            fn: Callable[Concatenate[WorkerContext[WD], P], Awaitable[R]],
        ) -> Task[WD, P, R]:
            task = Task(self.deps, fn, timeout=timeout, ttl=ttl)
            self.registry[task.fn_name] = task
            return task

        return wrapped

    def cron(
        self,
        cron_str: str,
        timeout: timedelta | int | None = None,
        ttl: timedelta | int | None = None,
    ):
        def wrapped(
            fn: Callable[Concatenate[WorkerContext[WD], P], Awaitable[R]],
        ) -> Task[WD, P, R]:
            task = Task(self.deps, fn, timeout=timeout, ttl=ttl)
            self.registry[task.fn_name] = task
            return task

        return wrapped

    def deps(self) -> WorkerContext[WD]:
        if self._deps is None:
            raise StreaqError("Worker did not start correctly!")
        return WorkerContext(deps=self._deps, id=self.id, redis=self.redis)

    async def start(self):
        async with self:
            while True:
                jobs = await self.redis.xreadgroup(
                    groupname=GROUP,
                    consumername=self.id,
                    streams={STREAM: ">"},
                    block=0,
                    count=self.concurrency,
                )
                print(jobs)
                await asyncio.sleep(1)

    async def run(self, fn_name: str, *args, **kwargs):
        """
        Execute the registered task, then store the result in Redis.
        """
        task = self.registry[fn_name]
        async with self.job_lifespan(self.deps()):
            result = await asyncio.wait_for(
                task.run(*args, **kwargs), timeout_seconds(task.timeout)
            )
        if result is not None:
            data = pickle.dumps(result)
            await self.redis.set(task.queue_key, data, ex=task.ttl)

    async def __aenter__(self):
        self._deps = await self.worker_lifespan.__aenter__()
        return self

    async def __aexit__(self, *exc):
        await self.worker_lifespan.__aexit__(*exc)

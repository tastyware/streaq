import asyncio
import pickle
from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager, suppress
from datetime import timedelta
from typing import Any, AsyncIterator, Concatenate, Generic
from uuid import uuid4

from croniter import croniter
from redis.asyncio import Redis
from redis.exceptions import ResponseError
from redis.typing import EncodableT

from streaq import REDIS_CHANNEL, logger, REDIS_GROUP, REDIS_STREAM
from streaq.types import P, R, WD, StreamMessage, WorkerContext
from streaq.utils import StreaqError
from streaq.task import RegisteredTask, task_lifespan


@asynccontextmanager
async def worker_lifespan() -> AsyncIterator[None]:
    logger.info("Worker started.")
    yield
    logger.info("Worker shutdown.")


class Worker(Generic[WD]):
    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        concurrency: int = 16,
        queue_fetch_limit: int | None = None,
        lifespan: Callable[[], AbstractAsyncContextManager] = worker_lifespan,
        serializer: Callable[[Any], EncodableT] = pickle.dumps,
        deserializer: Callable[[EncodableT], Any] = pickle.loads,  # type: ignore
    ):
        self.redis = Redis.from_url(redis_url)
        self.concurrency = concurrency
        self.queue_fetch_limit = queue_fetch_limit or concurrency * 4
        self.bs = asyncio.BoundedSemaphore(concurrency + 1)
        self.lifespan = lifespan()
        self.task_lifespan = task_lifespan
        self._deps: WD | None = None
        self.registry: dict[str, RegisteredTask] = {}
        self.id = uuid4().hex
        self.serializer = serializer
        self.deserializer = deserializer
        self.queue = asyncio.Queue()

    def task(
        self,
        lifespan: Callable[
            [WorkerContext[WD]], AbstractAsyncContextManager[None]
        ] = task_lifespan,
        timeout: timedelta | int | None = None,
        ttl: timedelta | int | None = None,
        unique: bool = False,  # TODO: implement this
    ):
        def wrapped(
            fn: Callable[Concatenate[WorkerContext[WD], P], Awaitable[R]],
        ) -> RegisteredTask[WD, P, R]:
            task = RegisteredTask(
                self.deps,
                fn,
                self.serializer,
                self.deserializer,
                lifespan,
                timeout=timeout,
                ttl=ttl,
                unique=unique,
            )
            self.registry[task.fn_name] = task
            return task

        return wrapped

    # TODO: add cron

    def deps(self) -> WorkerContext[WD]:
        if self._deps is None:
            raise StreaqError("Worker did not start correctly!")
        return WorkerContext(deps=self._deps, id=self.id, redis=self.redis)

    async def start(self):
        logger.info(f"Starting worker {self.id} for {len(self.registry)} functions...")
        async with self:
            await self.listen_stream()

    async def listen_stream(self):
        while True:
            messages = []
            async with self.bs:
                msgs = await self.redis.xreadgroup(
                    groupname=REDIS_GROUP,
                    consumername=self.id,
                    streams={REDIS_STREAM: ">"},
                    block=0,  # indefinitely
                    count=self.queue_fetch_limit,
                )
                for _, msg in msgs:
                    for msg_id, task in msg:
                        messages.append(
                            StreamMessage(
                                fn_name=task[b"fn_name"].decode(),
                                message_id=msg_id.decode(),
                                task_id=task[b"task_id"].decode(),
                                score=int(task[b"score"]),
                            )
                        )
            self.start_tasks(messages)

    def start_tasks(self, tasks: list[StreamMessage]):
        for task in tasks:
            asyncio.create_task(self.run_task(task.fn_name, task.task_id))

    async def run_task(self, fn_name: str, id: str):
        """
        Execute the registered task, then store the result in Redis.
        """
        # acquire semaphore
        async with self.bs:
            task = self.registry[fn_name]
            data = await self.redis.get(task.task_key(id))
            map = task.deserializer(data)
            result = await task.run(*map["a"], **map["k"])
            # notify anything waiting on results
            await self.redis.publish(self.channel_key(id), "Job completed!")
            if result is not None:
                if task.ttl != 0:
                    data = self.serializer(result)
                    await self.redis.set(task.result_key(id), data, ex=task.ttl)

    async def __aenter__(self):
        self._deps = await self.lifespan.__aenter__()
        # create consumer group if it doesn't exist
        with suppress(ResponseError):
            await self.redis.xgroup_create(
                name=REDIS_STREAM, groupname=REDIS_GROUP, id=0, mkstream=True
            )
        return self

    async def __aexit__(self, *exc):
        await self.lifespan.__aexit__(*exc)

    def channel_key(self, id: str) -> str:
        return f"{REDIS_CHANNEL}:{id}"

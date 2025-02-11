import asyncio
from functools import partial
import pickle
import signal
from collections import defaultdict
from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager, suppress
from datetime import timedelta
from signal import Signals
from time import time
from typing import Any, AsyncIterator, Concatenate, Generic
from uuid import uuid4

from redis.asyncio import Redis
from redis.commands.core import AsyncScript
from redis.exceptions import ResponseError
from redis.typing import EncodableT

from streaq import logger
from streaq.constants import (
    DEFAULT_QUEUE_NAME,
    DEFAULT_TTL,
    REDIS_CHANNEL,
    REDIS_GROUP,
    REDIS_HEALTH,
    REDIS_MESSAGE,
    REDIS_QUEUE,
    REDIS_RESULT,
    REDIS_STREAM,
    REDIS_TASK,
    REDIS_TIMEOUT,
)
from streaq.lua import register_scripts
from streaq.types import P, R, WD, StreamMessage, WrappedContext
from streaq.utils import StreaqError, log_redis_info, now_ms, to_seconds
from streaq.task import RegisteredTask, task_lifespan


@asynccontextmanager
async def do_nothing() -> AsyncIterator[None]:
    yield


class Worker(Generic[WD]):
    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        concurrency: int = 16,
        queue_name: str = DEFAULT_QUEUE_NAME,
        queue_fetch_limit: int | None = None,
        lifespan: Callable[[], AbstractAsyncContextManager] = do_nothing,
        serializer: Callable[[Any], EncodableT] = pickle.dumps,
        deserializer: Callable[[EncodableT], Any] = pickle.loads,  # type: ignore
        handle_signals: bool = False,
    ):
        self.redis = Redis.from_url(redis_url)
        self.concurrency = concurrency
        self.queue_name = queue_name
        self.queue_fetch_limit = queue_fetch_limit or concurrency * 4
        self.bs = asyncio.BoundedSemaphore(concurrency + 1)
        self.counters = defaultdict(lambda: 0)
        self.lifespan = lifespan()
        self.loop = asyncio.get_event_loop()
        self.task_lifespan = task_lifespan
        self._deps: WD | None = None
        self.scripts: dict[str, AsyncScript] = {}
        self.registry: dict[str, RegisteredTask] = {}
        self.id = uuid4().hex
        self.serializer = serializer
        self.deserializer = deserializer
        self.tasks: dict[str, asyncio.Task] = {}
        self.handle_signals = handle_signals
        if handle_signals:
            self._add_signal_handler(signal.SIGINT, self.handle_signal)
            self._add_signal_handler(signal.SIGTERM, self.handle_signal)

    @property
    def deps(self) -> WD:
        if self._deps is None:
            raise StreaqError("Worker did not start correctly!")
        return self._deps

    def build_context(
        self, registered_task: RegisteredTask, id: str, tries: int = 1
    ) -> WrappedContext[WD]:
        return WrappedContext(
            deps=self.deps,
            redis=self.redis,
            task_id=id,
            timeout=registered_task.timeout,
            tries=tries,
            ttl=registered_task.ttl,
            worker_id=self.id,
        )

    def task(
        self,
        cron: str | None = None,
        lifespan: Callable[
            [WrappedContext[WD]], AbstractAsyncContextManager[None]
        ] = task_lifespan,
        timeout: timedelta | int | None = None,
        ttl: timedelta | int | None = None,
        unique: bool = False,
    ):
        def wrapped(
            fn: Callable[Concatenate[WrappedContext[WD], P], Awaitable[R]],
        ) -> RegisteredTask[WD, P, R]:
            task = RegisteredTask(
                fn,
                self.serializer,
                self.deserializer,
                lifespan,
                timeout,
                ttl,
                unique,
                self,
            )
            self.registry[task.fn_name] = task
            return task

        return wrapped

    def run_sync(self) -> None:
        """
        Sync function to run the worker, finally closes worker connections.
        """
        self.main_task = self.loop.create_task(self.main())
        try:
            self.loop.run_until_complete(self.main_task)
        except asyncio.CancelledError:
            pass
        finally:
            self.loop.run_until_complete(self.close())

    async def run_async(self) -> None:
        self.main_task = self.loop.create_task(self.main())
        await self.main_task

    async def main(self):
        logger.info(f"Starting worker {self.id} for {len(self.registry)} functions...")
        # create consumer group if it doesn't exist
        with suppress(ResponseError):
            await self.redis.xgroup_create(
                name=self.queue_name + REDIS_STREAM,
                groupname=REDIS_GROUP,
                id=0,
                mkstream=True,
            )
        await log_redis_info(self.redis)
        async with self:
            done, pending = await asyncio.wait(
                [
                    self.listen_queue(),
                    self.listen_stream(),
                    self.consumer_cleanup(),
                    self.health_check(),
                ],
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            for task in done:
                task.result()
            await self.close()

    async def listen_queue(self) -> None:
        while True:
            job_ids = await self.redis.zrange(
                self.queue_name + REDIS_QUEUE,
                start="-inf",  # type: ignore
                end=now_ms(),
                num=self.queue_fetch_limit,
                offset=0,
                withscores=True,
                byscore=True,
            )
            async with self.redis.pipeline(transaction=False) as pipe:
                for job_id, score in job_ids:
                    expire_ms = int(score - now_ms() + DEFAULT_TTL)
                    if expire_ms <= 0:
                        expire_ms = DEFAULT_TTL

                    await self.scripts["publish_delayed_task"](
                        keys=[
                            self.queue_name,
                            self.queue_name + REDIS_STREAM,
                            REDIS_MESSAGE + job_id.decode(),
                        ],
                        args=[job_id.decode(), expire_ms],
                        client=pipe,
                    )

                await pipe.execute()

    async def consumer_cleanup(self) -> None:
        while True:
            await asyncio.sleep(300)  # every 5 minutes
            consumers_info = await self.redis.xinfo_consumers(
                self.queue_name + REDIS_STREAM,
                groupname=REDIS_GROUP,
            )
            for consumer_info in consumers_info:
                if self.id == consumer_info["name"].decode():
                    continue

                idle = timedelta(milliseconds=consumer_info["idle"]).seconds
                pending = consumer_info["pending"]

                if pending == 0 and idle > DEFAULT_TTL / 1000:
                    await self.redis.xgroup_delconsumer(
                        name=self.queue_name + REDIS_STREAM,
                        groupname=REDIS_GROUP,
                        consumername=consumer_info["name"],
                    )

    async def health_check(self):
        while True:
            await asyncio.sleep(300)  # every 5 minutes
            await self.record_health()

    async def listen_stream(self):
        while True:
            messages: list[StreamMessage] = []
            count = self.queue_fetch_limit - len(self)
            if len(self) < self.concurrency:
                messages.extend(await self.reclaim_expired_tasks(count))
            if count - len(messages) > 0:
                res = await self.redis.xreadgroup(
                    groupname=REDIS_GROUP,
                    consumername=self.id,
                    streams={self.queue_name + REDIS_STREAM: ">"},
                    block=1,
                    count=count,
                )
                for _, msgs in res:
                    messages.extend(
                        [
                            StreamMessage(
                                fn_name=msg[b"fn_name"].decode(),
                                message_id=msg_id.decode(),
                                task_id=msg[b"task_id"].decode(),
                                score=int(msg[b"score"]),
                            )
                            for msg_id, msg in msgs
                        ]
                    )
            # startup tasks
            for message in messages:
                coro = self.run_task(message.fn_name, message.task_id)
                self.tasks[message.task_id] = self.loop.create_task(coro)

    async def reclaim_expired_tasks(self, count: int) -> list[StreamMessage]:
        now = round(time())
        ids = await self.redis.zrangebyscore(self.queue_name + REDIS_TIMEOUT, 0, now)
        if not ids:
            return []
        if len(ids) > count:
            ids = ids[:count]
        msgs = await self.redis.xclaim(
            self.queue_name + REDIS_STREAM,
            groupname=REDIS_GROUP,
            consumername=self.id,
            min_idle_time=0,
            message_ids=ids,
        )
        return [
            StreamMessage(
                fn_name=msg[b"fn_name"].decode(),
                message_id=msg_id.decode(),
                task_id=msg[b"task_id"].decode(),
                score=int(msg[b"score"]),
            )
            for msg_id, msg in msgs
        ]

    async def run_task(self, fn_name: str, id: str):
        """
        Execute the registered task, then store the result in Redis.
        """
        # acquire semaphore
        async with self.bs:
            task = self.registry[fn_name]
            ctx = self.build_context(task, id)
            raw = await self.redis.get(self.queue_name + REDIS_TASK + id)
            data = task.deserializer(raw)
            timeout = (
                "inf"
                if task.timeout is None
                else round(time()) + to_seconds(task.timeout) + 1
            )
            async with task.lifespan(ctx):
                try:
                    await self.redis.zadd(
                        self.queue_name + REDIS_TIMEOUT, {id: timeout}
                    )
                    result = await asyncio.wait_for(
                        task.fn(ctx, *data[0], **data[1]),
                        to_seconds(task.timeout) if task.timeout is not None else None,
                    )
                    async with self.redis.pipeline(transaction=True) as pipe:
                        # notify anything waiting on results
                        pipe.publish(self.queue_name + REDIS_CHANNEL, id)
                        if result is not None:
                            if task.ttl != 0:
                                data = task.serializer(result)
                                pipe.set(
                                    self.queue_name + REDIS_RESULT + id,
                                    data,
                                    ex=task.ttl,
                                )
                except Exception:
                    pass
                else:
                    pass
                finally:
                    del self.tasks[id]

    def _add_signal_handler(self, signum: int, handler: Callable[[int], None]) -> None:
        try:
            self.loop.add_signal_handler(signum, partial(handler, signum))
        except NotImplementedError:
            logger.error("Windows does not support handling signals for an event loop!")

    def handle_signal(self, signum: int) -> None:
        signal = Signals(signum)
        logger.info(
            f"Shutdown on {signal.name}: "
            + repr(self)[1:-1].replace("running", "interrupted")
        )
        for t in self.tasks.values():
            if not t.done():
                t.cancel()
        self.main_task.cancel()

    async def queue_size(self) -> int:
        """
        Returns the number of tasks currently queued in Redis.
        """
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.xlen(self.queue_name + REDIS_STREAM)
            pipe.zcard(self.queue_name + REDIS_QUEUE)
            stream_size, queue_size = await pipe.execute()

        return stream_size + queue_size

    async def record_health(self) -> None:
        health = repr(self)[1:-1]
        logger.info(health)
        await self.redis.hset(  # type: ignore
            self.queue_name + REDIS_HEALTH,
            self.id,
            health,
        )
        await log_redis_info(self.redis)

    async def close(self) -> None:
        if not self.handle_signals:
            self.handle_signal(signal.SIGUSR1)
        await asyncio.gather(*self.tasks.values())
        # delete health hash
        await self.redis.delete(self.queue_name + REDIS_HEALTH)
        await self.redis.close(close_connection_pool=True)

    async def __aenter__(self):
        # register lua scripts
        self.scripts.update(register_scripts(self.redis))
        # user-defined deps
        self._deps = await self.lifespan.__aenter__()
        return self

    async def __aexit__(self, *exc):
        await self.lifespan.__aexit__(*exc)

    def __len__(self):
        """
        Returns the number of tasks running in the worker currently.
        """
        return sum(not t.done() for t in self.tasks.values())

    def __repr__(self) -> str:
        return (
            f"<Worker {self.id} completed={self.counters['completed']} failed="
            f"{self.counters['failed']} retried={self.counters['retried']} running="
            f"{len(self)}>"
        )

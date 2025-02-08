import asyncio
from collections import defaultdict
import pickle
from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager, suppress
from datetime import timedelta
from functools import partial
from signal import Signals
from typing import Any, AsyncIterator, Concatenate, Generic
from uuid import uuid4

from redis.asyncio import Redis
from redis.commands.core import AsyncScript
from redis.exceptions import ResponseError
from redis.typing import EncodableT

from streaq import logger
from streaq.constants import (
    DEFAULT_QUEUE_NAME,
    REDIS_CHANNEL,
    REDIS_GROUP,
    REDIS_QUEUE,
    REDIS_RESULT,
    REDIS_STREAM,
    REDIS_TASK,
)
from streaq.lua import register_scripts
from streaq.types import P, R, WD, StreamMessage, WrappedContext
from streaq.utils import StreaqError, to_seconds
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
        self.tasks: list[asyncio.Task] = []

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

    # TODO: add cron

    def task(
        self,
        lifespan: Callable[
            [WrappedContext[WD]], AbstractAsyncContextManager[None]
        ] = task_lifespan,
        timeout: timedelta | int | None = None,
        ttl: timedelta | int | None = None,
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
                self,
            )
            self.registry[task.fn_name] = task
            return task

        return wrapped

    """
    TODO: implement
    """

    def run_sync(self) -> None:
        """
        Sync function to run the worker, finally closes worker connections.
        """
        self.main_task = self.loop.create_task(self.run())
        try:
            self.loop.run_until_complete(self.main_task)
        except asyncio.CancelledError:
            # happens on shutdown, fine
            pass

    async def run(self):
        logger.info(f"Starting worker {self.id} for {len(self.registry)} functions...")
        async with self:
            try:
                await self.listen_stream()
            except asyncio.CancelledError:
                pass
            finally:
                await asyncio.gather(*self.tasks)
                await self.redis.close(close_connection_pool=True)

    async def listen_stream(self):
        while True:
            messages = []
            async with self.bs:
                msgs = await self.redis.xreadgroup(
                    groupname=REDIS_GROUP,
                    consumername=self.id,
                    streams={self.queue_name + REDIS_STREAM: ">"},
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
            self.create_tasks(messages)

    def create_tasks(self, tasks: list[StreamMessage]):
        for task in tasks:
            coro = self.run_task(task.fn_name, task.task_id)
            self.tasks.append(self.loop.create_task(coro))

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
            async with task.lifespan(ctx):
                result = await asyncio.wait_for(
                    task.fn(ctx, *data[0], **data[1]),
                    to_seconds(task.timeout) if task.timeout else None,
                )
                async with self.redis.pipeline(transaction=True) as pipe:
                    # notify anything waiting on results
                    pipe.publish(self.queue_name + REDIS_CHANNEL, id)
                    if result is not None:
                        if task.ttl != 0:
                            data = task.serializer(result)
                            pipe.set(
                                self.queue_name + REDIS_RESULT + id, data, ex=task.ttl
                            )

    async def queue_size(self) -> int:
        """
        Returns the number of tasks currently queued in Redis.
        """
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.xlen(self.queue_name + REDIS_STREAM)
            pipe.zcount(self.queue_name + REDIS_QUEUE, "-inf", "+inf")
            stream_size, queue_size = await pipe.execute()

        return stream_size + queue_size

    async def __aenter__(self):
        self.scripts.update(register_scripts(self.redis))
        self._deps = await self.lifespan.__aenter__()
        # create consumer group if it doesn't exist
        with suppress(ResponseError):
            await self.redis.xgroup_create(
                name=self.queue_name + REDIS_STREAM,
                groupname=REDIS_GROUP,
                id=0,
                mkstream=True,
            )
        return self

    async def __aexit__(self, *exc):
        await self.lifespan.__aexit__(*exc)

    def __len__(self):
        """
        Returns the number of tasks running in the worker currently.
        """
        return sum(not t.done() for t in self.tasks)

    def __repr__(self) -> str:
        return (
            f"<Worker done={self.counters['done']} failed={self.counters['failed']} "
            f"retried={self.counters['retried']} running={len(self)}>"
        )

    """
    TODO: implement
    """

    def handle_signal(self, signum: Signals) -> None:
        sig = Signals(signum)
        logger.info(
            "shutdown on %s ◆ %d jobs complete ◆ %d failed ◆ %d retries ◆ %d running to cancel",
            sig.name,
            self.counters["complete"],
            self.counters["failed"],
            self.counters["retried"],
            len(self.tasks),
        )
        for t in self.tasks:
            if not t.done():
                t.cancel()
        # self.main_task and self.main_task.cancel()
        # self.on_stop and self.on_stop(sig)

    def _add_signal_handler(
        self, signum: Signals, handler: Callable[[Signals], None]
    ) -> None:
        try:
            self.loop.add_signal_handler(signum, partial(handler, signum))
        except NotImplementedError:
            logger.debug(
                "Windows does not support adding a signal handler to an event loop!"
            )

    async def record_health(self) -> None:
        now_ts = time()
        if (now_ts - self._last_health_check) < self.health_check_interval:
            return
        self._last_health_check = now_ts
        pending_tasks = sum(not t.done() for t in self.tasks.values())
        queued = await self.redis.zcard(REDIS_QUEUE)
        info = (
            f"{datetime.now():%b-%d %H:%M:%S} j_complete={self.jobs_complete} j_failed={self.jobs_failed} "
            f"j_retried={self.jobs_retried} j_ongoing={pending_tasks} queued={queued}"
        )
        await self.pool.psetex(  # type: ignore[no-untyped-call]
            self.health_check_key,
            int((self.health_check_interval + 1) * 1000),
            info.encode(),
        )
        log_suffix = info[info.index("j_complete=") :]
        if self._last_health_check_log and log_suffix != self._last_health_check_log:
            logger.info("recording health: %s", info)
            self._last_health_check_log = log_suffix
        elif not self._last_health_check_log:
            self._last_health_check_log = log_suffix

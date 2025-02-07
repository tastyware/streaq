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

from croniter import croniter
from redis.asyncio import Redis
from redis.commands.core import AsyncScript
from redis.exceptions import ResponseError
from redis.typing import EncodableT

from streaq import REDIS_CHANNEL, REDIS_QUEUE, logger, REDIS_GROUP, REDIS_STREAM
from streaq.lua import register_scripts
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

    def deps(self) -> WorkerContext[WD]:
        if self._deps is None:
            raise StreaqError("Worker did not start correctly!")
        return WorkerContext(
            deps=self._deps, id=self.id, redis=self.redis, scripts=self.scripts
        )

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

    async def start(self):
        logger.info(f"Starting worker {self.id} for {len(self.registry)} functions...")
        async with self:
            try:
                await self.listen_stream()
            except asyncio.CancelledError:
                pass
            finally:
                # await asyncio.gather(*self.tasks)
                await self.redis.close(close_connection_pool=True)

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
            self.run_tasks(messages)

    def run_tasks(self, tasks: list[StreamMessage]):
        for task in tasks:
            self.tasks.append(
                self.loop.create_task(self.run_task(task.fn_name, task.task_id))
            )

    async def run_task(self, fn_name: str, id: str):
        """
        Execute the registered task, then store the result in Redis.
        """
        # acquire semaphore
        async with self.bs:
            task = self.registry[fn_name]
            data = await self.redis.get(task.task_key(id))
            info = task.deserializer(data)
            result = await task.run(*info[0], **info[1])
            # notify anything waiting on results
            await self.redis.publish(self.channel_key(id), "Job completed!")
            if result is not None:
                if task.ttl != 0:
                    data = self.serializer(result)
                    await self.redis.set(task.result_key(id), data, ex=task.ttl)

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

    def _add_signal_handler(
        self, signum: Signals, handler: Callable[[Signals], None]
    ) -> None:
        try:
            self.loop.add_signal_handler(signum, partial(handler, signum))
        except NotImplementedError:
            logger.debug(
                "Windows does not support adding a signal handler to an event loop!"
            )

    def __len__(self):
        """
        Returns the number of tasks currently running.
        """
        return sum([v for v in self.counters.values()]) + len(self.tasks)

    async def queue_size(self) -> int:
        """
        Returns the number of tasks currently queued in Redis.
        """
        async with self.redis.pipeline(transaction=True) as pipe:
            await pipe.xlen(REDIS_STREAM)
            await pipe.zcount(REDIS_QUEUE, "-inf", "+inf")
            stream_size, queue_size = await pipe.execute()

        return stream_size + queue_size

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

    async def __aenter__(self):
        self.scripts.update(register_scripts(self.redis))
        self._deps = await self.lifespan.__aenter__()
        # create consumer group if it doesn't exist
        with suppress(ResponseError):
            await self.redis.xgroup_create(
                name=REDIS_STREAM, groupname=REDIS_GROUP, id=0, mkstream=True
            )
        return self

    async def __aexit__(self, *exc):
        await self.lifespan.__aexit__(*exc)

    def __repr__(self) -> str:
        return (
            f"<Worker complete={self.counters['complete']} failed={self.counters['failed']} retried={self.counters['retried']} "
            f"running={sum(not t.done() for t in self.tasks)}>"
        )

    def channel_key(self, id: str) -> str:
        return f"{REDIS_CHANNEL}:{id}"

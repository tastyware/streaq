import asyncio
from functools import partial
import pickle
import signal
from collections import defaultdict
from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager, suppress
from datetime import timedelta, timezone, tzinfo
from signal import Signals
from time import time
from typing import Any, AsyncIterator, Concatenate, Generic
from uuid import uuid4

from crontab import CronTab
from redis.asyncio import Redis
from redis.commands.core import AsyncScript
from redis.exceptions import ResponseError
from redis.typing import EncodableT

from streaq import logger
from streaq.constants import (
    DEFAULT_QUEUE_NAME,
    DEFAULT_TTL,
    REDIS_ABORT,
    REDIS_CHANNEL,
    REDIS_GROUP,
    REDIS_HEALTH,
    REDIS_LOCK,
    REDIS_MESSAGE,
    REDIS_QUEUE,
    REDIS_RESULT,
    REDIS_RETRY,
    REDIS_RUNNING,
    REDIS_STREAM,
    REDIS_TASK,
    REDIS_TIMEOUT,
)
from streaq.lua import register_scripts
from streaq.types import P, R, WD, StreamMessage, WrappedContext
from streaq.utils import StreaqError, StreaqRetry, now_ms, to_seconds
from streaq.task import RegisteredCron, RegisteredTask, task_lifespan


@asynccontextmanager
async def do_nothing() -> AsyncIterator[None]:
    yield


class Worker(Generic[WD]):
    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        concurrency: int = 16,
        queue_name: str = DEFAULT_QUEUE_NAME,
        group_name: str = REDIS_GROUP,
        queue_fetch_limit: int | None = None,
        lifespan: Callable[[], AbstractAsyncContextManager] = do_nothing,
        serializer: Callable[[Any], EncodableT] = pickle.dumps,
        deserializer: Callable[[EncodableT], Any] = pickle.loads,  # type: ignore
        burst: bool = False,
        tz: tzinfo = timezone.utc,
        handle_signals: bool = False,
        health_check_interval: timedelta | int = 300,
    ):
        self.redis = Redis.from_url(redis_url)
        self.concurrency = concurrency
        self.queue_name = queue_name
        self.group_name = group_name
        self.queue_fetch_limit = queue_fetch_limit or concurrency * 4
        self.bs = asyncio.BoundedSemaphore(concurrency + 1)
        self.counters = defaultdict(lambda: 0)
        self.lifespan = lifespan()
        self.loop = asyncio.get_event_loop()
        self.task_lifespan = task_lifespan
        self._deps: WD | None = None
        self.scripts: dict[str, AsyncScript] = {}
        self.registry: dict[str, RegisteredTask] = {}
        self.cron_jobs: dict[str, RegisteredCron] = {}
        self.id = uuid4().hex
        self.serializer = serializer
        self.deserializer = deserializer
        self.tasks: dict[str, asyncio.Task] = {}
        self.handle_signals = handle_signals
        self.health_check_interval = health_check_interval
        self.tz: tzinfo = tz
        self.aborting_tasks: set[str] = set()
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

    def cron(
        self,
        tab: str,
        lifespan: Callable[
            [WrappedContext[WD]], AbstractAsyncContextManager[None]
        ] = task_lifespan,
        max_tries: int | None = 3,
        timeout: timedelta | int | None = None,
        ttl: timedelta | int | None = None,
        unique: bool = True,
    ):
        def wrapped(
            fn: Callable[[WrappedContext[WD]], Awaitable[R]],
        ) -> RegisteredCron[WD, R]:
            task = RegisteredCron(
                fn,
                self.serializer,
                self.deserializer,
                lifespan,
                max_tries,
                CronTab(tab),
                timeout,
                ttl,
                unique,
                self,
            )
            self.cron_jobs[task.fn_name] = task
            return task

        return wrapped

    def task(
        self,
        lifespan: Callable[
            [WrappedContext[WD]], AbstractAsyncContextManager[None]
        ] = task_lifespan,
        max_tries: int | None = 3,
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
                max_tries,
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
                groupname=self.group_name,
                id=0,
                mkstream=True,
            )
        async with self:
            done, pending = await asyncio.wait(
                [
                    asyncio.ensure_future(self.listen_queue()),
                    asyncio.ensure_future(self.listen_stream()),
                    asyncio.ensure_future(self.consumer_cleanup()),
                    asyncio.ensure_future(self.health_check()),
                    asyncio.ensure_future(self.redis_health_check()),
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
                start=0,
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
                groupname=self.group_name,
            )
            for consumer_info in consumers_info:
                if self.id == consumer_info["name"].decode():
                    continue

                idle = timedelta(milliseconds=consumer_info["idle"]).seconds
                pending = consumer_info["pending"]

                if pending == 0 and idle > DEFAULT_TTL / 1000:
                    await self.redis.xgroup_delconsumer(
                        name=self.queue_name + REDIS_STREAM,
                        groupname=self.group_name,
                        consumername=consumer_info["name"],
                    )

    async def listen_stream(self):
        while True:
            messages: list[StreamMessage] = []
            count = self.queue_fetch_limit - len(self)
            if len(self) < self.concurrency:
                expired = await self.handle_expired_tasks(count)
                count -= len(expired)
                messages.extend(expired)
            if count > 0:
                res = await self.redis.xreadgroup(
                    groupname=self.group_name,
                    consumername=self.id,
                    streams={self.queue_name + REDIS_STREAM: ">"},
                    block=500,
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
            # start new tasks and cleanup aborted ones
            await self.start_tasks(messages)
            await self.cancel_tasks()
            for task_id, task in list(self.tasks.items()):
                if task.done():
                    del self.tasks[task_id]
                    # propagate error
                    task.result()

    async def start_tasks(self, messages: list[StreamMessage]) -> None:
        for message in messages:
            coro = self.run_task(message.fn_name, message.task_id, message.message_id)
            self.tasks[message.task_id] = self.loop.create_task(coro)

    async def cancel_tasks(self) -> None:
        """
        Go through job_ids in the aborted tasks set and cancel those tasks.
        """
        set_name = self.queue_name + REDIS_ABORT
        aborted_ids = await self.redis.smembers(set_name)  # type: ignore
        aborted: set[str] = set()
        for task_id_bytes in aborted_ids:
            task_id = task_id_bytes.decode()
            if task_id in self.tasks:
                self.tasks[task_id].cancel()
                aborted.add(task_id)

        if aborted:
            self.aborting_tasks.update(aborted)
            await self.redis.srem(set_name, *aborted)  # type: ignore

    async def handle_expired_tasks(self, count: int) -> list[StreamMessage]:
        now = round(time())
        ids = await self.redis.zrangebyscore(self.queue_name + REDIS_TIMEOUT, 0, now)
        if not ids:
            return []
        if len(ids) > count:
            ids = ids[:count]
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.zrem(self.queue_name + REDIS_TIMEOUT, ids)
            pipe.xclaim(
                self.queue_name + REDIS_STREAM,
                groupname=self.group_name,
                consumername=self.id,
                min_idle_time=0,
                message_ids=ids,
            )
            pipe.sadd(self.queue_name + REDIS_ABORT, ids)
            _, msgs = await pipe.execute()
        return [
            StreamMessage(
                fn_name=msg[b"fn_name"].decode(),
                message_id=msg_id.decode(),
                task_id=msg[b"task_id"].decode(),
                score=int(msg[b"score"]),
            )
            for msg_id, msg in msgs
        ]

    async def run_task(self, fn_name: str, task_id: str, message_id: str):
        """
        Execute the registered task, then store the result in Redis.
        """
        key = lambda mid: self.queue_name + mid + id
        # acquire semaphore
        async with self.bs:
            start_time = now_ms()
            task = self.registry[fn_name]
            ctx = self.build_context(task, task_id)
            timeout = (
                "inf"
                if task.timeout is None
                else round(time()) + to_seconds(task.timeout) + 1
            )
            async with self.redis.pipeline(transaction=True) as pipe:
                pipe.get(key(REDIS_TASK))
                pipe.incr(key(REDIS_RETRY))
                pipe.expire(key(REDIS_RETRY), DEFAULT_TTL // 1000)
                pipe.srem(self.queue_name + REDIS_ABORT, task_id)
                pipe.zadd(self.queue_name + REDIS_TIMEOUT, {task_id: timeout})
                raw, task_try, _, abort = await pipe.execute()

            async def handle_failure(exc: BaseException) -> None:
                self.counters["failed"] += 1
                data = {
                    "s": False,
                    "r": exc,
                    "st": start_time,
                    "ft": now_ms(),
                }
                try:
                    raw = self.serializer(data)
                    await asyncio.shield(
                        self.finish_failed_task(task_id, message_id, raw, task.ttl)
                    )
                except Exception as e:
                    raise StreaqError(
                        f"Failed to serialize result for task {id}!"
                    ) from e

            if not raw:
                logger.warning(f"Task {task_id} expired.")
                return await handle_failure(StreaqError("Task execution failed!"))

            try:
                data = task.deserializer(raw)
            except Exception as e:
                logger.exception(f"Failed to deserialize task {task_id}: {e}")
                return await handle_failure(e)

            if abort:
                logger.info(f"Task {task_id} aborted prior to starting.")
                return await handle_failure(asyncio.CancelledError())

            if task.max_tries and task_try > task.max_tries:
                logger.warning(f"Max retry attempts reached for task {task_id}!")
                self.counters["failed"] += 1
                return await handle_failure(
                    StreaqError(f"Max retry attempts reached for task {task_id}!")
                )

            async with task.lifespan(ctx):
                try:
                    logger.info(
                        f"Starting task {task_id} for function {task.fn_name}..."
                    )
                    result = await asyncio.wait_for(
                        task.fn(ctx, *data["a"], **data["k"]),
                        to_seconds(task.timeout) if task.timeout is not None else None,
                    )
                except StreaqRetry as e:
                    logger.info(f"Retrying task {task_id} in {e.delay} seconds...")
                except (Exception, asyncio.CancelledError) as e:
                    finish_time = now_ms()
                    run_time = (start_time - finish_time) / 1000
                else:
                    pass
                finally:
                    pass

    async def finish_task(
        self,
        task_id: str,
        message_id: str,
        score: int,
        finish: bool,
        result_data: bytes | None,
        ttl: timedelta | int | None,
        incr_score: int | None,
    ) -> None:
        key = lambda mid: self.queue_name + mid + task_id
        async with self.redis.pipeline(transaction=True) as pipe:
            delete_keys = set()
            stream_key = self.queue_name + REDIS_STREAM
            delete_keys.add(key(REDIS_RUNNING))
            pipe.xack(
                stream_key,
                self.group_name,
                message_id,
            )
            pipe.xdel(stream_key, message_id)
            pipe.publish(self.queue_name + REDIS_CHANNEL, task_id)

            if finish:
                if result_data:
                    pipe.set(key(REDIS_RESULT), result_data, ex=ttl)
                delete_keys.add(key(REDIS_RETRY))
                delete_keys.add(key(REDIS_TASK))
                delete_keys.add(key(REDIS_MESSAGE))
                pipe.zrem(self.queue_name + REDIS_TIMEOUT, task_id)
                pipe.srem(self.queue_name + REDIS_ABORT, task_id)
            elif incr_score:
                delete_keys.add(key(REDIS_MESSAGE))
                pipe.zadd(self.queue_name, {task_id: score + incr_score})
            else:
                job_message_id_expire = score - now_ms() + DEFAULT_TTL
                await self.scripts["publish_task"](
                    keys=[stream_key, key(REDIS_MESSAGE)],
                    args=[task_id, score, job_message_id_expire],
                    client=pipe,
                )
            if delete_keys:
                pipe.delete(*delete_keys)
            await pipe.execute()

    async def finish_failed_task(
        self,
        task_id: str,
        message_id: str,
        result_data: EncodableT,
        ttl: timedelta | int | None,
    ) -> None:
        key = lambda mid: self.queue_name + mid + task_id
        stream_key = self.queue_name + REDIS_STREAM
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.delete(
                key(REDIS_RETRY),
                key(REDIS_RUNNING),
                key(REDIS_TASK),
                key(REDIS_MESSAGE),
            )
            pipe.publish(self.queue_name + REDIS_CHANNEL, task_id)
            pipe.zrem(self.queue_name + REDIS_ABORT, task_id)
            pipe.xack(stream_key, self.group_name, message_id)
            pipe.xdel(stream_key, message_id)
            if result_data is not None:
                pipe.set(key(REDIS_RESULT), result_data, ex=ttl)
            await pipe.execute()

    async def redis_health_check(self):
        timeout = to_seconds(self.health_check_interval)
        async with self.redis.lock(
            self.queue_name + REDIS_LOCK,
            sleep=timeout + 1,
            timeout=timeout,
        ):
            while True:
                await asyncio.sleep(timeout)
                async with self.redis.pipeline(transaction=False) as pipe:
                    pipe.info(section="Server")
                    pipe.info(section="Memory")
                    pipe.info(section="Clients")
                    pipe.dbsize()
                    pipe.xlen(self.queue_name + REDIS_STREAM)
                    pipe.zcard(self.queue_name + REDIS_QUEUE)
                    (
                        info_server,
                        info_memory,
                        info_clients,
                        key_count,
                        stream_size,
                        queue_size,
                    ) = await pipe.execute()

                redis_version = info_server.get("redis_version", "?")
                mem_usage = info_memory.get("used_memory_human", "?")
                clients_connected = info_clients.get("connected_clients", "?")
                health = (
                    f"redis_version={redis_version} "
                    f"mem_usage={mem_usage} "
                    f"clients_connected={clients_connected} "
                    f"db_keys={key_count} "
                    f"queued={stream_size} "
                    f"scheduled={queue_size}"
                )
                logger.info(health)
                await self.redis.hset(  # type: ignore
                    self.queue_name + REDIS_HEALTH, "redis", health
                )

    async def health_check(self):
        while True:
            await asyncio.sleep(to_seconds(self.health_check_interval))
            health = repr(self)[1:-1]
            logger.info(health)
            await self.redis.hset(  # type: ignore
                self.queue_name + REDIS_HEALTH, self.id, health
            )

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
            f"<Worker {self.id}: completed={self.counters['completed']} failed="
            f"{self.counters['failed']} retried={self.counters['retried']} running="
            f"{len(self)}>"
        )

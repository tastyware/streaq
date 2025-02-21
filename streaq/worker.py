import asyncio
import pickle
from collections import defaultdict
from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager, suppress
from datetime import timedelta, timezone, tzinfo
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
from streaq.utils import StreaqError, now_ms, to_ms, to_seconds
from streaq.task import RegisteredCron, RegisteredTask


class StreaqRetry(StreaqError):
    """
    An exception you can manually raise in your tasks to make sure the task
    is retried.

    :param msg: error message to show
    :param delay:
        amount of time to wait before retrying the task; if none, will be the
        square of the number of attempts in seconds
    """

    def __init__(self, msg: str, delay: timedelta | int | None = None):
        super().__init__(msg)
        self.delay = delay


@asynccontextmanager
async def task_lifespan(ctx: WrappedContext[WD]) -> AsyncIterator[None]:
    yield


@asynccontextmanager
async def worker_lifespan() -> AsyncIterator[None]:
    yield


class Worker(Generic[WD]):
    """
    Worker object that fetches and executes tasks from a queue.

    :param redis_url: connection URI for Redis
    :param concurrency: number of tasks the worker can run simultaneously
    :param queue_name: name of queue in Redis
    :param group_name: name of consumer group for the Redis stream
    :param queue_fetch_limit: max number of tasks to prefetch from Redis
    :param task_lifespan: async context manager that will wrap tasks
    :param worker_lifespan:
        async context manager that wraps worker execution and provides task
        dependencies
    :param serializer: function to serialize task data for Redis
    :param deserializer: function to deserialize task data from Redis
    :param tz: timezone to use for cron jobs
    :param health_check_interval: frequency to print health information
    """

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        concurrency: int = 16,
        queue_name: str = DEFAULT_QUEUE_NAME,
        group_name: str = REDIS_GROUP,
        queue_fetch_limit: int | None = None,
        task_lifespan: Callable[
            [WrappedContext[WD]], AbstractAsyncContextManager[None]
        ] = task_lifespan,
        worker_lifespan: Callable[
            [], AbstractAsyncContextManager[WD]
        ] = worker_lifespan,
        serializer: Callable[[Any], EncodableT] = pickle.dumps,
        deserializer: Callable[[EncodableT], Any] = pickle.loads,  # type: ignore
        tz: tzinfo = timezone.utc,
        health_check_interval: timedelta | int = 300,
    ):
        # TODO: optimizations, __slots__?
        #: Redis connection
        self.redis = Redis.from_url(redis_url)
        self.concurrency = concurrency
        self.queue_name = queue_name
        self.group_name = group_name
        self.queue_fetch_limit = queue_fetch_limit or concurrency * 2
        #: semaphore controlling concurrency
        self.bs = asyncio.BoundedSemaphore(concurrency + 1)
        #: mapping of type of task -> number of tasks of that type
        #: eg {"completed": 4, "failed": 1, "retried": 0}
        self.counters = defaultdict(int)
        self.worker_lifespan = worker_lifespan()
        #: event loop for running tasks
        self.loop = asyncio.get_event_loop()
        self.task_lifespan = task_lifespan
        self._deps: WD | None = None
        #: Redis scripts for common operations
        self.scripts: dict[str, AsyncScript] = {}
        #: mapping of task name -> task wrapper
        self.registry: dict[str, RegisteredCron | RegisteredTask] = {}
        #: mapping of task name -> cron wrapper
        self.cron_jobs: dict[str, RegisteredCron] = {}
        #: mapping of task name -> next execution time in ms
        self.cron_schedule: dict[str, int] = {}
        #: unique ID of worker
        self.id = uuid4().hex
        self.serializer = serializer
        self.deserializer = deserializer
        #: mapping of task ID -> asyncio Task
        self.tasks: dict[str, asyncio.Task] = {}
        self.health_check_interval = health_check_interval
        self.tz: tzinfo = tz
        #: set of tasks currently scheduled for abortion
        self.aborting_tasks: set[str] = set()
        self._block_new_tasks: bool = False

    @property
    def deps(self) -> WD:
        if self._deps is None:
            raise StreaqError("Worker did not start correctly!")
        return self._deps

    def build_context(
        self, registered_task: RegisteredCron | RegisteredTask, id: str, tries: int = 1
    ) -> WrappedContext[WD]:
        """
        Creates the context for a task to be run given task metadata
        """
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
        max_tries: int | None = 3,
        run_at_startup: bool = False,
        timeout: timedelta | int | None = None,
        ttl: timedelta | int | None = 0,
        unique: bool = True,
    ):
        """
        Registers a task to be run at regular intervals as specified.

        :param tab:
            crontab for scheduling, follows the specification
            `here <https://github.com/josiahcarlson/parse-crontab?tab=readme-ov-file#description>`_.
        :param max_tries:
            number of times to retry the task should it fail during execution
        :param run_at_startup:
            whether to run the task at worker startup, regardless of crontab content
        :param timeout: time after which to abort the task, if None will never time out
        :param ttl: time to store results in Redis, if None will never expire
        :param unique: whether multiple instances of the task can exist simultaneously
        """

        def wrapped(
            fn: Callable[[WrappedContext[WD]], Awaitable[R]],
        ) -> RegisteredCron[WD, R]:
            task = RegisteredCron(
                fn,
                task_lifespan,
                max_tries,
                run_at_startup,
                CronTab(tab),
                timeout,
                ttl,
                unique,
                self,
            )
            self.cron_jobs[task.fn_name] = task
            self.registry[task.fn_name] = task
            return task

        return wrapped

    def task(
        self,
        max_tries: int | None = 3,
        timeout: timedelta | int | None = None,
        ttl: timedelta | int | None = timedelta(minutes=5),
        unique: bool = False,
    ):
        """
        Registers a task with the worker which can later be enqueued by the user.

        :param max_tries:
            number of times to retry the task should it fail during execution
        :param timeout: time after which to abort the task, if None will never time out
        :param ttl: time to store results in Redis, if None will never expire
        :param unique: whether multiple instances of the task can exist simultaneously
        """

        def wrapped(
            fn: Callable[Concatenate[WrappedContext[WD], P], Awaitable[R]],
        ) -> RegisteredTask[WD, P, R]:
            task = RegisteredTask(
                fn,
                task_lifespan,
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

    async def main(self):
        """
        Main loop for handling worker tasks, aggregates and runs other tasks
        """
        logger.info(f"starting worker {self.id} for {len(self)} functions")
        # create consumer group if it doesn't exist
        with suppress(ResponseError):
            await self.redis.xgroup_create(
                name=self.queue_name + REDIS_STREAM,
                groupname=self.group_name,
                id=0,
                mkstream=True,
            )
        # set schedule for cron jobs
        ts = now_ms()
        for name, cron_job in self.cron_jobs.items():
            if cron_job.run_at_startup and name not in self.cron_schedule:
                self.cron_schedule[name] = ts
            else:
                self.cron_schedule[name] = cron_job.next()
        # run loops
        async with self:
            await asyncio.gather(
                self.listen_queue(),
                self.listen_stream(),
                self.consumer_cleanup(),
                self.health_check(),
                self.redis_health_check(),
            )

    async def consumer_cleanup(self) -> None:
        """
        Infrequently check for offline consumers to delete
        """
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

    async def listen_queue(self) -> None:
        """
        Periodically check the future queue (sorted set) for tasks, adding
        them to the live queue (stream) when ready, as well as adding cron
        jobs to the live queue when ready.
        """
        set_name = self.queue_name + REDIS_ABORT
        while not self._block_new_tasks:
            start_time = time()
            async with self.redis.pipeline(transaction=True) as pipe:
                pipe.zrange(
                    self.queue_name + REDIS_QUEUE,
                    start=0,
                    end=now_ms(),
                    num=self.queue_fetch_limit,
                    offset=0,
                    withscores=True,
                    byscore=True,
                )
                pipe.smembers(set_name)
                task_ids, aborted_ids = await pipe.execute()
            async with self.redis.pipeline(transaction=False) as pipe:
                for task_id, score in task_ids:
                    expire_ms = int(score - now_ms() + DEFAULT_TTL)
                    if expire_ms <= 0:
                        expire_ms = DEFAULT_TTL

                    decoded = task_id.decode()
                    await self.scripts["publish_delayed_task"](
                        keys=[
                            self.queue_name + REDIS_QUEUE,
                            self.queue_name + REDIS_STREAM,
                            self.queue_name + REDIS_MESSAGE + decoded,
                        ],
                        args=[decoded, expire_ms],
                        client=pipe,
                    )
                # Go through task_ids in the aborted tasks set and cancel those tasks.
                aborted: set[str] = set()
                for task_id_bytes in aborted_ids:
                    task_id = task_id_bytes.decode()
                    if task_id in self.tasks:
                        self.tasks[task_id].cancel()
                        aborted.add(task_id)
                if aborted:
                    self.aborting_tasks.update(aborted)
                    pipe.srem(set_name, *aborted)
                await pipe.execute()

            # cron jobs
            futures = set()
            ts = now_ms()
            for name, cron_job in self.cron_jobs.items():
                diff = max(0, self.cron_schedule[name] - ts)
                if diff < 1000:
                    futures.add(
                        cron_job.enqueue().start(delay=timedelta(milliseconds=diff))
                    )
                    self.cron_schedule[name] = cron_job._upcoming()
            if futures:
                await asyncio.gather(*futures)

            # cleanup aborted tasks
            for task_id, task in list(self.tasks.items()):
                if task.done():
                    del self.tasks[task_id]
                    # propagate error
                    task.result()

            delay = time() - start_time
            if delay < 0.5:
                await asyncio.sleep(0.5 - delay)

    async def listen_stream(self):
        """
        Listen for new tasks from the stream, and periodically check for tasks
        that were never XACK'd but have timed out to reclaim.
        """
        while not self._block_new_tasks:
            messages: list[StreamMessage] = []
            active_tasks = self.active
            count = self.queue_fetch_limit - active_tasks
            if active_tasks < self.concurrency:
                expired = await self._get_idle_tasks(count)
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
                                message_id=msg_id.decode(),
                                task_id=msg[b"task_id"].decode(),
                                score=int(msg[b"score"]),
                            )
                            for msg_id, msg in msgs
                        ]
                    )
            # start new tasks
            for message in messages:
                coro = self.run_task(message.task_id, message.message_id)
                self.tasks[message.task_id] = self.loop.create_task(coro)

    async def _get_idle_tasks(self, count: int) -> list[StreamMessage]:
        ids = await self.redis.zrangebyscore(
            self.queue_name + REDIS_TIMEOUT, 0, now_ms()
        )
        if not ids:
            return []
        if len(ids) > count:
            ids = ids[:count]
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.zrem(self.queue_name + REDIS_TIMEOUT, *ids)
            pipe.xclaim(
                self.queue_name + REDIS_STREAM,
                groupname=self.group_name,
                consumername=self.id,
                min_idle_time=1000,
                message_ids=ids,
            )
            _, msgs = await pipe.execute()
        return [
            StreamMessage(
                message_id=msg_id.decode(),
                task_id=msg[b"task_id"].decode(),
                score=int(msg[b"score"]),
            )
            for msg_id, msg in msgs
        ]

    async def run_task(self, task_id: str, message_id: str):
        """
        Execute the registered task, then store the result in Redis.
        """
        key = lambda mid: self.queue_name + mid + task_id
        # acquire semaphore
        async with self.bs:
            start_time = now_ms()
            async with self.redis.pipeline(transaction=True) as pipe:
                pipe.get(key(REDIS_TASK))
                pipe.incr(key(REDIS_RETRY))
                pipe.srem(self.queue_name + REDIS_ABORT, task_id)
                pipe.expire(key(REDIS_RETRY), DEFAULT_TTL // 1000)
                raw, task_try, abort, _ = await pipe.execute()

            async def handle_failure(
                exc: BaseException, ttl: timedelta | int | None = 300
            ) -> None:
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
                        self.finish_failed_task(task_id, message_id, raw, ttl)
                    )
                except Exception as e:
                    raise StreaqError(
                        f"Failed to serialize result for task {task_id}!"
                    ) from e

            if not raw:
                logger.warning(f"task {task_id} expired †")
                return await handle_failure(StreaqError("Task execution failed!"))

            try:
                data = self.deserializer(raw)
            except Exception as e:
                logger.exception(f"Failed to deserialize task {task_id}: {e}")
                return await handle_failure(e)

            fn_name = data["f"]
            task = self.registry[fn_name]

            if abort:
                logger.info(f"task {task_id} aborted ⊘ prior to run")
                return await handle_failure(asyncio.CancelledError(), ttl=task.ttl)

            timeout = (
                "inf"
                if task.timeout is None
                else round(time()) + to_seconds(task.timeout) + 1
            )
            await self.redis.zadd(
                self.queue_name + REDIS_TIMEOUT, {message_id: timeout}
            )

            if task.max_tries and task_try > task.max_tries:
                logger.warning(
                    f"task {task_id} failed × after {task.max_tries} retries"
                )
                return await handle_failure(
                    StreaqError(f"Max retry attempts reached for task {task_id}!"),
                    ttl=task.ttl,
                )

            ctx = self.build_context(task, task_id)
            async with task.lifespan(ctx):
                success = True
                delay = None
                done = True
                try:
                    logger.info(f"task {task_id} → worker {self.id}")
                    result = await asyncio.wait_for(
                        task.fn(ctx, *data["a"], **data["k"]),
                        to_seconds(task.timeout) if task.timeout is not None else None,
                    )
                except StreaqRetry as e:
                    result = e
                    success = False
                    done = False
                    if e.delay is not None:
                        delay = to_seconds(e.delay)
                    else:
                        delay = task_try**2
                    logger.info(f"retrying ↻ task {task_id} in {delay}s")
                except asyncio.TimeoutError as e:
                    logger.error(f"task {task_id} timed out …")
                    result = e
                    success = False
                    done = True
                except asyncio.CancelledError as e:
                    if task_id in self.aborting_tasks:
                        logger.info(f"task {task_id} aborted ⊘")
                        self.aborting_tasks.remove(task_id)
                        done = True
                    else:
                        logger.info(f"task {task_id} cancelled, will be retried ↻")
                        done = False
                    result = e
                    success = False
                except Exception as e:
                    result = e
                    success = False
                    done = True
                    logger.info(f"task {task_id} failed ×")
                    logger.exception(e)
                finally:
                    await asyncio.shield(
                        self.finish_task(
                            task_id,
                            message_id,
                            finish=done,
                            return_value=result,  # type: ignore
                            ttl=task.ttl,
                            delay=delay,
                            success=success,
                            start_time=start_time,
                        )
                    )

    async def finish_task(
        self,
        task_id: str,
        message_id: str,
        *,
        finish: bool,
        delay: int | None,
        return_value: Any,
        start_time: int,
        success: bool,
        ttl: timedelta | int | None,
    ) -> None:
        """
        Cleanup for a task that executed successfully or will be retried.
        """
        data = {
            "s": success,
            "r": return_value,
            "st": start_time,
            "ft": now_ms(),
        }
        try:
            result = self.serializer(data)
        except Exception as e:
            raise StreaqError(f"Failed to serialize result for task {task_id}!") from e
        key = lambda mid: self.queue_name + mid + task_id
        async with self.redis.pipeline(transaction=True) as pipe:
            stream_key = self.queue_name + REDIS_STREAM
            pipe.delete(key(REDIS_RUNNING))
            pipe.xack(
                stream_key,
                self.group_name,
                message_id,
            )
            pipe.xdel(stream_key, message_id)
            pipe.publish(self.queue_name + REDIS_CHANNEL, task_id)

            if finish:
                self.counters["completed"] += 1
                if result and ttl != 0:
                    pipe.set(key(REDIS_RESULT), result, ex=ttl)
                pipe.delete(
                    key(REDIS_RETRY),
                    key(REDIS_TASK),
                    key(REDIS_MESSAGE),
                )
                pipe.zrem(self.queue_name + REDIS_TIMEOUT, message_id)
                pipe.srem(self.queue_name + REDIS_ABORT, task_id)
                if success:
                    logger.info(f"task {task_id} ← {str(return_value):.32}")
            elif delay:
                self.counters["retried"] += 1
                pipe.delete(key(REDIS_MESSAGE))
                pipe.zadd(self.queue_name + REDIS_QUEUE, {task_id: now_ms() + delay})
            else:
                self.counters["retried"] += 1
                score = now_ms()
                ttl_ms = to_ms(ttl) if ttl is not None else None
                expire = (ttl_ms or score) + DEFAULT_TTL
                await self.scripts["publish_task"](
                    keys=[stream_key, key(REDIS_MESSAGE)],
                    args=[task_id, score, expire],
                    client=pipe,
                )
            await pipe.execute()

    async def finish_failed_task(
        self,
        task_id: str,
        message_id: str,
        result_data: EncodableT,
        ttl: timedelta | int | None,
    ) -> None:
        """
        Cleanup for a task that failed during execution.
        """
        key = lambda mid: self.queue_name + mid + task_id
        stream_key = self.queue_name + REDIS_STREAM
        self.counters["failed"] += 1
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.delete(
                key(REDIS_RETRY),
                key(REDIS_RUNNING),
                key(REDIS_TASK),
                key(REDIS_MESSAGE),
            )
            pipe.publish(self.queue_name + REDIS_CHANNEL, task_id)
            pipe.srem(self.queue_name + REDIS_ABORT, task_id)
            pipe.zrem(self.queue_name + REDIS_TIMEOUT, message_id)
            pipe.xack(stream_key, self.group_name, message_id)
            pipe.xdel(stream_key, message_id)
            if result_data is not None:
                pipe.set(key(REDIS_RESULT), result_data, ex=ttl)
            await pipe.execute()

    async def redis_health_check(self):
        """
        Checks Redis for current state and logs to the console.
        Only one worker can run this at a time.
        """
        timeout = to_seconds(self.health_check_interval)
        while True:
            async with self.redis.lock(
                self.queue_name + REDIS_LOCK,
                sleep=timeout,
                timeout=timeout,
            ):
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
                await asyncio.sleep(timeout)

    async def health_check(self):
        """
        Periodically logs info about the worker to the console.
        """
        while True:
            await asyncio.sleep(to_seconds(self.health_check_interval))
            health = repr(self)[1:-1]
            logger.info(health)
            await self.redis.hset(  # type: ignore
                self.queue_name + REDIS_HEALTH, self.id, health
            )

    async def queue_size(self) -> int:
        """
        Returns the number of tasks currently queued in Redis.
        """
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.xlen(self.queue_name + REDIS_STREAM)
            pipe.zcard(self.queue_name + REDIS_QUEUE)
            stream_size, queue_size = await pipe.execute()

        return stream_size + queue_size

    @property
    def active(self) -> int:
        """
        The number of currently active tasks for the worker
        """
        return sum(not t.done() for t in self.tasks.values())

    async def close(self) -> None:
        """
        Cleanup worker and Redis connection
        """
        self._block_new_tasks = True
        for t in self.tasks.values():
            if not t.done():
                t.cancel()
                self.counters["interrupted"] += 1
        self.main_task.cancel()
        await asyncio.gather(*self.tasks.values())
        for t in self.tasks.values():
            t.result()
        await self.redis.close(close_connection_pool=True)
        logger.info(
            f"shutdown worker {self.id} completed={self.counters['completed']} failed="
            f"{self.counters['failed']} retried={self.counters['retried']} interrupted="
            f"{self.counters['interrupted']}"
        )

    async def __aenter__(self):
        # register lua scripts
        self.scripts.update(register_scripts(self.redis))
        # user-defined deps
        self._deps = await self.worker_lifespan.__aenter__()
        return self

    async def __aexit__(self, *exc):
        await self.worker_lifespan.__aexit__(*exc)

    def __len__(self):
        return len(self.registry)

    def __repr__(self) -> str:
        return (
            f"<Worker {self.id}: completed={self.counters['completed']} failed="
            f"{self.counters['failed']} retried={self.counters['retried']} running="
            f"{self.active}>"
        )

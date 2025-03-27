import asyncio
from functools import partial
import pickle
import signal
from collections import defaultdict
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from datetime import datetime, timedelta, timezone, tzinfo
from signal import Signals
from time import time
from typing import (
    Any,
    AsyncIterator,
    Callable,
    Concatenate,
    Coroutine,
    Generic,
    cast,
)
from uuid import uuid4

from anyio.abc import CapacityLimiter
from crontab import CronTab
from redis.asyncio import Redis
from redis.asyncio.sentinel import Sentinel
from redis.commands.core import AsyncScript
from redis.typing import EncodableT

from streaq import logger
from streaq.constants import (
    DEFAULT_QUEUE_NAME,
    DEFAULT_TTL,
    REDIS_ABORT,
    REDIS_CHANNEL,
    REDIS_DEPENDENCIES,
    REDIS_DEPENDENTS,
    REDIS_GROUP,
    REDIS_HEALTH,
    REDIS_MESSAGE,
    REDIS_PREFIX,
    REDIS_QUEUE,
    REDIS_RESULT,
    REDIS_RETRY,
    REDIS_RUNNING,
    REDIS_STREAM,
    REDIS_TASK,
)
from streaq.lua import register_scripts
from streaq.types import P, R, WD, StreamMessage, WrappedContext
from streaq.utils import StreaqError, asyncify, now_ms, to_ms, to_seconds
from streaq.task import RegisteredCron, RegisteredTask, StreaqRetry, Task, TaskPriority

"""
Empty object representing uninitialized dependencies. This is distinct from
None because it is possible for an initialized worker to have no dependencies
(implying the WD type is 'None').
"""
uninitialized = object()


@asynccontextmanager
async def _lifespan(worker: "Worker") -> AsyncIterator[None]:
    yield


async def _placeholder(self, *args): ...


class Worker(Generic[WD]):
    """
    Worker object that fetches and executes tasks from a queue.

    :param redis_url: connection URI for Redis
    :param concurrency: number of tasks the worker can run simultaneously
    :param sync_concurrency:
        max number of synchronous tasks the worker can run simultaneously
        in separate threads; defaults to the same as ``concurrency``
    :param queue_name: name of queue in Redis
    :param queue_fetch_limit: max number of tasks to prefetch from Redis
    :param lifespan:
        async context manager that wraps worker execution and provides task
        dependencies
    :param serializer: function to serialize task data for Redis
    :param deserializer: function to deserialize task data from Redis
    :param tz: timezone to use for cron jobs
    :param handle_signals: whether to handle signals for graceful shutdown
    :param health_crontab: crontab for frequency to log and store health info
    :param with_scheduler:
        whether to run a scheduler alongside the worker; if None, the CLI will run
        a single scheduler instead of each worker running one. Multiple schedulers
        can be redundant, so running just one may slightly improve performance.
    """

    __slots__ = (
        "redis",
        "concurrency",
        "queue_name",
        "group_name",
        "queue_fetch_limit",
        "bs",
        "counters",
        "loop",
        "_deps",
        "scripts",
        "registry",
        "cron_jobs",
        "cron_schedule",
        "id",
        "serializer",
        "deserializer",
        "task_wrappers",
        "tasks",
        "tz",
        "aborting_tasks",
        "burst",
        "_handle_signals",
        "_block_new_tasks",
        "_aentered",
        "_aexited",
        "lifespan",
        "_queue_key",
        "_stream_key",
        "_abort_key",
        "_health_key",
        "_channel_key",
        "main_task",
        "_start_time",
        "_prefix",
        "sync_concurrency",
        "_limiter",
        "_sentinel",
        "_health_tab",
        "with_scheduler",
        "middlewares",
    )

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        redis_sentinel_nodes: list[tuple[str, int]] | None = None,
        redis_sentinel_master: str = "mymaster",
        concurrency: int = 16,
        sync_concurrency: int | None = None,
        queue_name: str = DEFAULT_QUEUE_NAME,
        queue_fetch_limit: int | None = None,
        lifespan: Callable[["Worker"], AbstractAsyncContextManager[WD]] = _lifespan,
        serializer: Callable[[Any], EncodableT] = pickle.dumps,
        deserializer: Callable[[Any], Any] = pickle.loads,
        tz: tzinfo = timezone.utc,
        handle_signals: bool = True,
        health_crontab: str = "*/5 * * * *",
        with_scheduler: bool | None = None,
    ):
        #: Redis connection
        if redis_sentinel_nodes:
            self._sentinel = Sentinel(redis_sentinel_nodes, socket_timeout=2.0)
            self.redis = self._sentinel.master_for(redis_sentinel_master)
        else:
            self.redis = Redis.from_url(redis_url)
        self.concurrency = concurrency
        self.queue_name = queue_name
        self.group_name = REDIS_GROUP
        self.queue_fetch_limit = queue_fetch_limit or concurrency * 2
        #: semaphore controlling concurrency
        self.bs = asyncio.BoundedSemaphore(concurrency)
        #: mapping of type of task -> number of tasks of that type
        #: eg ``{"completed": 4, "failed": 1, "retried": 0}``
        self.counters = defaultdict(int)
        #: event loop for running tasks
        self.loop = asyncio.get_event_loop()
        self._deps = uninitialized
        #: Redis scripts for common operations
        self.scripts: dict[str, AsyncScript] = {}
        #: mapping of task name -> task wrapper
        self.registry: dict[str, RegisteredCron | RegisteredTask] = {}
        #: mapping of task name -> cron wrapper
        self.cron_jobs: dict[str, RegisteredCron] = {}
        #: mapping of task name -> next execution time in ms
        self.cron_schedule: dict[str, int] = defaultdict(int)
        #: unique ID of worker
        self.id = uuid4().hex[:8]
        self.serializer = serializer
        self.deserializer = deserializer
        #: mapping of task ID -> asyncio Task wrapper
        self.task_wrappers: dict[str, asyncio.Task] = {}
        #: mapping of task ID -> asyncio Task for task
        self.tasks: dict[str, asyncio.Task] = {}
        self._handle_signals = handle_signals
        self.tz = tz
        #: set of tasks currently scheduled for abortion
        self.aborting_tasks: set[str] = set()
        #: whether to shut down the worker when the queue is empty; set via CLI
        self.burst = False
        #: list of middlewares added to the worker
        self.middlewares: list[
            Callable[
                [WrappedContext[WD], Callable[..., Coroutine]], Callable[..., Coroutine]
            ]
        ] = []
        self.with_scheduler = with_scheduler
        self.sync_concurrency = sync_concurrency or concurrency
        self._limiter = CapacityLimiter(self.sync_concurrency)
        self._block_new_tasks = False
        self._aentered = False
        self._aexited = False
        self.lifespan = lifespan(self)
        self._prefix = REDIS_PREFIX + self.queue_name
        self._queue_key = self._prefix + REDIS_QUEUE
        self._stream_key = self._prefix + REDIS_STREAM
        self._abort_key = self._prefix + REDIS_ABORT
        self._health_key = self._prefix + REDIS_HEALTH
        self._channel_key = self._prefix + REDIS_CHANNEL
        self._start_time = now_ms()
        self._health_tab = CronTab(health_crontab)

        @self.cron(health_crontab, ttl=0)
        async def redis_health_check(ctx: WrappedContext[WD]) -> None:
            """
            Saves Redis health in Redis, then logs worker and Redis health.
            """
            async with self.redis.pipeline(transaction=True) as pipe:
                pipe.info(section="Memory")
                pipe.info(section="Clients")
                pipe.dbsize()
                for priority in TaskPriority:
                    pipe.xlen(self._stream_key + priority.value)
                pipe.zcard(self._queue_key)
                (
                    info_memory,
                    info_clients,
                    key_count,
                    low_size,
                    medium_size,
                    high_size,
                    queue_size,
                ) = await pipe.execute()

            mem_usage = info_memory.get("used_memory_human", "?")
            clients = info_clients.get("connected_clients", "?")
            queued = low_size + medium_size + high_size
            health = (
                f"redis {{memory: {mem_usage}, clients: {clients}, keys: {key_count}, "
                f"queued: {queued}, scheduled: {queue_size}}}"
            )
            async with self.redis.pipeline(transaction=True) as pipe:
                pipe.hset(self._health_key, "redis", health)
                pipe.hgetall(self._health_key)
                _, res = await pipe.execute()
            formatted = [v.decode() for v in res.values()]
            logger.info(
                "health check results:\n" + "\n".join(f for f in sorted(formatted))
            )

    @property
    def deps(self) -> WD:
        if self._deps == uninitialized:
            raise StreaqError(
                "Worker did not initialize correctly, are you using the async context manager?"
            )
        return cast(WD, self._deps)

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
        timeout: timedelta | int | None = None,
        ttl: timedelta | int | None = timedelta(minutes=5),
        unique: bool = True,
    ):
        """
        Registers a task to be run at regular intervals as specified.

        :param tab:
            crontab for scheduling, follows the specification
            `here <https://github.com/josiahcarlson/parse-crontab?tab=readme-ov-file#description>`_.
        :param max_tries:
            number of times to retry the task should it fail during execution
        :param timeout: time after which to abort the task, if None will never time out
        :param ttl: time to store results in Redis, if None will never expire
        :param unique: whether multiple instances of the task can exist simultaneously
        """

        def wrapped(
            fn: Callable[[WrappedContext[WD]], Coroutine[Any, Any, R] | R],
        ) -> RegisteredCron[WD, R]:
            if not asyncio.iscoroutinefunction(fn):
                _fn: Callable[[WrappedContext[WD]], Coroutine[Any, Any, R]] = asyncify(
                    fn, self._limiter
                )  # type: ignore
            else:
                _fn = fn
            task = RegisteredCron(
                _fn,
                max_tries,
                CronTab(tab),
                timeout,
                ttl,
                unique,
                self,
            )
            self.cron_jobs[task.fn_name] = task
            self.registry[task.fn_name] = task
            logger.debug(f"cron job {task.fn_name} registered in worker {self.id}")
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
            fn: Callable[
                Concatenate[WrappedContext[WD], P], Coroutine[Any, Any, R] | R
            ],
        ) -> RegisteredTask[WD, P, R]:
            if not asyncio.iscoroutinefunction(fn):
                _fn: Callable[
                    Concatenate[WrappedContext[WD], P], Coroutine[Any, Any, R]
                ] = asyncify(fn, self._limiter)  # type: ignore
            else:
                _fn = fn
            task = RegisteredTask(
                _fn,
                max_tries,
                timeout,
                ttl,
                unique,
                self,
            )
            self.registry[task.fn_name] = task
            logger.debug(f"task {task.fn_name} registered in worker {self.id}")
            return task

        return wrapped

    def middleware(
        self,
        fn: Callable[
            [WrappedContext[WD], Callable[..., Coroutine]], Callable[..., Coroutine]
        ],
    ):
        """
        Registers the given middleware with the worker.
        """
        self.middlewares.append(fn)
        return fn

    def run_sync(self) -> None:
        """
        Sync function to run the worker, finally closes worker connections.
        """
        self.main_task = self.loop.create_task(self.main())
        try:
            self.loop.run_until_complete(self.main_task)
        except asyncio.CancelledError:
            logger.debug(f"main loop interrupted, closing worker {self.id}")
        finally:
            self.loop.run_until_complete(self.close())

    async def run_async(self) -> None:
        """
        Async function to run the worker. Cleanup should be handled separately.
        """
        self.main_task = self.loop.create_task(self.main())
        await self.main_task

    async def main(self) -> None:
        """
        Main loop for handling worker tasks, aggregates and runs other tasks
        """
        logger.info(f"starting worker {self.id} for {len(self)} functions")
        # register signal handlers
        if self._handle_signals:
            self._add_signal_handler(signal.SIGINT)
            self._add_signal_handler(signal.SIGTERM)
        # create consumer group if it doesn't exist
        async with self.redis.pipeline(transaction=False) as pipe:
            for priority in TaskPriority:
                pipe.xgroup_create(
                    name=self._stream_key + priority.value,
                    groupname=self.group_name,
                    id=0,
                    mkstream=True,
                )
            await pipe.execute(raise_on_error=False)
            logger.debug(f"consumer group {self.group_name} created for queues")
        async with self:
            # run loops
            tasks = [self.listen_stream(), self.health_check()]
            if self.with_scheduler:
                tasks.append(self.listen_queue())
            futures = [self.loop.create_task(t) for t in tasks]
            try:
                _, pending = await asyncio.wait(
                    futures,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                logger.debug(f"main loop wrapping up execution for worker {self.id}")
                for task in pending:
                    task.cancel()
            except asyncio.CancelledError:
                for task in futures:
                    task.cancel()
                await asyncio.gather(*futures, return_exceptions=True)

    async def listen_queue(self) -> None:
        """
        Periodically check the future queue (sorted set) for tasks, adding
        them to the live queue (stream) when ready, as well as adding cron
        jobs to the live queue when ready.
        """
        # schedule initial cron jobs
        futures = set()
        for name, cron_job in self.cron_jobs.items():
            self.cron_schedule[name] = cron_job.next()
            futures.add(cron_job.enqueue().start(schedule=cron_job.schedule()))
        if futures:
            logger.debug(f"enqueuing {len(futures)} cron jobs in worker {self.id}")
            await asyncio.gather(*futures)
        while not self._block_new_tasks:
            start_time = time()
            task_ids = await self.redis.zrange(
                self._queue_key,
                start=0,
                end=now_ms(),
                num=self.queue_fetch_limit,
                offset=0,
                withscores=True,
                byscore=True,
            )
            async with self.redis.pipeline(transaction=False) as pipe:
                for task_id, score in task_ids:
                    expire_ms = int(score - now_ms() + DEFAULT_TTL)
                    if expire_ms <= 0:
                        expire_ms = DEFAULT_TTL

                    decoded = task_id.decode()
                    await self.scripts["publish_delayed_task"](
                        keys=[
                            self._queue_key,
                            self._stream_key,
                            self._prefix + REDIS_MESSAGE + decoded,
                        ],
                        args=[decoded, expire_ms, TaskPriority.MEDIUM.value],
                        client=pipe,
                    )
                if task_ids:
                    logger.debug(
                        f"enqueuing {len(task_ids)} delayed tasks in worker {self.id}"
                    )
                await pipe.execute()

            # cron jobs
            futures = set()
            ts = now_ms()
            for name, cron_job in self.cron_jobs.items():
                if ts - 500 > self.cron_schedule[name]:
                    self.cron_schedule[name] = cron_job.next()
                    futures.add(cron_job.enqueue().start(schedule=cron_job.schedule()))
            if futures:
                logger.debug(f"enqueuing {len(futures)} cron jobs in worker {self.id}")
                await asyncio.gather(*futures)

            delay = time() - start_time
            if delay < 0.5:
                await asyncio.sleep(0.5 - delay)

    async def listen_stream(self) -> None:
        """
        Listen for new tasks from the stream, and periodically check for tasks
        that were never XACK'd but have timed out to reclaim.
        """
        high_stream = self._stream_key + TaskPriority.HIGH.value
        medium_stream = self._stream_key + TaskPriority.MEDIUM.value
        low_stream = self._stream_key + TaskPriority.LOW.value
        priority_order = {
            TaskPriority.HIGH.value: 0,
            TaskPriority.MEDIUM.value: 1,
            TaskPriority.LOW.value: 2,
        }
        while not self._block_new_tasks:
            messages: list[StreamMessage] = []
            active_tasks = self.concurrency - self.bs._value
            pending_tasks = len(self.task_wrappers)
            count = self.queue_fetch_limit - pending_tasks
            async with self.redis.pipeline(transaction=False) as pipe:
                pipe.smembers(self._abort_key)
                if count > 0:
                    pipe.xreadgroup(
                        groupname=self.group_name,
                        consumername=self.id,
                        streams={high_stream: ">", medium_stream: ">", low_stream: ">"},
                        block=500,
                        count=count,
                    )
                res = await pipe.execute()
            if count > 0:
                for stream, msgs in res[-1]:
                    priority = stream.decode().split(":")[-1]
                    messages.extend(
                        [
                            StreamMessage(
                                message_id=msg_id.decode(),
                                task_id=msg[b"task_id"].decode(),
                                priority=priority,
                            )
                            for msg_id, msg in msgs
                        ]
                    )
            # Go through task_ids in the aborted tasks set and cancel those tasks.
            if res[0]:
                aborted: set[str] = set()
                for task_id_bytes in res[0]:
                    task_id = task_id_bytes.decode()
                    if task_id in self.tasks:
                        self.tasks[task_id].cancel()
                        aborted.add(task_id)
                if aborted:
                    logger.debug(f"aborting {len(aborted)} tasks in worker {self.id}")
                    self.aborting_tasks.update(aborted)
                    await self.redis.srem(self._abort_key, *aborted)  # type: ignore
            # start new tasks
            if messages:
                logger.debug(f"starting {len(messages)} new tasks in worker {self.id}")
                messages.sort(key=lambda msg: priority_order[msg.priority])
            for message in messages:
                coro = self.run_task(message)
                self.task_wrappers[message.task_id] = self.loop.create_task(coro)
            # wrap things up if we burstin'
            if (
                self.burst
                and not messages
                and active_tasks == 0
                and count > 0
                and not self.task_wrappers
            ):
                self._block_new_tasks = True
            # cleanup aborted tasks
            for task_id, task in list(self.task_wrappers.items()):
                if task.done():
                    del self.task_wrappers[task_id]
                    # propagate error
                    task.result()

    async def run_task(self, msg: StreamMessage) -> None:
        """
        Execute the registered task, then store the result in Redis.
        """
        task_id = msg.task_id
        key = lambda mid: self._prefix + mid + task_id
        # acquire semaphore
        async with self.bs:
            start_time = now_ms()
            async with self.redis.pipeline(transaction=True) as pipe:
                pipe.get(key(REDIS_TASK))
                pipe.incr(key(REDIS_RETRY))
                pipe.srem(self._abort_key, task_id)
                pipe.pexpire(key(REDIS_RETRY), DEFAULT_TTL)
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
                    await asyncio.shield(self.finish_failed_task(msg, raw, ttl))
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
            if fn_name not in self.registry:
                logger.error(
                    f"Missing function {fn_name}, can't execute task {task_id}!"
                )
                return await handle_failure(StreaqError("Nonexistent function!"))
            task = self.registry[fn_name]

            if abort:
                logger.info(f"task {task_id} aborted ⊘ prior to run")
                return await handle_failure(asyncio.CancelledError(), ttl=task.ttl)

            timeout = (
                None
                if task.timeout is None
                else start_time + 1000 + to_ms(task.timeout)
            )
            await self.redis.set(key(REDIS_RUNNING), 1, pxat=timeout)

            if task.max_tries and task_try > task.max_tries:
                logger.warning(
                    f"task {task_id} failed × after {task.max_tries} retries"
                )
                return await handle_failure(
                    StreaqError(f"Max retry attempts reached for task {task_id}!"),
                    ttl=task.ttl,
                )

            ctx = self.build_context(task, task_id, tries=task_try)
            success = True
            delay = None
            done = True
            finish_time = None

            async def _fn(ctx, *args, **kwargs):
                return await asyncio.wait_for(
                    task.fn(ctx, *args, **kwargs),
                    to_seconds(task.timeout) if task.timeout is not None else None,
                )

            try:
                logger.info(f"task {task_id} → worker {self.id}")
                wrapped = _fn
                for middleware in reversed(self.middlewares):
                    wrapped = middleware(ctx, wrapped)
                coro = wrapped(ctx, *data["a"], **data["k"])
                try:
                    self.tasks[task_id] = self.loop.create_task(coro)
                    result = await self.tasks[task_id]
                except Exception as e:
                    raise e  # re-raise for outer try/except
                finally:
                    del self.tasks[task_id]
                    finish_time = now_ms()
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
                    self.counters["aborted"] += 1
                    self.counters["failed"] -= 1  # this will get incremented later
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
                        msg,
                        finish=done,
                        delay=delay,
                        return_value=result,  # type: ignore
                        start_time=start_time,
                        finish_time=finish_time or now_ms(),
                        success=success,
                        ttl=task.ttl,
                    )
                )

    async def finish_task(
        self,
        msg: StreamMessage,
        finish: bool,
        delay: float | None,
        return_value: Any,
        start_time: int,
        finish_time: int,
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
            "ft": finish_time,
        }
        task_id = msg.task_id
        try:
            result = self.serializer(data)
        except Exception as e:
            raise StreaqError(f"Failed to serialize result for task {task_id}!") from e
        key = lambda mid: self._prefix + mid + task_id
        stream_key = self._stream_key + msg.priority
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.xack(stream_key, self.group_name, msg.message_id)
            pipe.xdel(stream_key, msg.message_id)
            if finish:
                pipe.publish(self._channel_key, task_id)
                if success:
                    self.counters["completed"] += 1
                else:
                    self.counters["failed"] += 1
                if result and ttl != 0:
                    pipe.set(key(REDIS_RESULT), result, ex=ttl)
                pipe.delete(
                    key(REDIS_RETRY),
                    key(REDIS_RUNNING),
                    key(REDIS_TASK),
                    key(REDIS_MESSAGE),
                )
                pipe.srem(self._abort_key, task_id)
                if success:
                    logger.info(f"task {task_id} ← {str(return_value):.32}")
                    script = self.scripts["update_dependents"]
                else:
                    script = self.scripts["fail_dependents"]
                await script(
                    keys=[
                        self._prefix + REDIS_DEPENDENTS,
                        self._prefix + REDIS_DEPENDENCIES,
                        task_id,
                    ],
                    client=pipe,
                )
            elif delay:
                self.counters["retried"] += 1
                pipe.delete(key(REDIS_MESSAGE))
                pipe.zadd(self._queue_key, {task_id: now_ms() + delay * 1000})
            else:
                self.counters["retried"] += 1
                ttl_ms = to_ms(ttl) if ttl is not None else None
                expire = (ttl_ms or 0) + DEFAULT_TTL
                await self.scripts["retry_task"](
                    keys=[stream_key, key(REDIS_MESSAGE)],
                    args=[task_id, expire],
                    client=pipe,
                )
            res = await pipe.execute()
        if finish and res[-1]:
            if success:
                async with self.redis.pipeline(transaction=False) as pipe:
                    for dep_id in res[-1]:
                        dep_id = dep_id.decode()
                        await self.scripts["publish_dependent"](
                            keys=[
                                stream_key,
                                self._prefix + REDIS_MESSAGE + dep_id,
                                dep_id,
                            ],
                            args=[DEFAULT_TTL],
                            client=pipe,
                        )
                    await pipe.execute()
            else:
                failure = {
                    "s": False,
                    "r": StreaqError("Dependency failed, not running task!"),
                    "st": -1,
                    "ft": -1,
                }
                try:
                    result = self.serializer(failure)
                except Exception as e:
                    raise StreaqError(
                        f"Failed to serialize result for dependency of task {task_id}!"
                    ) from e
                async with self.redis.pipeline(transaction=False) as pipe:
                    self.counters["failed"] += len(res[-1])
                    for dep_id in res[-1]:
                        dep_id = dep_id.decode()
                        logger.info(f"task {dep_id} dependency failed ×")
                        await self.scripts["unpublish_dependent"](
                            keys=[
                                self._prefix + REDIS_TASK + dep_id,
                                self._prefix + REDIS_RESULT + dep_id,
                                self._channel_key,
                                dep_id,
                            ],
                            args=[result],
                            client=pipe,
                        )
                    await pipe.execute()

    async def finish_failed_task(
        self,
        msg: StreamMessage,
        result_data: EncodableT,
        ttl: timedelta | int | None,
    ) -> None:
        """
        Cleanup for a task that failed during execution.
        """
        task_id = msg.task_id
        key = lambda mid: self._prefix + mid + task_id
        self.counters["failed"] += 1
        stream_key = self._stream_key + msg.priority
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.delete(
                key(REDIS_RETRY),
                key(REDIS_RUNNING),
                key(REDIS_TASK),
                key(REDIS_MESSAGE),
            )
            pipe.publish(self._channel_key, task_id)
            pipe.srem(self._abort_key, task_id)
            pipe.xack(stream_key, self.group_name, msg.message_id)
            pipe.xdel(stream_key, msg.message_id)
            if result_data is not None:
                pipe.set(key(REDIS_RESULT), result_data, ex=ttl)
            await self.scripts["fail_dependents"](
                keys=[
                    self._prefix + REDIS_DEPENDENTS,
                    self._prefix + REDIS_DEPENDENCIES,
                    task_id,
                ],
                client=pipe,
            )
            res = await pipe.execute()
        if res[-1]:
            failure = {
                "s": False,
                "r": StreaqError("Dependency failed, not running task!"),
                "st": -1,
                "ft": -1,
            }
            try:
                result = self.serializer(failure)
            except Exception as e:
                raise StreaqError(
                    f"Failed to serialize result for task {task_id}!"
                ) from e
            async with self.redis.pipeline(transaction=False) as pipe:
                self.counters["failed"] += len(res[-1])
                for dep_id in res[-1]:
                    dep_id = dep_id.decode()
                    logger.info(f"task {dep_id} dependency failed ×")
                    await self.scripts["unpublish_dependent"](
                        keys=[
                            self._prefix + REDIS_TASK + dep_id,
                            self._prefix + REDIS_RESULT + dep_id,
                            self._channel_key,
                            dep_id,
                        ],
                        args=[result],
                        client=pipe,
                    )
                await pipe.execute()

    def enqueue_unsafe(
        self,
        fn_name: str,
        *args,
        unique: bool = False,
        **kwargs,
    ) -> Task:
        """
        Allows for enqueuing a task that is registered elsewhere without having access
        to the worker it's registered to. This is unsafe because it doesn't check if the
        task is registered with the worker and doesn't enforce types, so it should only
        be used if you need to separate the task queuing and task execution code for
        performance reasons.

        :param fn_name:
            name of the function to run, much match its __qualname__. If you're unsure,
            check ``Worker.registry``.
        :param unique: whether multiple instances of the task can exist simultaneously
        :param args: positional arguments for the task
        :param kwargs: keyword arguments for the task

        :return: task object
        """
        registered = RegisteredTask(
            fn=_placeholder,
            max_tries=None,
            timeout=None,
            ttl=None,
            unique=unique,
            worker=self,
            _fn_name=fn_name,
        )
        return Task(args, kwargs, registered)

    async def queue_size(self) -> int:
        """
        Returns the number of tasks currently queued in Redis.
        """
        async with self.redis.pipeline(transaction=True) as pipe:
            for priority in TaskPriority:
                pipe.xlen(self._stream_key + priority.value)
            pipe.zcard(self._queue_key)
            res = await pipe.execute()

        return sum(res)

    @property
    def active(self) -> int:
        """
        The number of currently active tasks for the worker
        """
        return sum(not t.done() for t in self.tasks.values())

    async def health_check(self) -> None:
        """
        Periodically stores info about the worker in Redis.
        """
        while True:
            await asyncio.sleep(self._health_tab.next(now=datetime.now(self.tz)))  # type: ignore
            await self.redis.hset(self._health_key, self.id, str(self))  # type: ignore

    def _add_signal_handler(self, signum: Signals) -> None:
        try:
            self.loop.add_signal_handler(signum, partial(self.handle_signal, signum))
        except NotImplementedError:
            logger.error("Windows does not support handling Unix signals!")

    def handle_signal(self, signum: Signals) -> None:
        """
        Gracefully shutdown the worker when a signal is received.
        """
        logger.info(f"received signal {signum.name}, shutting down worker {self.id}")
        self._block_new_tasks = True

    async def close(self) -> None:
        """
        Cleanup worker and Redis connection
        """
        self._block_new_tasks = True
        for t in self.task_wrappers.values():
            if not t.done():
                t.cancel()
        self.main_task.cancel()
        await asyncio.gather(
            *self.task_wrappers.values(), self.main_task, return_exceptions=True
        )
        # delete consumers
        async with self.redis.pipeline(transaction=False) as pipe:
            for priority in TaskPriority:
                pipe.xgroup_delconsumer(
                    name=self._stream_key + priority.value,
                    groupname=self.group_name,
                    consumername=self.id,
                )
            pipe.hdel(self._health_key, self.id)
            await pipe.execute(raise_on_error=False)
        await self.redis.close(close_connection_pool=True)
        run_time = now_ms() - self._start_time
        logger.info(f"shutdown {str(self)} after {run_time}ms")

    async def __aenter__(self):
        # reentrant
        if not self._aentered:
            self._aentered = True
            # register lua scripts
            self.scripts.update(register_scripts(self.redis))
            # user-defined deps
            self._deps = await self.lifespan.__aenter__()
        return self

    async def __aexit__(self, *exc):
        # reentrant
        if not self._aexited:
            self._aexited = True
            await self.lifespan.__aexit__(*exc)

    def __len__(self):
        return len(self.registry)

    def __str__(self) -> str:
        counters_str = dict.__repr__(self.counters).replace("'", "")
        return f"worker {self.id} {counters_str}"

    def __repr__(self) -> str:
        return f"<{str(self)}>"

from __future__ import annotations

import asyncio
import pickle
import signal
from collections import defaultdict
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from contextvars import ContextVar
from datetime import datetime, timedelta, timezone, tzinfo
from functools import partial
from signal import Signals
from time import time
from types import TracebackType
from typing import Any, AsyncIterator, Callable, Generic, Type, cast
from uuid import uuid4

from anyio.abc import CapacityLimiter
from coredis import PureToken, Redis
from coredis.commands import PubSub, Script
from coredis.sentinel import Sentinel
from crontab import CronTab

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
    REDIS_TIMEOUT,
)
from streaq.lua import register_scripts
from streaq.task import (
    RegisteredCron,
    RegisteredTask,
    StreaqRetry,
    Task,
    TaskData,
    TaskPriority,
    TaskResult,
    TaskStatus,
)
from streaq.types import (
    WD,
    AsyncCron,
    AsyncTask,
    CronDefinition,
    Middleware,
    P,
    R,
    StreamMessage,
    SyncCron,
    SyncTask,
    TaskContext,
    TaskDefinition,
    TypedCoroutine,
)
from streaq.utils import StreaqError, asyncify, now_ms, to_ms, to_seconds, to_tuple


@asynccontextmanager
async def _lifespan(worker: Worker[None]) -> AsyncIterator[None]:
    yield None


async def _placeholder() -> None: ...


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
    :param health_crontab: crontab for frequency to store health info
    :param with_scheduler:
        whether to run a scheduler alongside the worker; if None, the CLI will run
        a single scheduler instead of each worker running one. Multiple schedulers
        can be redundant, so running just one may slightly improve performance.
    """

    __slots__ = (
        "redis",
        "concurrency",
        "queue_name",
        "_group_name",
        "queue_fetch_limit",
        "bs",
        "counters",
        "loop",
        "_worker_context",
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
        "lifespan",
        "_stack",
        "queue_key",
        "stream_key",
        "_abort_key",
        "_health_key",
        "_channel_key",
        "_timeout_key",
        "main_task",
        "_start_time",
        "_prefix",
        "sync_concurrency",
        "_limiter",
        "_sentinel",
        "_health_tab",
        "with_scheduler",
        "middlewares",
        "_task_context",
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
        lifespan: Callable[[Worker[WD]], AbstractAsyncContextManager[WD]] = _lifespan,  # type: ignore
        serializer: Callable[[Any], Any] = pickle.dumps,
        deserializer: Callable[[Any], Any] = pickle.loads,
        tz: tzinfo = timezone.utc,
        handle_signals: bool = True,
        health_crontab: str = "*/5 * * * *",
        with_scheduler: bool | None = None,
    ):
        #: Redis connection
        if redis_sentinel_nodes:
            self._sentinel = Sentinel(
                redis_sentinel_nodes, decode_responses=True, socket_timeout=2.0
            )
            self.redis = self._sentinel.primary_for(redis_sentinel_master)
        else:
            self.redis = Redis.from_url(redis_url, decode_responses=True)
        self.concurrency = concurrency
        self.queue_name = queue_name
        self._group_name = REDIS_GROUP
        self.queue_fetch_limit = queue_fetch_limit or concurrency * 2
        #: semaphore controlling concurrency
        self.bs = asyncio.BoundedSemaphore(concurrency)
        #: mapping of type of task -> number of tasks of that type
        #: eg ``{"completed": 4, "failed": 1, "retried": 0}``
        self.counters: dict[str, int] = defaultdict(int)
        #: event loop for running tasks
        self.loop = asyncio.get_event_loop()
        #: Redis scripts for common operations
        self.scripts: dict[str, Script[str]] = {}
        #: mapping of task name -> task wrapper
        self.registry: dict[
            str, RegisteredCron[Any, Any] | RegisteredTask[Any, Any, Any]
        ] = {}
        #: mapping of task name -> cron wrapper
        self.cron_jobs: dict[str, RegisteredCron[Any, Any]] = {}
        #: mapping of task name -> next execution time in ms
        self.cron_schedule: dict[str, int] = defaultdict(int)
        #: unique ID of worker
        self.id = uuid4().hex[:8]
        self.serializer = serializer
        self.deserializer = deserializer
        #: mapping of task ID -> asyncio Task wrapper
        self.task_wrappers: dict[str, asyncio.Task[Any]] = {}
        #: mapping of task ID -> asyncio Task for task
        self.tasks: dict[str, asyncio.Task[Any]] = {}
        self._handle_signals = handle_signals
        self.tz = tz
        #: set of tasks currently scheduled for abortion
        self.aborting_tasks: set[str] = set()
        #: whether to shut down the worker when the queue is empty; set via CLI
        self.burst = False
        #: list of middlewares added to the worker
        self.middlewares: list[Middleware] = []
        self.with_scheduler = with_scheduler
        self.sync_concurrency = sync_concurrency or concurrency
        self._limiter = CapacityLimiter(self.sync_concurrency)
        self._block_new_tasks = False
        self.lifespan = lifespan
        self._stack: list[AbstractAsyncContextManager[WD]] = []
        self._prefix = REDIS_PREFIX + self.queue_name
        #: prefix in Redis for delayed task queue
        self.queue_key = self._prefix + REDIS_QUEUE
        #: prefix in Redis for currently queued tasks
        self.stream_key = self._prefix + REDIS_STREAM
        self._abort_key = self._prefix + REDIS_ABORT
        self._health_key = self._prefix + REDIS_HEALTH
        self._channel_key = self._prefix + REDIS_CHANNEL
        self._timeout_key = self._prefix + REDIS_TIMEOUT
        self._start_time = now_ms()
        self._health_tab = CronTab(health_crontab)
        self._task_context: ContextVar[TaskContext] = ContextVar("_task_context")

        @self.cron(health_crontab, silent=True, ttl=0)
        async def redis_health_check() -> None:  # type: ignore[unused-function]
            """
            Saves Redis health in Redis.
            """
            pipe = await self.redis.pipeline(transaction=False)
            await pipe.info("Memory", "Clients")
            await pipe.dbsize()
            for priority in TaskPriority:
                await pipe.xlen(self.stream_key + priority.value)
            await pipe.zcard(self.queue_key)
            (
                info,
                key_count,
                low_size,
                medium_size,
                high_size,
                queue_size,
            ) = await pipe.execute()
            mem_usage = info.get("used_memory_human", "?")
            clients = info.get("connected_clients", "?")
            queued = low_size + medium_size + high_size
            health = (
                f"redis {{memory: {mem_usage}, clients: {clients}, keys: {key_count}, "
                f"queued: {queued}, scheduled: {queue_size}}}"
            )
            ttl = int(self._delay_for(self._health_tab)) + 5
            await self.redis.set(self._health_key + ":redis", health, ex=ttl)

    def task_context(self) -> TaskContext:
        """
        Fetch task information for the currently running task.
        This can only be called from within a task!
        """
        return self._task_context.get()

    @property
    def context(self) -> WD:
        """
        Worker dependencies initialized with the async context manager.
        """
        if not self._stack:
            raise StreaqError(
                "Worker did not initialize correctly, are you using the "
                "async context manager?"
            )
        return self._worker_context

    def build_context(
        self,
        fn_name: str,
        registered_task: RegisteredCron[Any, Any] | RegisteredTask[Any, Any, Any],
        id: str,
        tries: int = 1,
    ) -> TaskContext:
        """
        Creates the context for a task to be run given task metadata
        """
        return TaskContext(
            fn_name=fn_name,
            task_id=id,
            timeout=registered_task.timeout,
            tries=tries,
            ttl=registered_task.ttl,
        )

    def cron(
        self,
        tab: str,
        *,
        max_tries: int | None = 3,
        silent: bool = False,
        timeout: timedelta | int | None = None,
        ttl: timedelta | int | None = timedelta(minutes=5),
        unique: bool = True,
    ) -> CronDefinition[WD]:
        """
        Registers a task to be run at regular intervals as specified.

        :param tab:
            crontab for scheduling, follows the specification
            `here <https://github.com/josiahcarlson/parse-crontab?tab=readme-ov-file#description>`_.
        :param max_tries:
            number of times to retry the task should it fail during execution
        :param silent:
            whether to silence task logs and success/failure tracking; defaults to False
        :param timeout: time after which to abort the task, if None will never time out
        :param ttl: time to store results in Redis, if None will never expire
        :param unique: whether multiple instances of the task can exist simultaneously
        """

        def wrapped(fn: AsyncCron[R] | SyncCron[R]) -> RegisteredCron[WD, R]:
            if asyncio.iscoroutinefunction(fn):
                _fn = fn
            else:
                _fn = asyncify(fn, self._limiter)
            task = RegisteredCron(
                cast(AsyncCron[R], _fn),
                CronTab(tab),
                max_tries,
                silent,
                timeout,
                ttl,
                unique,
                self,
            )
            self.cron_jobs[task.fn_name] = task
            self.registry[task.fn_name] = task
            logger.debug(f"cron job {task.fn_name} registered in worker {self.id}")
            return task

        return wrapped  # type: ignore

    def task(
        self,
        *,
        max_tries: int | None = 3,
        silent: bool = False,
        timeout: timedelta | int | None = None,
        ttl: timedelta | int | None = timedelta(minutes=5),
        unique: bool = False,
    ) -> TaskDefinition[WD]:
        """
        Registers a task with the worker which can later be enqueued by the user.

        :param max_tries:
            number of times to retry the task should it fail during execution
        :param silent:
            whether to silence task logs and success/failure tracking; defaults to False
        :param timeout: time after which to abort the task, if None will never time out
        :param ttl: time to store results in Redis, if None will never expire
        :param unique: whether multiple instances of the task can exist simultaneously
        """

        def wrapped(
            fn: AsyncTask[P, R] | SyncTask[P, R],
        ) -> RegisteredTask[WD, P, R]:
            if asyncio.iscoroutinefunction(fn):
                _fn = fn
            else:
                _fn = asyncify(fn, self._limiter)
            task = RegisteredTask(
                cast(AsyncTask[P, R], _fn),
                max_tries,
                silent,
                timeout,
                ttl,
                unique,
                self,
            )
            self.registry[task.fn_name] = task
            logger.debug(f"task {task.fn_name} registered in worker {self.id}")
            return task

        return wrapped  # type: ignore

    def middleware(self, fn: Middleware) -> Middleware:
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
        async with self:
            # create consumer group if it doesn't exist
            await self.scripts["create_groups"](
                keys=[self.stream_key, self._group_name],
                args=[p.value for p in TaskPriority],
            )
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
        message_prefix = self._prefix + REDIS_MESSAGE
        futures: set[TypedCoroutine[Task[Any]]] = set()
        # schedule initial cron jobs
        for name, cron_job in self.cron_jobs.items():
            self.cron_schedule[name] = cron_job.next()
            futures.add(cron_job.enqueue().start(schedule=cron_job.schedule()))
        if futures:
            logger.debug(f"enqueuing {len(futures)} cron jobs in worker {self.id}")
            await asyncio.gather(*futures)
        while not self._block_new_tasks:
            start_time = time()
            task_ids = await self.redis.zrange(
                self.queue_key,
                0,
                now_ms(),
                count=self.queue_fetch_limit,
                offset=0,
                withscores=True,
                sortby=PureToken.BYSCORE,
            )
            pipe = await self.redis.pipeline(transaction=False)
            for priority in TaskPriority:
                await self.scripts["unclaim_idle_tasks"](
                    keys=[
                        self._timeout_key + priority.value,
                        self.stream_key + priority.value,
                        self._group_name,
                        self.id,
                        message_prefix,
                    ],
                    args=[now_ms(), DEFAULT_TTL],
                    client=pipe,
                )
            for task_id, score in task_ids:
                expire_ms = int(score - now_ms() + DEFAULT_TTL)
                if expire_ms <= 0:
                    expire_ms = DEFAULT_TTL
                await self.scripts["publish_delayed_task"](
                    keys=[
                        self.queue_key,
                        self.stream_key,
                        self._prefix + REDIS_MESSAGE + task_id,  # type: ignore
                    ],
                    args=[task_id, expire_ms, TaskPriority.MEDIUM.value],
                    client=pipe,
                )
            if task_ids:
                logger.debug(
                    f"enqueuing {len(task_ids)} delayed tasks in worker {self.id}"
                )
            res = await pipe.execute()
            idle = res[0] + res[1] + res[2]  # for each priority level
            if idle:
                logger.info(f"retrying ↻ {idle} idle tasks")

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

            if (delay := time() - start_time) < 0.5:
                await asyncio.sleep(0.5 - delay)

    async def listen_stream(self) -> None:
        """
        Listen for new tasks from the stream, and periodically check for tasks
        that were never XACK'd but have timed out to reclaim.
        """
        high_stream = self.stream_key + TaskPriority.HIGH.value
        medium_stream = self.stream_key + TaskPriority.MEDIUM.value
        low_stream = self.stream_key + TaskPriority.LOW.value
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
            pipe = await self.redis.pipeline(transaction=False)
            await pipe.smembers(self._abort_key)
            if count > 0:
                await pipe.xreadgroup(
                    self._group_name,
                    self.id,
                    streams={high_stream: ">", medium_stream: ">", low_stream: ">"},
                    block=500,
                    count=count,
                )
            res = await pipe.execute()
            if count > 0 and res[-1]:
                for stream, msgs in res[-1].items():
                    priority = stream.split(":")[-1]
                    messages.extend(
                        [
                            StreamMessage(
                                message_id=msg_id,
                                task_id=msg["task_id"],
                                priority=priority,
                            )
                            for msg_id, msg in msgs
                        ]
                    )
            else:
                await asyncio.sleep(0)  # yield control
            # Go through task_ids in the aborted tasks set and cancel those tasks.
            if res[0]:
                aborted: set[str] = set()
                for task_id in res[0]:
                    if task_id in self.tasks:
                        self.tasks[task_id].cancel()
                        aborted.add(task_id)
                if aborted:
                    logger.debug(f"aborting {len(aborted)} tasks in worker {self.id}")
                    self.aborting_tasks.update(aborted)
                    await self.redis.srem(self._abort_key, aborted)
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

        def key(mid: str) -> str:
            return self._prefix + mid + task_id

        # acquire semaphore
        async with self.bs:
            start_time = now_ms()
            pipe = await self.redis.pipeline(transaction=True)
            await pipe.get(key(REDIS_TASK))
            await pipe.incr(key(REDIS_RETRY))
            await pipe.srem(self._abort_key, [task_id])
            await pipe.pexpire(key(REDIS_RETRY), DEFAULT_TTL)
            raw, task_try, abort, _ = await pipe.execute()

            async def handle_failure(
                exc: BaseException,
                silent: bool = False,
                ttl: timedelta | int | None = 300,
            ) -> None:
                data = {
                    "s": False,
                    "r": exc,
                    "st": start_time,
                    "ft": now_ms(),
                }
                try:
                    raw = self.serializer(data)
                    await asyncio.shield(self.finish_failed_task(msg, raw, silent, ttl))
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

            if (fn_name := data["f"]) not in self.registry:
                logger.error(
                    f"Missing function {fn_name}, can't execute task {task_id}!"
                )
                return await handle_failure(StreaqError("Nonexistent function!"))
            task = self.registry[fn_name]

            if abort:
                if not task.silent:
                    logger.info(f"task {task_id} aborted ⊘ prior to run")
                return await handle_failure(
                    asyncio.CancelledError(), silent=task.silent, ttl=task.ttl
                )
            if task.max_tries and task_try > task.max_tries:
                if not task.silent:
                    logger.warning(
                        f"task {task_id} failed × after {task.max_tries} retries"
                    )
                return await handle_failure(
                    StreaqError(f"Max retry attempts reached for task {task_id}!"),
                    silent=task.silent,
                    ttl=task.ttl,
                )
            timeout = (
                None
                if task.timeout is None
                else start_time + 1000 + to_ms(task.timeout)
            )
            after = data.get("A")
            pipe = await self.redis.pipeline(transaction=True)
            lock_key = (
                self._prefix + REDIS_RUNNING + task.fn_name
                if task.unique
                else key(REDIS_RUNNING)
            )
            if task.unique:
                await pipe.set(lock_key, task_id, condition=PureToken.NX, pxat=timeout)
            else:
                await pipe.set(lock_key, task.fn_name, pxat=timeout)
            if timeout:
                await pipe.zadd(
                    self._timeout_key + msg.priority, {msg.message_id: timeout}
                )
            if after:
                await pipe.get(self._prefix + REDIS_RESULT + after + ":raw")
            res = await pipe.execute()
            if task.unique and not res[0]:
                if not task.silent:
                    logger.warning(f"unique task {task_id} clashed with running task ↯")
                return await handle_failure(
                    StreaqError(
                        "Task is unique and another instance of the same task is "
                        "already running!"
                    ),
                    silent=task.silent,
                    ttl=task.ttl,
                )
            _args = data["a"] if not after else self.deserializer(res[-1])

            ctx = self.build_context(fn_name, task, task_id, tries=task_try)
            success = True
            delay = None
            done = True
            finish_time = None

            async def _fn(*args: Any, **kwargs: Any) -> Any:
                return await asyncio.wait_for(
                    task.fn(*args, **kwargs), to_seconds(task.timeout)
                )

            if not task.silent:
                logger.info(f"task {task_id} → worker {self.id}")

            wrapped = _fn
            for middleware in reversed(self.middlewares):
                wrapped = middleware(wrapped)
            coro = wrapped(*_args, **data["k"])
            token = self._task_context.set(ctx)
            self.tasks[task_id] = self.loop.create_task(coro)
            result = None
            try:
                result = await self.tasks[task_id]
            except StreaqRetry as e:
                result = e
                success = False
                done = False
                delay = to_seconds(e.delay) if e.delay is not None else task_try**2
                if not task.silent:
                    logger.exception(e)
                    logger.info(f"retrying ↻ task {task_id} in {delay}s")
            except asyncio.TimeoutError as e:
                if not task.silent:
                    logger.error(f"task {task_id} timed out …")
                result = e
                success = False
                done = True
            except asyncio.CancelledError as e:
                if task_id in self.aborting_tasks:
                    self.aborting_tasks.remove(task_id)
                    done = True
                    if not task.silent:
                        logger.info(f"task {task_id} aborted ⊘")
                        self.counters["aborted"] += 1
                        self.counters["failed"] -= 1  # this will get incremented later
                else:
                    if not task.silent:
                        logger.info(f"task {task_id} cancelled, will be retried ↻")
                    done = False
                result = e
                success = False
            except Exception as e:
                result = e
                success = False
                done = True
                if not task.silent:
                    logger.info(f"task {task_id} failed ×")
                    logger.exception(e)
            finally:
                del self.tasks[task_id]
                finish_time = now_ms()
                await asyncio.shield(
                    self.finish_task(
                        msg,
                        finish=done,
                        delay=delay,
                        return_value=result,
                        start_time=start_time,
                        finish_time=finish_time or now_ms(),
                        success=success,
                        silent=task.silent,
                        ttl=task.ttl,
                        triggers=data.get("T"),
                        lock_key=lock_key,
                    )
                )
                self._task_context.reset(token)

    async def finish_task(
        self,
        msg: StreamMessage,
        finish: bool,
        delay: float | None,
        return_value: Any,
        start_time: int,
        finish_time: int,
        success: bool,
        silent: bool,
        ttl: timedelta | int | None,
        triggers: str | None,
        lock_key: str,
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

        def key(mid: str) -> str:
            return self._prefix + mid + task_id

        stream_key = self.stream_key + msg.priority
        pipe = await self.redis.pipeline(transaction=True)
        await pipe.xack(stream_key, self._group_name, [msg.message_id])
        await pipe.xdel(stream_key, [msg.message_id])
        await pipe.zrem(self._timeout_key + msg.priority, [msg.message_id])
        if finish:
            await pipe.publish(self._channel_key, task_id)
            if not silent:
                if success:
                    self.counters["completed"] += 1
                else:
                    self.counters["failed"] += 1
            if result and ttl != 0:
                await pipe.set(key(REDIS_RESULT), result, ex=ttl)
            await pipe.delete(
                [
                    key(REDIS_RETRY),
                    key(REDIS_TASK),
                    key(REDIS_MESSAGE),
                    lock_key,
                ]
            )
            await pipe.srem(self._abort_key, [task_id])
            if success:
                output, truncate_length = str(return_value), 32
                if len(output) > truncate_length:
                    output = f"{output[:truncate_length]}…"
                if not silent:
                    logger.info(f"task {task_id} ← {output}")
                if triggers:
                    args = self.serializer(to_tuple(return_value))
                    await pipe.set(
                        key(REDIS_RESULT) + ":raw", args, ex=timedelta(minutes=5)
                    )
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
            if not silent:
                self.counters["retried"] += 1
            await pipe.delete([key(REDIS_MESSAGE), lock_key])
            await pipe.zadd(self.queue_key, {task_id: now_ms() + delay * 1000})
        else:
            if not silent:
                self.counters["retried"] += 1
            ttl_ms = to_ms(ttl) if ttl is not None else None
            expire = (ttl_ms or 0) + DEFAULT_TTL
            await pipe.delete([lock_key])
            await self.scripts["retry_task"](
                keys=[stream_key, key(REDIS_MESSAGE)],
                args=[task_id, expire],
                client=pipe,
            )
        res = await pipe.execute()
        if finish and res[-1]:
            if success:
                pipe = await self.redis.pipeline(transaction=False)
                for dep_id in res[-1]:
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
                pipe = await self.redis.pipeline(transaction=False)
                self.counters["failed"] += len(res[-1])
                for dep_id in res[-1]:
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
        result_data: Any,
        silent: bool,
        ttl: timedelta | int | None,
    ) -> None:
        """
        Cleanup for a task that failed during execution.
        """
        task_id = msg.task_id

        def key(mid: str) -> str:
            return self._prefix + mid + task_id

        if not silent:
            self.counters["failed"] += 1
        stream_key = self.stream_key + msg.priority
        pipe = await self.redis.pipeline(transaction=True)
        await pipe.delete(
            [
                key(REDIS_RETRY),
                key(REDIS_RUNNING),
                key(REDIS_TASK),
                key(REDIS_MESSAGE),
            ]
        )
        await pipe.publish(self._channel_key, task_id)
        await pipe.srem(self._abort_key, [task_id])
        await pipe.xack(stream_key, self._group_name, [msg.message_id])
        await pipe.xdel(stream_key, [msg.message_id])
        await pipe.zrem(self._timeout_key + msg.priority, [msg.message_id])
        if result_data is not None and ttl:
            await pipe.set(key(REDIS_RESULT), result_data, ex=ttl)
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
            pipe = await self.redis.pipeline(transaction=False)
            self.counters["failed"] += len(res[-1])
            for dep_id in res[-1]:
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
        *args: Any,
        **kwargs: Any,
    ) -> Task[Any]:
        """
        Allows for enqueuing a task that is registered elsewhere without having access
        to the worker it's registered to. This is unsafe because it doesn't check if the
        task is registered with the worker and doesn't enforce types, so it should only
        be used if you need to separate the task queuing and task execution code for
        performance reasons.

        :param fn_name:
            name of the function to run, much match its __qualname__. If you're unsure,
            check ``Worker.registry``.
        :param args: positional arguments for the task
        :param kwargs: keyword arguments for the task

        :return: task object
        """
        registered = RegisteredTask(
            fn=_placeholder,
            max_tries=None,
            silent=False,
            timeout=None,
            ttl=None,
            unique=False,
            worker=self,
            _fn_name=fn_name,
        )
        return Task(args, kwargs, registered)

    async def queue_size(self) -> int:
        """
        Returns the number of tasks currently queued in Redis.
        """
        pipe = await self.redis.pipeline(transaction=True)
        for priority in TaskPriority:
            await pipe.xlen(self.stream_key + priority.value)
        await pipe.zcard(self.queue_key)
        res = await pipe.execute()

        return sum(res)

    @property
    def active(self) -> int:
        """
        The number of currently active tasks for the worker
        """
        return sum(not t.done() for t in self.tasks.values())

    def _delay_for(self, tab: CronTab) -> float:
        return tab.next(now=datetime.now(self.tz))  # type: ignore

    async def health_check(self) -> None:
        """
        Periodically stores info about the worker in Redis.
        """
        while not self._block_new_tasks:
            try:
                await asyncio.sleep(self._delay_for(self._health_tab))
                ttl = int(self._delay_for(self._health_tab)) + 5
                await self.redis.set(f"{self._health_key}:{self.id}", str(self), ex=ttl)
            except asyncio.CancelledError:
                break

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
        pipe = await self.redis.pipeline(transaction=False)
        for priority in TaskPriority:
            await pipe.xgroup_delconsumer(
                self.stream_key + priority.value,
                groupname=self._group_name,
                consumername=self.id,
            )
        await pipe.hdel(self._health_key, [self.id])
        await pipe.execute(raise_on_error=False)
        run_time = now_ms() - self._start_time
        logger.info(f"shutdown {str(self)} after {run_time}ms")

    async def _listen_for_result(self, pubsub: PubSub[str], task_id: str) -> None:
        async for msg in pubsub:
            if msg.get("data") == task_id:
                break

    async def status_by_id(self, task_id: str) -> TaskStatus:
        """
        Fetch the current status of the given task. Note that nonexistent
        tasks will return pending.

        :param task_id: ID of the task to check

        :return: status of the task
        """

        def key(mid: str) -> str:
            return self._prefix + mid + task_id

        pipe = await self.redis.pipeline(transaction=True)
        await pipe.exists([key(REDIS_RESULT)])
        await pipe.exists([key(REDIS_RUNNING)])
        await pipe.zscore(self.queue_key, task_id)
        await pipe.exists([key(REDIS_MESSAGE)])
        finished, running, score, queued = await pipe.execute()

        if finished:
            return TaskStatus.DONE
        elif running:
            return TaskStatus.RUNNING
        elif score:
            return TaskStatus.SCHEDULED
        elif queued:
            return TaskStatus.QUEUED
        return TaskStatus.PENDING

    async def result_by_id(
        self, task_id: str, timeout: timedelta | int | None = None
    ) -> TaskResult[Any]:
        """
        Wait for and return the given task's result, optionally with a timeout.

        :param task_id: ID of the task to get results for
        :param timeout: amount of time to wait before raising a `TimeoutError`

        :return: wrapped result object
        """
        result_key = self._prefix + REDIS_RESULT + task_id
        async with self.redis.pubsub(channels=[self._channel_key]) as pubsub:
            if not (raw := await self.redis.get(result_key)):
                await asyncio.wait_for(
                    self._listen_for_result(pubsub, task_id), to_seconds(timeout)
                )
        if not (raw := await self.redis.get(result_key)):
            raise StreaqError(
                "Task finished but result was not stored, did you set ttl=0?"
            )
        try:
            data = self.deserializer(raw)
            return TaskResult(
                success=data["s"],
                result=data["r"],
                start_time=data["st"],
                finish_time=data["ft"],
                queue_name=self.queue_name,
            )
        except Exception as e:
            raise StreaqError(
                f"Unable to deserialize result for task {task_id}:"
            ) from e

    async def abort_by_id(self, task_id: str, timeout: timedelta | int = 5) -> bool:
        """
        Notify workers that the task should be aborted.

        :param task_id: ID of the task to abort
        :param timeout: how long to wait to confirm abortion was successful

        :return: whether the task was aborted successfully
        """
        await self.redis.sadd(self._abort_key, [task_id])
        try:
            result = await self.result_by_id(task_id, timeout=timeout)
            return not result.success and isinstance(
                result.result, asyncio.CancelledError
            )
        except asyncio.TimeoutError:
            return False

    async def info_by_id(self, task_id: str) -> TaskData:
        """
        Fetch info about a previously enqueued task.

        :param task_id: ID of the task to get info for

        :return: task info object
        """

        def key(mid: str) -> str:
            return self._prefix + mid + task_id

        pipe = await self.redis.pipeline(transaction=False)
        await pipe.get(key(REDIS_TASK))
        await pipe.get(key(REDIS_RETRY))
        await pipe.zscore(self.queue_key, task_id)
        raw, task_try, score = await pipe.execute()
        data = self.deserializer(raw)
        dt = datetime.fromtimestamp(score / 1000, tz=self.tz) if score else None
        return TaskData(
            fn_name=data["f"],
            enqueue_time=data["t"],
            task_try=task_try,
            scheduled=dt,
        )

    async def __aenter__(self) -> Worker[WD]:
        # register lua scripts
        self.scripts.update(register_scripts(self.redis))
        # user-defined deps
        lifespan = self.lifespan(self)
        self._worker_context = await lifespan.__aenter__()
        self._stack.append(lifespan)
        return self

    async def __aexit__(
        self,
        exc_type: Type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        lifespan = self._stack.pop()
        await lifespan.__aexit__(exc_type, exc_value, traceback)

    def __len__(self) -> int:
        return len([v for v in self.registry.values() if not v.silent])

    def __str__(self) -> str:
        counters_str = dict.__repr__(self.counters).replace("'", "")  # type: ignore
        return f"worker {self.id} {counters_str}"

    def __repr__(self) -> str:
        return f"<{str(self)}>"

from __future__ import annotations

import asyncio
import hmac
import json
import pickle
import signal
from collections import defaultdict
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from contextvars import ContextVar
from datetime import datetime, timedelta, timezone, tzinfo
from functools import partial
from signal import Signals
from types import TracebackType
from typing import Any, AsyncIterator, Callable, Generic, Type, cast
from uuid import uuid4

from anyio.abc import CapacityLimiter
from coredis import PureToken, Redis
from coredis.commands import PubSub, Script
from coredis.sentinel import Sentinel
from coredis.typing import KeyT
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
    REDIS_PREFIX,
    REDIS_PREVIOUS,
    REDIS_QUEUE,
    REDIS_RESULT,
    REDIS_RETRY,
    REDIS_RUNNING,
    REDIS_STREAM,
    REDIS_TASK,
    REDIS_TIMEOUT,
    REDIS_UNIQUE,
)
from streaq.lua import (
    CREATE_GROUPS,
    FAIL_DEPENDENTS,
    PUBLISH_DELAYED_TASKS,
    PUBLISH_TASK,
    RECLAIM_IDLE_TASKS,
    UPDATE_DEPENDENTS,
)
from streaq.task import (
    RegisteredCron,
    RegisteredTask,
    StreaqRetry,
    Task,
    TaskInfo,
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
from streaq.utils import (
    StreaqError,
    asyncify,
    datetime_ms,
    now_ms,
    to_ms,
    to_seconds,
    to_tuple,
)


@asynccontextmanager
async def _lifespan(worker: Worker[None]) -> AsyncIterator[None]:
    yield None


async def _placeholder() -> None: ...


class Worker(Generic[WD]):
    """
    Worker object that fetches and executes tasks from a queue.

    :param redis_url: connection URI for Redis
    :param redis_kwargs: additional keyword arguments for Redis client
    :param concurrency: number of tasks the worker can run simultaneously
    :param sync_concurrency:
        max number of synchronous tasks the worker can run simultaneously
        in separate threads; defaults to the same as ``concurrency``
    :param queue_name: name of queue in Redis
    :param priorities: list of priorities from lowest to highest
    :param prefetch:
        max number of tasks to prefetch from Redis, defaults to same as ``concurrency``
    :param lifespan:
        async context manager that wraps worker execution and provides task
        dependencies
    :param serializer: function to serialize task data for Redis
    :param deserializer: function to deserialize task data from Redis
    :param tz: timezone to use for cron jobs
    :param handle_signals: whether to handle signals for graceful shutdown
    :param health_crontab: crontab for frequency to store health info
    :param signing_secret:
        if provided, used to sign data stored in Redis, which can improve security
        especially if using pickle. For binary serializers only. You can generate
        a key using secrets, for example: `secrets.token_urlsafe(32)`
    :param idle_timeout: the amount of time prefetched tasks wait before being requeued
    """

    create_groups: Script[str]
    publish_task: Script[str]
    publish_delayed_tasks: Script[str]
    fail_dependents: Script[str]
    update_dependents: Script[str]
    reclaim_idle_tasks: Script[str]
    _worker_context: WD

    __slots__ = (
        "redis",
        "concurrency",
        "queue_name",
        "_group_name",
        "prefetch",
        "bs",
        "counters",
        "loop",
        "_worker_context",
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
        "dependents_key",
        "dependencies_key",
        "results_key",
        "_abort_key",
        "_health_key",
        "_channel_key",
        "_timeout_key",
        "idle_timeout",
        "main_task",
        "_start_time",
        "prefix",
        "sync_concurrency",
        "_limiter",
        "_sentinel",
        "_health_tab",
        "middlewares",
        "signing_secret",
        "_task_context",
        "priorities",
        "create_groups",
        "publish_task",
        "publish_delayed_tasks",
        "fail_dependents",
        "update_dependents",
        "reclaim_idle_tasks",
    )

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        redis_sentinel_nodes: list[tuple[str, int]] | None = None,
        redis_sentinel_master: str = "mymaster",
        redis_kwargs: dict[str, Any] | None = None,
        concurrency: int = 16,
        sync_concurrency: int | None = None,
        queue_name: str = DEFAULT_QUEUE_NAME,
        priorities: list[str] | None = None,
        prefetch: int | None = None,
        lifespan: Callable[[Worker[WD]], AbstractAsyncContextManager[WD]] = _lifespan,  # type: ignore
        serializer: Callable[[Any], Any] = pickle.dumps,
        deserializer: Callable[[Any], Any] = pickle.loads,
        tz: tzinfo = timezone.utc,
        handle_signals: bool = True,
        health_crontab: str = "*/5 * * * *",
        signing_secret: str | None = None,
        idle_timeout: timedelta | int = 300,
    ):
        #: Redis connection
        redis_kwargs = redis_kwargs or {}
        if redis_kwargs.pop("decode_responses", None) is not None:
            logger.warning("decode_responses ignored in redis_kwargs")
        if redis_sentinel_nodes:
            redis_kwargs["socket_timeout"] = redis_kwargs.get("socket_timeout", 2.0)
            self._sentinel = Sentinel(
                redis_sentinel_nodes,
                decode_responses=True,
                **redis_kwargs,
            )
            self.redis = self._sentinel.primary_for(redis_sentinel_master)
        else:
            self.redis = Redis.from_url(
                redis_url, decode_responses=True, **redis_kwargs
            )
        self.concurrency = concurrency
        self.queue_name = queue_name
        self.priorities = priorities or ["default"]
        self._group_name = REDIS_GROUP
        self.prefetch = prefetch or concurrency
        #: semaphore controlling concurrency
        self.bs = asyncio.BoundedSemaphore(concurrency)
        #: mapping of type of task -> number of tasks of that type
        #: eg ``{"completed": 4, "failed": 1, "retried": 0}``
        self.counters: dict[str, int] = defaultdict(int)
        #: event loop for running tasks
        self.loop = asyncio.get_event_loop()
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
        self.signing_secret = signing_secret.encode() if signing_secret else None
        self.sync_concurrency = sync_concurrency or concurrency
        # internal objects
        self._limiter = CapacityLimiter(self.sync_concurrency)
        self._block_new_tasks = False
        self.lifespan = lifespan
        self._stack: list[AbstractAsyncContextManager[WD]] = []
        self.idle_timeout = to_ms(idle_timeout)
        self._start_time = now_ms()
        self._health_tab = CronTab(health_crontab)
        self._task_context: ContextVar[TaskContext] = ContextVar("_task_context")
        # precalculate Redis prefixes
        self.prefix = REDIS_PREFIX + self.queue_name
        self.queue_key = self.prefix + REDIS_QUEUE
        self.stream_key = self.prefix + REDIS_STREAM
        self.dependents_key = self.prefix + REDIS_DEPENDENTS
        self.dependencies_key = self.prefix + REDIS_DEPENDENCIES
        self.results_key = self.prefix + REDIS_RESULT
        self._abort_key = self.prefix + REDIS_ABORT
        self._health_key = self.prefix + REDIS_HEALTH
        self._channel_key = self.prefix + REDIS_CHANNEL
        self._timeout_key = self.prefix + REDIS_TIMEOUT

        @self.cron(health_crontab, silent=True, timeout=3, ttl=0)
        async def _() -> None:
            """
            Saves Redis health in Redis.
            """
            pipe = await self.redis.pipeline(transaction=False)
            streams = (
                pipe.xlen(self.stream_key + priority) for priority in self.priorities
            )
            queues = (
                pipe.zcard(self.queue_key + priority) for priority in self.priorities
            )
            infos = (
                pipe.info("Memory", "Clients"),
                pipe.dbsize(),
            )
            await pipe.execute()
            info, keys = await asyncio.gather(*infos)
            mem_usage = info.get("used_memory_human", "?")
            clients = info.get("connected_clients", "?")
            queued = sum(await asyncio.gather(*streams))
            scheduled = sum(await asyncio.gather(*queues))
            health = (
                f"redis {{memory: {mem_usage}, clients: {clients}, keys: {keys}, "  # type: ignore
                f"queued: {queued}, scheduled: {scheduled}}}"
            )
            ttl = int(self._delay_for(self._health_tab)) + 5
            await self.redis.set(self._health_key + ":redis", health, ex=ttl)

    def task_context(self) -> TaskContext:
        """
        Fetch task information for the currently running task.
        This can only be called from within a running task or a middleware.
        """
        try:
            return self._task_context.get()
        except LookupError as e:
            raise StreaqError(
                "Worker.task_context() can only be called within a running task or a "
                "middleware!"
            ) from e

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
            await self.create_groups(
                keys=[self.stream_key, self._group_name],
                args=self.priorities,  # type: ignore
            )
            # run loops
            tasks = [self.listen_stream(), self.health_check()]
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

    async def listen_stream(self) -> None:
        """
        Listen for new tasks or stale tasks from the stream and start them up,
        as well as add cron jobs to the queue when ready.
        """
        streams = {self.stream_key + p: ">" for p in reversed(self.priorities)}
        priority_order = {p: -i for i, p in enumerate(self.priorities)}
        while not self._block_new_tasks:
            messages: list[StreamMessage] = []
            active_tasks = self.concurrency - self.bs._value
            pending_tasks = len(self.task_wrappers)
            count = self.concurrency + self.prefetch - pending_tasks
            pipe = await self.redis.pipeline(transaction=False)
            if count > 0:
                idle = await self.reclaim_idle_tasks(
                    keys=[
                        self._timeout_key,
                        self.stream_key,
                        self._group_name,
                        self.id,
                    ],
                    args=[now_ms(), count, *self.priorities],
                )
                mapping: Any = json.loads(idle)  # type: ignore
                if mapping:  # pragma: no cover
                    for priority, _entries in mapping.items():
                        messages.extend(
                            [
                                StreamMessage(
                                    priority=priority,
                                    task_id=entry[1][1],
                                    message_id=entry[0],
                                )
                                for entry in _entries
                            ]
                        )
                    logger.info(f"retrying ↻ {len(messages)} idle tasks")
                    count -= len(messages)
                if count > 0:
                    entries = await self.redis.xreadgroup(
                        self._group_name,
                        self.id,
                        streams=streams,  # type: ignore
                        block=500,
                        count=count,
                    )
                    if entries:
                        for stream, msgs in entries.items():
                            priority = stream.split(":")[-1]
                            messages.extend(
                                [
                                    StreamMessage(
                                        message_id=msg_id,  # type: ignore
                                        task_id=msg["task_id"],  # type: ignore
                                        priority=priority,
                                    )
                                    for msg_id, msg in msgs
                                ]
                            )
                priorities: dict[str, list[str]] = defaultdict(list)
                for msg in messages:
                    priorities[msg.priority].append(msg.message_id)
                expire = now_ms() + self.idle_timeout
                for k, v in priorities.items():
                    pipe.zadd(self._timeout_key + k, {m: expire for m in v})
            self.publish_delayed_tasks(
                keys=[self.queue_key, self.stream_key],
                args=[now_ms(), *self.priorities],
                client=pipe,
            )
            command = pipe.smembers(self._abort_key)
            await pipe.execute()
            res = await command
            # Go through task_ids in the aborted tasks set and cancel those tasks.
            if res:
                aborted: set[str] = set()
                for task_id in res:
                    if task_id in self.tasks:
                        self.tasks[task_id].cancel()
                        aborted.add(task_id)
                if aborted:
                    logger.debug(f"aborting {len(aborted)} tasks in worker {self.id}")
                    self.aborting_tasks.update(aborted)
                    await self.redis.srem(self._abort_key, aborted)
            # cron jobs
            futures: set[TypedCoroutine[Task[Any]]] = set()
            ts = now_ms()
            for name, cron_job in self.cron_jobs.items():
                if ts - 500 > self.cron_schedule[name]:
                    self.cron_schedule[name] = cron_job.next()
                    futures.add(
                        cron_job.enqueue()
                        .start(schedule=cron_job.schedule())
                        ._enqueue()  # type: ignore
                    )
            if futures:
                logger.debug(f"enqueuing {len(futures)} cron jobs in worker {self.id}")
                await asyncio.gather(*futures)
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

    async def finish_failed_task(
        self,
        msg: StreamMessage,
        exc: BaseException,
        enqueue_time: int = 0,
        fn_name: str = "Unknown",
        silent: bool = False,
        ttl: timedelta | int | None = 300,
    ) -> None:
        """
        Serialize a failed task with metadata and handle failure.
        """
        now = now_ms()
        task_id = msg.task_id
        data = {
            "f": fn_name,
            "et": enqueue_time,
            "s": False,
            "r": exc,
            "st": now,
            "ft": now,
        }
        try:
            raw = self.serialize(data)
        except Exception as e:
            raise StreaqError(
                f"Failed to serialize result for task {msg.task_id}!"
            ) from e

        def key(mid: str) -> str:
            return self.prefix + mid + task_id

        if not silent:
            self.counters["failed"] += 1
        stream_key = self.stream_key + msg.priority
        pipe = await self.redis.pipeline(transaction=True)
        pipe.delete([key(REDIS_RETRY), key(REDIS_RUNNING), key(REDIS_TASK)])
        pipe.publish(self._channel_key, task_id)
        pipe.srem(self._abort_key, [task_id])
        pipe.xack(stream_key, self._group_name, [msg.message_id])
        pipe.xdel(stream_key, [msg.message_id])
        pipe.zrem(self._timeout_key + msg.priority, [msg.message_id])
        if raw is not None and ttl:
            pipe.set(key(REDIS_RESULT), raw, ex=ttl)
        command = self.fail_dependents(
            keys=[
                self.prefix + REDIS_DEPENDENTS,
                self.prefix + REDIS_DEPENDENCIES,
                task_id,
            ],
            client=pipe,
        )
        await pipe.execute()
        res = cast(list[str], await command)
        if res:
            await self.fail_dependencies(task_id, res)

    async def finish_task(
        self,
        msg: StreamMessage,
        finish: bool,
        delay: float | None,
        return_value: Any,
        start_time: int,
        finish_time: int,
        enqueue_time: int,
        fn_name: str,
        success: bool,
        silent: bool,
        ttl: timedelta | int | None,
        triggers: str | None,
        lock_key: str | None,
    ) -> None:
        """
        Cleanup for a task that executed successfully or will be retried.
        """
        data = {
            "f": fn_name,
            "et": enqueue_time,
            "s": success,
            "r": return_value,
            "st": start_time,
            "ft": finish_time,
        }
        task_id = msg.task_id
        try:
            result = self.serialize(data)
        except Exception as e:
            raise StreaqError(f"Failed to serialize result for task {task_id}!") from e

        def key(mid: str) -> str:
            return self.prefix + mid + task_id

        stream_key = self.stream_key + msg.priority
        pipe = await self.redis.pipeline(transaction=True)
        pipe.xack(stream_key, self._group_name, [msg.message_id])
        pipe.xdel(stream_key, [msg.message_id])
        pipe.zrem(self._timeout_key + msg.priority, [msg.message_id])
        to_delete: list[KeyT] = [key(REDIS_RUNNING)]
        if lock_key:
            to_delete.append(lock_key)
        if finish:
            pipe.publish(self._channel_key, task_id)
            if not silent:
                if success:
                    self.counters["completed"] += 1
                else:
                    self.counters["failed"] += 1
            if result and ttl != 0:
                pipe.set(key(REDIS_RESULT), result, ex=ttl)
            to_delete.extend([key(REDIS_RETRY), key(REDIS_TASK)])
            pipe.delete(to_delete)
            pipe.srem(self._abort_key, [task_id])
            if success:
                output, truncate_length = str(return_value), 32
                if len(output) > truncate_length:
                    output = f"{output[:truncate_length]}…"
                if not silent:
                    logger.info(f"task {task_id} ← {output}")
                if triggers:
                    args = self.serialize(to_tuple(return_value))
                    pipe.set(key(REDIS_PREVIOUS), args, ex=timedelta(minutes=5))
                script = self.update_dependents
            else:
                script = self.fail_dependents
            command = script(
                keys=[
                    self.prefix + REDIS_DEPENDENTS,
                    self.prefix + REDIS_DEPENDENCIES,
                    task_id,
                ],
                client=pipe,
            )
        elif delay:
            if not silent:
                self.counters["retried"] += 1
            pipe.delete(to_delete)
            pipe.zadd(self.queue_key + msg.priority, {task_id: now_ms() + delay * 1000})
        else:
            if not silent:
                self.counters["retried"] += 1
            pipe.delete(to_delete)
            pipe.xadd(stream_key, {"task_id": task_id})
        await pipe.execute()
        if finish and (res := cast(list[str], await command)):  # type: ignore
            if success:
                pipe = await self.redis.pipeline(transaction=False)
                for dep_id in res:
                    logger.info(f"↳ dependent {dep_id} triggered")
                    await pipe.xadd(stream_key, {"task_id": dep_id})
                await pipe.execute()
            else:
                await self.fail_dependencies(task_id, res)

    async def run_task(self, msg: StreamMessage) -> None:
        """
        Execute the registered task, then store the result in Redis.
        """
        task_id = msg.task_id

        def key(mid: str) -> str:
            return self.prefix + mid + task_id

        # acquire semaphore
        async with self.bs:
            pipe = await self.redis.pipeline(transaction=True)
            commands = (
                pipe.get(key(REDIS_TASK)),
                pipe.incr(key(REDIS_RETRY)),
                pipe.srem(self._abort_key, [task_id]),
                pipe.zrem(self._timeout_key + msg.priority, [msg.message_id]),
            )
            pipe.pexpire(key(REDIS_RETRY), DEFAULT_TTL)
            pipe.zadd(
                self._timeout_key + msg.priority,
                {msg.message_id: now_ms() + self.idle_timeout},
            )
            await pipe.execute()
            raw, task_try, abort, removed = await asyncio.gather(*commands)
            if not raw:
                logger.warning(f"task {task_id} expired †")
                return await asyncio.shield(
                    self.finish_failed_task(msg, StreaqError("Task execution failed!"))
                )
            if not removed:
                logger.warning(f"task {task_id} reclaimed ↩ from worker {self.id}")
                self.counters["relinquished"] += 1
                return

            try:
                data: dict[str, Any] = self.deserialize(raw)
            except Exception as e:
                logger.exception(f"Failed to deserialize task {task_id}!")
                return await asyncio.shield(self.finish_failed_task(msg, e))

            if (fn_name := data["f"]) not in self.registry:
                logger.error(
                    f"Missing function {fn_name}, can't execute task {task_id}!"
                )
                return await asyncio.shield(
                    self.finish_failed_task(
                        msg,
                        StreaqError("Nonexistent function!"),
                        enqueue_time=data["t"],
                        fn_name=data["f"],
                    )
                )
            task = self.registry[fn_name]

            if abort:
                if not task.silent:
                    logger.info(f"task {task_id} aborted ⊘ prior to run")
                return await asyncio.shield(
                    self.finish_failed_task(
                        msg,
                        asyncio.CancelledError(),
                        enqueue_time=data["t"],
                        fn_name=data["f"],
                        silent=task.silent,
                        ttl=task.ttl,
                    )
                )
            if task.max_tries and task_try > task.max_tries:
                if not task.silent:
                    logger.warning(
                        f"task {task_id} failed × after {task.max_tries} retries"
                    )
                return await asyncio.shield(
                    self.finish_failed_task(
                        msg,
                        StreaqError(f"Max retry attempts reached for task {task_id}!"),
                        enqueue_time=data["t"],
                        fn_name=data["f"],
                        silent=task.silent,
                        ttl=task.ttl,
                    )
                )
            start_time = now_ms()
            timeout = (
                None
                if task.timeout is None
                else start_time + 1000 + to_ms(task.timeout)
            )
            after = data.get("A")
            pipe = await self.redis.pipeline(transaction=True)
            _removed = pipe.zrem(self._timeout_key + msg.priority, [msg.message_id])
            if task.unique:
                lock_key = self.prefix + REDIS_UNIQUE + task.fn_name
                locked = pipe.set(
                    lock_key, task_id, condition=PureToken.NX, pxat=timeout
                )
            else:
                lock_key = None
            pipe.set(key(REDIS_RUNNING), 1, pxat=timeout)
            if timeout:
                pipe.zadd(self._timeout_key + msg.priority, {msg.message_id: timeout})
            if after:
                previous = pipe.get(self.prefix + REDIS_PREVIOUS + after)
            await pipe.execute()
            if not await _removed:
                logger.warning(f"task {task_id} reclaimed ↩ from worker {self.id}")
                self.counters["relinquished"] += 1
                return
            if task.unique and not await locked:  # type: ignore
                if not task.silent:
                    logger.warning(f"unique task {task_id} clashed ↯ with running task")
                return await asyncio.shield(
                    self.finish_failed_task(
                        msg,
                        StreaqError(
                            "Task is unique and another instance of the same task is "
                            "already running!"
                        ),
                        enqueue_time=data["t"],
                        fn_name=data["f"],
                        silent=task.silent,
                        ttl=task.ttl,
                    )
                )
            _args = data["a"] if not after else self.deserialize(await previous)  # type: ignore

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
                # don't start if we're shutting down
                if self._block_new_tasks:
                    self.tasks[task_id].cancel()
                result = await self.tasks[task_id]
            except StreaqRetry as e:
                result = e
                success = False
                done = False
                delay = to_seconds(e.delay) if e.delay is not None else task_try**2
                if not task.silent:
                    logger.exception(f"Retrying task {task_id}!")
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
                    logger.exception(f"Task {task_id} failed!")
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
                        enqueue_time=data["t"],
                        fn_name=data["f"],
                        success=success,
                        silent=task.silent,
                        ttl=task.ttl,
                        triggers=data.get("T"),
                        lock_key=lock_key,
                    )
                )
                self._task_context.reset(token)

    async def fail_dependencies(self, task_id: str, dependencies: list[str]) -> None:
        """
        Fail dependencies for the given task.
        """
        now = now_ms()
        failure = {
            "s": False,
            "r": StreaqError("Dependency failed, not running task!"),
            "st": now,
            "ft": now,
            "et": 0,
            "f": "Unknown",
        }
        try:
            result = self.serialize(failure)
        except Exception as e:
            raise StreaqError(f"Failed to serialize result for task {task_id}!") from e
        pipe = await self.redis.pipeline(transaction=False)
        self.counters["failed"] += len(dependencies)
        to_delete: list[KeyT] = []
        for dep_id in dependencies:
            logger.info(f"task {dep_id} dependency failed ×")
            to_delete.append(self.prefix + REDIS_TASK + dep_id)
            pipe.set(self.results_key + dep_id, result, ex=300)
            pipe.publish(self._channel_key, dep_id)
        pipe.delete(to_delete)
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

    async def enqueue_many(self, tasks: list[Task[Any]]) -> None:
        """
        Enqueue multiple tasks for immediate execution. This uses a Redis
        pipeline, so it's more efficient than awaiting each individual task.

        :param tasks: list of task objects to enqueue

        Example usage::

            # importantly, we're not using `await` here
            tasks = [foobar.enqueue(i) for i in range(10)]
            async with worker:
                await worker.enqueue_many(tasks)

        """
        if not self._stack:
            raise StreaqError(
                "Worker did not initialize correctly, are you using the async context "
                "manager?"
            )
        enqueue_time = now_ms()
        pipe = await self.redis.pipeline(transaction=False)
        for task in tasks:
            if task._after:  # type: ignore
                task.after.append(task._after.id)  # type: ignore
            if task.schedule:
                score = datetime_ms(task.schedule)
            elif task.delay is not None:
                score = enqueue_time + to_ms(task.delay)
            else:
                score = 0
            data = task.serialize(enqueue_time)
            _priority = task.priority or self.priorities[0]
            self.publish_task(
                keys=[
                    self.stream_key,
                    self.queue_key,
                    task.task_key(REDIS_TASK),
                    self.dependents_key,
                    self.dependencies_key,
                    self.results_key,
                ],
                args=[task.id, data, _priority, score] + task.after,
                client=pipe,
            )
        await pipe.execute()

    async def queue_size(self) -> int:
        """
        Returns the number of tasks currently queued in Redis.
        """
        pipe = await self.redis.pipeline(transaction=True)
        commands = [
            pipe.xlen(self.stream_key + priority) for priority in self.priorities
        ] + [pipe.zcard(self.queue_key + priority) for priority in self.priorities]
        await pipe.execute()
        return sum(await asyncio.gather(*commands))

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
        for t in self.tasks.values():
            if not t.done():
                t.cancel()
        self.main_task.cancel()
        await asyncio.gather(*self.task_wrappers.values(), self.main_task)
        run_time = now_ms() - self._start_time
        logger.info(f"shutdown {str(self)} after {run_time}ms")

    async def _listen_for_result(self, pubsub: PubSub[str], task_id: str) -> None:
        async for msg in pubsub:
            if msg.get("data") == task_id:
                break

    async def status_by_id(self, task_id: str) -> TaskStatus:
        """
        Fetch the current status of the given task.

        :param task_id: ID of the task to check

        :return: status of the task
        """

        def key(mid: str) -> str:
            return self.prefix + mid + task_id

        pipe = await self.redis.pipeline(transaction=True)
        delayed = [
            pipe.zscore(self.queue_key + priority, task_id)
            for priority in self.priorities
        ]
        commands = (
            pipe.exists([key(REDIS_RESULT)]),
            pipe.exists([key(REDIS_RUNNING)]),
            pipe.exists([key(REDIS_TASK)]),
            pipe.exists([key(REDIS_DEPENDENCIES)]),
        )
        await pipe.execute()
        done, running, data, dependencies = await asyncio.gather(*commands)

        if done:
            return TaskStatus.DONE
        elif running:
            return TaskStatus.RUNNING
        score = any(r for r in await asyncio.gather(*delayed))
        if score or dependencies:
            return TaskStatus.SCHEDULED
        elif data:
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
        result_key = self.results_key + task_id
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
            data = self.deserialize(raw)
            return TaskResult(
                fn_name=data["f"],
                enqueue_time=data["et"],
                success=data["s"],
                result=data["r"],
                start_time=data["st"],
                finish_time=data["ft"],
            )
        except Exception as e:
            raise StreaqError(
                f"Unable to deserialize result for task {task_id}:"
            ) from e

    async def abort_by_id(self, task_id: str, timeout: timedelta | int = 5) -> bool:
        """
        Notify workers that the task should be aborted if it's running.
        If the task is still enqueued, it will not be removed from the queue,
        but it will not be started when it gets dequeued.

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

    async def info_by_id(self, task_id: str) -> TaskInfo:
        """
        Fetch info about a previously enqueued task.

        :param task_id: ID of the task to get info for

        :return: task info object
        """

        def key(mid: str) -> str:
            return self.prefix + mid + task_id

        pipe = await self.redis.pipeline(transaction=False)
        delayed = [
            pipe.zscore(self.queue_key + priority, task_id)
            for priority in self.priorities
        ]
        commands = (
            pipe.get(key(REDIS_TASK)),
            pipe.get(key(REDIS_RETRY)),
            pipe.smembers(key(REDIS_DEPENDENCIES)),
            pipe.smembers(key(REDIS_DEPENDENTS)),
        )
        await pipe.execute()
        raw, try_count, dependencies, dependents = await asyncio.gather(*commands)
        data = self.deserialize(raw)
        res = await asyncio.gather(*delayed)
        score = next((r for r in res if r), None)
        dt = datetime.fromtimestamp(score / 1000, tz=self.tz) if score else None
        return TaskInfo(
            fn_name=data["f"],
            enqueue_time=data["t"],
            task_try=int(try_count or 0),
            scheduled=dt,
            dependencies=dependencies,
            dependents=dependents,
        )

    async def __aenter__(self) -> Worker[WD]:
        # register lua scripts
        self.create_groups = self.redis.register_script(CREATE_GROUPS)
        self.publish_task = self.redis.register_script(PUBLISH_TASK)
        self.publish_delayed_tasks = self.redis.register_script(PUBLISH_DELAYED_TASKS)
        self.fail_dependents = self.redis.register_script(FAIL_DEPENDENTS)
        self.update_dependents = self.redis.register_script(UPDATE_DEPENDENTS)
        self.reclaim_idle_tasks = self.redis.register_script(RECLAIM_IDLE_TASKS)
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

    def serialize(self, data: Any) -> Any:
        """
        Wrap serializer to append signature as last 32 bytes if applicable.
        """
        serialized = self.serializer(data)
        if self.signing_secret:
            try:
                # will only work if data is binary data
                serialized += hmac.digest(self.signing_secret, serialized, "sha256")
            except TypeError as e:
                raise StreaqError("Can't sign non-binary data from serializer!") from e
        return serialized

    def deserialize(self, data: Any) -> Any:
        """
        Wrap deserializer to validate signature from last 32 bytes if applicable.
        """
        if self.signing_secret:
            try:
                data_bytes, signature = data[:-32], data[-32:]
                verify = hmac.digest(self.signing_secret, data_bytes, "sha256")
                if not hmac.compare_digest(signature, verify):
                    raise StreaqError("Invalid signature for task data!")
                return self.deserializer(data_bytes)
            except IndexError as e:
                raise StreaqError("Missing signature for task data!") from e
        return self.deserializer(data)

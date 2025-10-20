from __future__ import annotations

import hmac
import pickle
import signal
from collections import defaultdict
from contextlib import AbstractAsyncContextManager, AsyncExitStack, asynccontextmanager
from contextvars import ContextVar
from datetime import datetime, timedelta, timezone, tzinfo
from inspect import iscoroutinefunction
from sys import platform
from textwrap import shorten
from typing import Any, AsyncGenerator, Callable, Generic, Literal, cast
from uuid import uuid4

from anyio import (
    TASK_STATUS_IGNORED,
    AsyncContextManagerMixin,
    CancelScope,
    CapacityLimiter,
    Path,
    create_memory_object_stream,
    create_task_group,
    current_time,
    fail_after,
    get_cancelled_exc_class,
    move_on_after,
    open_signal_receiver,
    run,
    sleep,
)
from anyio.abc import TaskStatus as AnyStatus
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
from coredis import PureToken, Redis
from coredis.response._callbacks.streams import MultiStreamRangeCallback
from coredis.sentinel import Sentinel
from coredis.typing import KeyT
from crontab import CronTab
from typing_extensions import Self

from streaq import logger
from streaq.constants import (
    DEFAULT_QUEUE_NAME,
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
    REDIS_UNIQUE,
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
    AsyncCron,
    AsyncTask,
    C,
    CronDefinition,
    Middleware,
    P,
    R,
    StreamMessage,
    SyncCron,
    SyncTask,
    TaskContext,
    TaskDefinition,
)
from streaq.utils import (
    StreaqCancelled,
    StreaqError,
    asyncify,
    datetime_ms,
    gather,
    now_ms,
    to_ms,
    to_seconds,
    to_tuple,
)


@asynccontextmanager
async def _lifespan() -> AsyncGenerator[None]:
    yield None


async def _placeholder() -> None: ...


class Worker(AsyncContextManagerMixin, Generic[C]):
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
    :param idle_timeout:
        the amount of time to wait before re-enqueuing idle tasks (either prefetched
        tasks that don't run, or running tasks that become unresponsive)
    :param anyio_backend: anyio backend to use, either Trio or asyncio
    :param anyio_kwargs: extra arguments to pass to anyio backend
    :param sentinel_nodes: list of (address, port) tuples to create sentinel from
    :param sentinel_master: name of sentinel master to use
    :param sentinel_kwargs: extra arguments to pass to sentinel (but not instances)
    """

    _worker_context: C

    __slots__ = (
        "_redis",
        "concurrency",
        "queue_name",
        "_group_name",
        "prefetch",
        "counters",
        "_worker_context",
        "registry",
        "cron_jobs",
        "cron_schedule",
        "id",
        "serializer",
        "deserializer",
        "_cancel_scopes",
        "_running_tasks",
        "tz",
        "burst",
        "trio",
        "_handle_signals",
        "_block_new_tasks",
        "lifespan",
        "queue_key",
        "stream_key",
        "dependents_key",
        "dependencies_key",
        "results_key",
        "_abort_key",
        "_health_key",
        "_channel_key",
        "idle_timeout",
        "_running",
        "prefix",
        "sync_concurrency",
        "_limiter",
        "_sentinel",
        "_health_tab",
        "middlewares",
        "signing_secret",
        "_task_context",
        "priorities",
        "_cancelled_class",
        "_initialized",
    )

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        redis_kwargs: dict[str, Any] | None = None,
        concurrency: int = 16,
        sync_concurrency: int | None = None,
        queue_name: str = DEFAULT_QUEUE_NAME,
        priorities: list[str] | None = None,
        prefetch: int | None = None,
        lifespan: Callable[[], AbstractAsyncContextManager[C]] = _lifespan,
        serializer: Callable[[Any], Any] = pickle.dumps,
        deserializer: Callable[[Any], Any] = pickle.loads,
        tz: tzinfo = timezone.utc,
        handle_signals: bool = True,
        health_crontab: str = "*/5 * * * *",
        signing_secret: str | None = None,
        idle_timeout: timedelta | int = 60,
        anyio_backend: Literal["asyncio", "trio"] = "asyncio",
        anyio_kwargs: dict[str, Any] | None = None,
        sentinel_nodes: list[tuple[str, int]] | None = None,
        sentinel_master: str = "mymaster",
        sentinel_kwargs: dict[str, Any] | None = None,
        id: str | None = None,
    ):
        # Redis connection
        redis_kwargs = redis_kwargs or {}
        if redis_kwargs.pop("decode_responses", None) is not None:
            logger.warning("decode_responses ignored in redis_kwargs")
        if sentinel_nodes:
            self._sentinel = Sentinel(
                sentinel_nodes,
                decode_responses=True,
                sentinel_kwargs=sentinel_kwargs,
                **redis_kwargs,
            )
            self._redis = self._sentinel.primary_for(sentinel_master)
        else:
            self._sentinel = None
            self._redis = Redis.from_url(
                redis_url, decode_responses=True, **redis_kwargs
            )
        # user-facing properties
        self.concurrency = concurrency
        self.queue_name = queue_name
        self.priorities = priorities or ["normal"]
        self.priorities.reverse()
        self.prefetch = prefetch or concurrency
        #: mapping of type of task -> number of tasks of that type
        #: eg ``{"completed": 4, "failed": 1, "retried": 0}``
        self.counters: dict[str, int] = defaultdict(int)
        #: mapping of task name -> task wrapper
        self.registry: dict[
            str, RegisteredCron[Any, Any] | RegisteredTask[Any, Any, Any]
        ] = {}
        #: mapping of task name -> cron wrapper
        self.cron_jobs: dict[str, RegisteredCron[Any, Any]] = {}
        #: mapping of task name -> next execution time in ms
        self.cron_schedule: dict[str, int] = defaultdict(int)
        #: unique ID of worker
        self.id = id or uuid4().hex[:8]
        self.serializer = serializer
        self.deserializer = deserializer
        self.tz = tz
        #: whether to shut down the worker when the queue is empty; set via CLI
        self.burst = False
        # save anyio configuration
        self.anyio_backend = anyio_backend
        self.anyio_kwargs = anyio_kwargs or {}
        if self.anyio_backend == "asyncio" and "use_uvloop" not in self.anyio_kwargs:
            self.anyio_kwargs["use_uvloop"] = platform != "win32"
        #: list of middlewares added to the worker
        self.middlewares: list[Middleware] = []
        self.signing_secret = signing_secret.encode() if signing_secret else None
        self.sync_concurrency = sync_concurrency or concurrency
        # internal objects
        self._group_name = REDIS_GROUP
        self._handle_signals = handle_signals
        self._running = False
        self._cancel_scopes: dict[str, CancelScope] = {}
        self._running_tasks: dict[str, set[str]] = defaultdict(set)
        self._limiter = CapacityLimiter(self.sync_concurrency)
        self._block_new_tasks = False
        self.lifespan = lifespan()
        self.idle_timeout = to_ms(idle_timeout)
        self._health_tab = CronTab(health_crontab)
        self._task_context: ContextVar[TaskContext] = ContextVar("_task_context")
        self._initialized = False
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

        @self.cron(health_crontab, silent=True, ttl=0, name="redis_health_check")
        async def _() -> None:
            """
            Saves Redis health in Redis.
            """
            async with self.redis.pipeline(transaction=False) as pipe:
                streams = [
                    pipe.xlen(self.stream_key + priority)
                    for priority in self.priorities
                ]
                queues = [
                    pipe.zcard(self.queue_key + priority)
                    for priority in self.priorities
                ]
                infos = (
                    pipe.info("Memory", "Clients"),
                    pipe.dbsize(),
                )
            info, keys = await gather(*infos)
            mem_usage = info.get("used_memory_human", "?")
            clients = info.get("connected_clients", "?")
            queued = sum(await gather(*streams))
            scheduled = sum(await gather(*queues))
            health = (
                f"redis {{memory: {mem_usage}, clients: {clients}, keys: {keys}, "  # type: ignore
                f"queued: {queued}, scheduled: {scheduled}}}"
            )
            ttl = int(self._delay_for(self._health_tab)) + 5
            await self.redis.set(self._health_key + ":redis", health, ex=ttl)

    def __len__(self) -> int:
        return len([v for v in self.registry.values() if not v.silent])

    def __str__(self) -> str:
        counters = {k: v for k, v in self.counters.items() if v}
        counters_str = repr(counters).replace("'", "")
        return f"worker {self.id} {counters_str}"

    @asynccontextmanager
    async def __asynccontextmanager__(self) -> AsyncGenerator[Self]:
        async with AsyncExitStack() as stack:
            if self._sentinel:
                await stack.enter_async_context(
                    self._sentinel.__asynccontextmanager__()
                )
            await stack.enter_async_context(self._redis.__asynccontextmanager__())
            # register lua scripts from library
            text = await (Path(__file__).parent / "lua/streaq.lua").read_text()
            await self._redis.register_library("streaq", text, replace=True)
            self._cancelled_class = get_cancelled_exc_class()
            self._initialized = True
            yield self
        self._initialized = False

    @property
    def redis(self) -> Redis[str]:
        if not self._initialized:
            raise StreaqError("Worker not initialized, use the async context manager!")
        return self._redis

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
    def context(self) -> C:
        """
        Worker dependencies initialized upon worker startup.
        This can only be called from within a running task or a middleware.
        """
        if not self._running:
            raise StreaqError(
                "Worker.context can only be accessed within a running task or a "
                "middleware!"
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
        name: str | None = None,
        silent: bool = False,
        timeout: timedelta | int | None = timedelta(hours=1),
        ttl: timedelta | int | None = timedelta(minutes=5),
        unique: bool = True,
    ) -> CronDefinition[C]:
        """
        Registers a task to be run at regular intervals as specified.

        :param tab:
            crontab for scheduling, follows the specification
            `here <https://github.com/josiahcarlson/parse-crontab?tab=readme-ov-file#description>`_.
        :param max_tries:
            number of times to retry the task should it fail during execution
        :param name: use a custom name for the cron job instead of the function name
        :param silent:
            whether to silence task logs and success/failure tracking; defaults to False
        :param timeout: time after which to abort the task, if None will never time out
        :param ttl: time to store results in Redis, if None will never expire
        :param unique: whether multiple instances of the task can exist simultaneously
        """

        def wrapped(fn: AsyncCron[R] | SyncCron[R]) -> RegisteredCron[C, R]:
            if iscoroutinefunction(fn):
                _fn = fn
            else:
                _fn = asyncify(fn, self._limiter)
            if unique and timeout is None:
                raise StreaqError("Unique tasks must have a timeout set!")
            task = RegisteredCron(
                fn=cast(AsyncCron[R], _fn),
                crontab=CronTab(tab),
                max_tries=max_tries,
                silent=silent,
                timeout=timeout,
                ttl=ttl,
                unique=unique,
                worker=self,
                _fn_name=name,
            )
            if task.fn_name in self.registry:
                raise StreaqError(
                    f"A task named {task.fn_name} has already been registered!"
                )
            self.cron_jobs[task.fn_name] = task
            self.registry[task.fn_name] = task
            return task

        return wrapped  # type: ignore

    def task(
        self,
        *,
        expire: timedelta | int | None = None,
        max_tries: int | None = 3,
        name: str | None = None,
        silent: bool = False,
        timeout: timedelta | int | None = None,
        ttl: timedelta | int | None = timedelta(minutes=5),
        unique: bool = False,
    ) -> TaskDefinition[C]:
        """
        Registers a task with the worker which can later be enqueued by the user.

        :param expire:
            time after which to dequeue the task, if None will never be dequeued
        :param max_tries:
            number of times to retry the task should it fail during execution
        :param name: use a custom name for the task instead of the function name
        :param silent:
            whether to silence task logs and success/failure tracking; defaults to False
        :param timeout: time after which to abort the task, if None will never time out
        :param ttl: time to store results in Redis, if None will never expire
        :param unique: whether multiple instances of the task can exist simultaneously
        """

        def wrapped(
            fn: AsyncTask[P, R] | SyncTask[P, R],
        ) -> RegisteredTask[C, P, R]:
            if iscoroutinefunction(fn):
                _fn = fn
            else:
                _fn = asyncify(fn, self._limiter)
            if unique and timeout is None:
                raise StreaqError("Unique tasks must have a timeout set!")
            task = RegisteredTask(
                fn=cast(AsyncTask[P, R], _fn),
                expire=expire,
                max_tries=max_tries,
                silent=silent,
                timeout=timeout,
                ttl=ttl,
                unique=unique,
                worker=self,
                _fn_name=name,
            )
            if task.fn_name in self.registry:
                raise StreaqError(
                    f"A task named {task.fn_name} has already been registered!"
                )
            self.registry[task.fn_name] = task
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
        run(
            self.run_async,
            backend=self.anyio_backend,
            backend_options=self.anyio_kwargs,
        )

    async def run_async(
        self, *, task_status: AnyStatus[None] = TASK_STATUS_IGNORED
    ) -> None:
        """
        Async function to run the worker, finally closes worker connections.
        Groups together and runs worker tasks.
        """
        logger.info(f"starting worker {self.id} for {len(self)} functions")
        start_time = current_time()
        # run user-defined initialization code
        async with self, self.lifespan as context:
            self._worker_context = context
            self._running = True
            # create consumer group if it doesn't exist
            await self.redis.fcall(
                "create_groups",
                keys=[self.stream_key, self._group_name],
                args=self.priorities,  # type: ignore
            )
            task_status.started()
            # start tasks
            try:
                send, receive = create_memory_object_stream[StreamMessage](
                    max_buffer_size=self.prefetch + self.concurrency
                )
                limiter = CapacityLimiter(self.concurrency)
                async with create_task_group() as tg:
                    # register signal handler
                    tg.start_soon(self.signal_handler, tg.cancel_scope)
                    tg.start_soon(self.health_check)
                    tg.start_soon(self.producer, send, limiter, tg.cancel_scope)
                    tg.start_soon(self.renew_idle_timeouts)
                    for _ in range(self.concurrency):
                        tg.start_soon(self.consumer, receive.clone(), limiter)
            finally:
                run_time = round((current_time() - start_time) * 1000)
                logger.info(f"shutdown {str(self)} after {run_time}ms")

    async def consumer(
        self, queue: MemoryObjectReceiveStream[StreamMessage], limiter: CapacityLimiter
    ) -> None:
        """
        Listen for and run tasks from the queue.
        """
        with queue:
            async for msg in queue:
                async with limiter:
                    await self.run_task(msg)

    async def renew_idle_timeouts(self) -> None:
        """
        Periodically renew idle timeout for running tasks. This allows the queue to
        be resilient to sudden shutdowns.
        """
        timeout = self.idle_timeout / 1000 * 0.9  # 10% buffer
        while True:
            await sleep(timeout)
            async with self.redis.pipeline(transaction=True) as pipe:
                for priority, tasks in self._running_tasks.items():
                    if tasks:
                        pipe.xclaim(
                            self.stream_key + priority,
                            self._group_name,
                            self.id,
                            0,
                            tasks,
                        )

    async def producer(
        self,
        queue: MemoryObjectSendStream[StreamMessage],
        limiter: CapacityLimiter,
        scope: CancelScope,
    ) -> None:
        """
        Listen for new tasks or stale tasks from the stream and add them to the queue.
        Also handles cron jobs, task abortion, and scheduling delayed tasks.
        """
        streams = {self.stream_key + p: ">" for p in self.priorities}
        priority_order = {self.stream_key + p: i for i, p in enumerate(self.priorities)}
        prefetch = self.concurrency + self.prefetch
        with queue:
            while not self._block_new_tasks:
                messages: list[StreamMessage] = []
                start_time = current_time()
                # Calculate how many messages to fetch to fill the buffer
                count = prefetch - limiter.borrowed_tokens - len(queue._state.buffer)  # type: ignore
                if count == 0:
                    # If we don't have space wait up to half a second for it to free up
                    with move_on_after(0.5):
                        # Acquire and release immediately, triggers when a task finishes
                        async with limiter:
                            count = (
                                prefetch
                                - limiter.borrowed_tokens
                                - len(queue._state.buffer)  # type: ignore
                            )
                # Fetch new messages
                if count > 0:
                    # non-blocking, priority ordered first
                    res = await self.redis.fcall(
                        "read_streams",
                        keys=[self.stream_key, self._group_name, self.id],
                        args=[count, self.idle_timeout, *self.priorities],
                    )
                    entries = MultiStreamRangeCallback[str]()(res)
                    # blocking second if nothing fetched
                    if not entries:
                        elapsed_ms = 500 - round((current_time() - start_time) * 1000)
                        if elapsed_ms > 0:
                            entries = await self.redis.xreadgroup(
                                self._group_name,
                                self.id,
                                streams=streams,  # type: ignore
                                block=elapsed_ms,
                                count=count,
                            )
                    if entries:
                        for stream, msgs in sorted(
                            entries.items(), key=lambda item: priority_order[item[0]]
                        ):
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
                        # start new tasks
                        logger.debug(
                            f"fetched {len(messages)} tasks in worker {self.id}"
                        )
                        for msg in messages:
                            # this will succeed since we manually compute quantity
                            queue.send_nowait(msg)
                # schedule delayed tasks
                async with self.redis.pipeline(transaction=False) as pipe:
                    pipe.fcall(
                        "publish_delayed_tasks",
                        keys=[self.queue_key, self.stream_key],
                        args=[now_ms(), *self.priorities],
                    )
                    command = pipe.smembers(self._abort_key)
                aborted = await command
                # aborted tasks
                if aborted:
                    await self.abort_tasks(aborted)
                # cron jobs
                await self.schedule_cron_jobs()
                # wrap things up if we burstin'
                if self.burst and not messages and limiter.borrowed_tokens == 0:
                    self._block_new_tasks = True
                    scope.cancel()

    async def abort_tasks(self, tasks: set[str]) -> None:
        """
        Aborts tasks scheduled for abortion if they're present on this worker.
        """
        for task_id in tasks:
            if (
                task_id in self._cancel_scopes
                and not self._cancel_scopes[task_id].cancel_called
            ):
                self._cancel_scopes[task_id].cancel()
                logger.debug(
                    f"task ⊘ {task_id} marked for abortion in worker {self.id}"
                )

    async def schedule_cron_jobs(self) -> None:
        """
        Schedules any pending cron jobs for future execution.
        """
        cron_jobs: list[Task[Any]] = []
        ts = now_ms()
        for name, cron_job in self.cron_jobs.items():
            if ts - 500 > self.cron_schedule[name]:
                self.cron_schedule[name] = cron_job.next()
                cron_jobs.append(cron_job.enqueue().start(schedule=cron_job.schedule()))
        if cron_jobs:
            await self.enqueue_many(cron_jobs)
            logger.debug(f"enqueuing {len(cron_jobs)} cron jobs in worker {self.id}")

    async def finish_failed_task(
        self,
        msg: StreamMessage,
        exc: BaseException,
        tries: int,
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
            "t": tries,
            "w": self.id,
        }
        raw = self.serialize(data)

        def key(mid: str) -> str:
            return self.prefix + mid + task_id

        self.counters["failed"] += 1
        stream_key = self.stream_key + msg.priority
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.delete([key(REDIS_RETRY), key(REDIS_RUNNING), key(REDIS_TASK)])
            pipe.publish(self._channel_key + task_id, raw)
            pipe.srem(self._abort_key, [task_id])
            pipe.xack(stream_key, self._group_name, [msg.message_id])
            pipe.xdel(stream_key, [msg.message_id])
            if raw is not None and ttl:
                pipe.set(key(REDIS_RESULT), raw, ex=ttl)
            command = pipe.fcall(
                "fail_dependents",
                keys=[
                    self.prefix + REDIS_DEPENDENTS,
                    self.prefix + REDIS_DEPENDENCIES,
                    task_id,
                ],
            )
        if res := await command:
            await self.fail_task_dependents(res)  # type: ignore

    async def finish_task(
        self,
        msg: StreamMessage,
        finish: bool,
        schedule: int | None,
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
        tries: int,
    ) -> None:
        """
        Cleanup for a task that executed successfully or will be retried.
        """
        task_id = msg.task_id

        def key(mid: str) -> str:
            return self.prefix + mid + task_id

        stream_key = self.stream_key + msg.priority
        to_delete: list[KeyT] = [key(REDIS_RUNNING)]
        if lock_key:
            to_delete.append(lock_key)
        if finish:
            data = {
                "f": fn_name,
                "et": enqueue_time,
                "s": success,
                "r": return_value,
                "st": start_time,
                "ft": finish_time,
                "t": tries,
                "w": self.id,
            }
            result = self.serialize(data)
            async with self.redis.pipeline(transaction=True) as pipe:
                pipe.xack(stream_key, self._group_name, [msg.message_id])
                pipe.xdel(stream_key, [msg.message_id])
                pipe.publish(self._channel_key + task_id, result)
                if success:
                    self.counters["completed"] += 1
                else:
                    self.counters["failed"] += 1
                if ttl != 0:
                    pipe.set(key(REDIS_RESULT), result, ex=ttl)
                to_delete.extend([key(REDIS_RETRY), key(REDIS_TASK)])
                pipe.delete(to_delete)
                pipe.srem(self._abort_key, [task_id])
                if success:
                    if not silent:
                        output = shorten(str(return_value), width=32)
                        logger.info(f"task {fn_name} ■ {task_id} ← {output}")
                    if triggers:
                        args = self.serialize(to_tuple(return_value))
                        pipe.set(key(REDIS_PREVIOUS), args, ex=timedelta(minutes=5))
                    script = "update_dependents"
                else:
                    script = "fail_dependents"
                command = pipe.fcall(
                    script,
                    keys=[
                        self.prefix + REDIS_DEPENDENTS,
                        self.prefix + REDIS_DEPENDENCIES,
                        task_id,
                    ],
                )
            if res := cast(list[str], await command):
                if success:
                    async with self.redis.pipeline(transaction=False) as pipe:
                        for dep_id in res:
                            logger.info(f"↳ dependent {dep_id} triggered")
                            pipe.xadd(stream_key, {"task_id": dep_id})
                else:
                    await self.fail_task_dependents(res)
        elif schedule:
            async with self.redis.pipeline(transaction=True) as pipe:
                pipe.xack(stream_key, self._group_name, [msg.message_id])
                pipe.xdel(stream_key, [msg.message_id])
                pipe.delete(to_delete)
                pipe.zadd(self.queue_key + msg.priority, {task_id: schedule})

    async def prepare_task(
        self, msg: StreamMessage
    ) -> (
        tuple[
            RegisteredCron[Any, Any] | RegisteredTask[Any, Any, Any],
            dict[str, Any],
            int,
            tuple[Any, ...],
            str | None,
        ]
        | None
    ):
        """
        Prepare task for execution. If something goes wrong, handles failure and
        returns nothing.
        """
        task_id = msg.task_id

        def key(mid: str) -> str:
            return self.prefix + mid + task_id

        async with self.redis.pipeline(transaction=True) as pipe:
            commands = (
                pipe.get(key(REDIS_TASK)),
                pipe.incr(key(REDIS_RETRY)),
                pipe.srem(self._abort_key, [task_id]),
                pipe.xclaim(
                    self.stream_key + msg.priority,
                    self._group_name,
                    self.id,
                    0,
                    [msg.message_id],
                    justid=True,
                ),
            )
        raw, task_try, abort, active = await gather(*commands)
        if not raw:
            logger.warning(f"task † {task_id} expired")
            return await self.finish_failed_task(
                msg, StreaqError("Task expired!"), task_try
            )
        if not active:
            logger.warning(f"task ↩ {task_id} reclaimed from worker {self.id}")
            self.counters["relinquished"] += 1
            return None

        try:
            data = self.deserialize(raw)
        except StreaqError as e:
            logger.error(f"task ☒ {task_id} failed to deserialize")
            return await self.finish_failed_task(msg, e, task_try)

        if (fn_name := data["f"]) not in self.registry:
            logger.error(f"task {fn_name} ⊘ {task_id} aborted, missing function")
            return await self.finish_failed_task(
                msg,
                StreaqError(f"Missing function {fn_name}!"),
                task_try,
                enqueue_time=data["t"],
                fn_name=data["f"],
            )
        task = self.registry[fn_name]

        if abort:
            if not task.silent:
                logger.info(f"task {fn_name} ⊘ {task_id} aborted prior to run")
            return await self.finish_failed_task(
                msg,
                StreaqCancelled("Task aborted prior to run!"),
                task_try,
                enqueue_time=data["t"],
                fn_name=fn_name,
                silent=task.silent,
                ttl=task.ttl,
            )
        if task.max_tries and task_try > task.max_tries:
            if not task.silent:
                logger.warning(
                    f"task {fn_name} × {task_id} failed after {task.max_tries} retries"
                )
            return await self.finish_failed_task(
                msg,
                StreaqError("Max retry attempts reached for task!"),
                task_try,
                enqueue_time=data["t"],
                fn_name=fn_name,
                silent=task.silent,
                ttl=task.ttl,
            )

        timeout = (
            None if task.timeout is None else self.idle_timeout + to_ms(task.timeout)
        )
        after = data.get("A")
        async with self.redis.pipeline(transaction=False) as pipe:
            if task.unique:
                lock_key = self.prefix + REDIS_UNIQUE + fn_name
                locked = pipe.set(
                    lock_key, task_id, get=True, condition=PureToken.NX, px=timeout
                )
            else:
                lock_key = None
            pipe.set(key(REDIS_RUNNING), 1, px=timeout)
            if after:
                previous = pipe.get(self.prefix + REDIS_PREVIOUS + after)
        if task.unique:
            existing = cast(str | None, await locked)  # type: ignore
            # allow retries of the same task but not new ones
            if existing and existing != task_id:
                if not task.silent:
                    logger.warning(
                        f"task {fn_name} ↯ {task_id} clashed with unique task "
                        f"{existing}"
                    )
                return await self.finish_failed_task(
                    msg,
                    StreaqError(
                        "Task is unique and another instance of the same task is "
                        "already running!"
                    ),
                    task_try,
                    enqueue_time=data["t"],
                    fn_name=fn_name,
                    silent=task.silent,
                    ttl=task.ttl,
                )

        _args = data["a"] if not after else self.deserialize(await previous)  # type: ignore
        return task, data, task_try, _args, lock_key

    async def run_task(self, msg: StreamMessage) -> None:
        """
        Execute the registered task, then store the result in Redis.
        """
        if not (res := await self.prepare_task(msg)):
            return
        task, data, task_try, _args, lock_key = res

        task_id = msg.task_id
        start_time = now_ms()
        ctx = self.build_context(task.fn_name, task, task_id, tries=task_try)
        success = True
        schedule = None
        done = True
        finish_time = None

        async def _fn(*args: Any, **kwargs: Any) -> Any:
            with fail_after(to_seconds(task.timeout)):
                return await task.fn(*args, **kwargs)

        if not task.silent:
            logger.info(f"task {task.fn_name} □ {task_id} → worker {self.id}")

        wrapped = _fn
        for middleware in reversed(self.middlewares):
            wrapped = middleware(wrapped)
        token = self._task_context.set(ctx)
        result: Any = None
        try:
            if self._block_new_tasks:
                raise StreaqCancelled("Not running task, worker shut down!")
            with CancelScope() as scope:
                self._cancel_scopes[task_id] = scope
                self._running_tasks[msg.priority].add(msg.message_id)
                result = await wrapped(*_args, **data["k"])
            if scope.cancelled_caught:
                result = StreaqCancelled("Task aborted by user!")
                success = False
                done = True
                if not task.silent:
                    logger.info(f"task {task.fn_name} ⊘ {task_id} aborted")
                self.counters["aborted"] += 1
                self.counters["failed"] -= 1  # this will get incremented later
        except StreaqRetry as e:
            success = False
            done = False
            self.counters["retried"] += 1
            if e.schedule:
                schedule = datetime_ms(e.schedule)
                if not task.silent:
                    logger.exception(f"Retrying task {task_id}!")
                    logger.info(
                        f"task {task.fn_name} ↻ {task_id} retrying at {schedule}"
                    )
            else:
                delay = to_ms(e.delay) if e.delay is not None else task_try**2 * 1000
                schedule = now_ms() + delay
                if not task.silent:
                    logger.exception(f"Retrying task {task_id}!")
                    logger.info(
                        f"task {task.fn_name} ↻ {task_id} retrying in {delay}ms"
                    )
        except TimeoutError as e:
            if not task.silent:
                logger.error(f"task {task.fn_name} … {task_id} timed out")
            result = e
            success = False
            done = True
        except self._cancelled_class:
            if not task.silent:
                logger.info(
                    f"task {task.fn_name} ↻ {task_id} cancelled, will be retried"
                )
            success = False
            done = False
            self.counters["retried"] += 1
            raise  # best practice from anyio docs
        except Exception as e:
            result = e
            success = False
            done = True
            if not task.silent:
                logger.exception(f"Task {task_id} failed!")
                logger.info(f"task {task.fn_name} × {task_id} failed")
        finally:
            finish_time = now_ms()
            self._cancel_scopes.pop(task_id, None)
            self._running_tasks[msg.priority].remove(msg.message_id)
            await self.finish_task(
                msg,
                finish=done,
                schedule=schedule,
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
                tries=task_try,
            )
            self._task_context.reset(token)

    async def fail_task_dependents(self, dependents: list[str]) -> None:
        """
        Fail dependents for the given task.
        """
        now = now_ms()
        failure = {
            "s": False,
            "r": StreaqError("Dependency failed, not running task!"),
            "st": now,
            "ft": now,
            "et": 0,
            "f": "Unknown",
            "t": 0,
            "w": self.id,
        }
        result = self.serialize(failure)
        self.counters["failed"] += len(dependents)
        to_delete: list[KeyT] = []
        async with self.redis.pipeline(transaction=False) as pipe:
            for dep_id in dependents:
                logger.info(f"task dependent × {dep_id} failed")
                to_delete.append(self.prefix + REDIS_TASK + dep_id)
                pipe.set(self.results_key + dep_id, result, ex=300)
                pipe.publish(self._channel_key + dep_id, result)
            pipe.delete(to_delete)

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
        be used if you need to separate the task queuing and task execution code. You
        also lose the ability to control certain parameters like uniqueness and queue
        expiration time. Consider using type stubs instead as explained `here <https://streaq.readthedocs.io/en/latest/integrations.html#separating-enqueuing-from-task-definitions>`_.

        :param fn_name: name of the function to run
        :param args: positional arguments for the task
        :param kwargs: keyword arguments for the task

        :return: task object
        """
        registered = RegisteredTask(
            fn=_placeholder,
            expire=None,
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
        enqueue_time = now_ms()
        async with self.redis.pipeline(transaction=False) as pipe:
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
                task.priority = task.priority or self.priorities[-1]
                expire = to_ms(task.parent.expire or 0)
                pipe.fcall(
                    "publish_task",
                    keys=[
                        self.stream_key,
                        self.queue_key,
                        task.task_key(REDIS_TASK),
                        self.dependents_key,
                        self.dependencies_key,
                        self.results_key,
                    ],
                    args=[task.id, data, task.priority, score, expire] + task.after,
                )

    async def queue_size(self) -> int:
        """
        Returns the number of tasks currently queued in Redis.
        """
        async with self.redis.pipeline(transaction=True) as pipe:
            commands = [
                pipe.xlen(self.stream_key + priority) for priority in self.priorities
            ] + [pipe.zcard(self.queue_key + priority) for priority in self.priorities]
        return sum(await gather(*commands))

    @property
    def active(self) -> int:
        """
        The number of currently active tasks for the worker
        """
        return len(self._cancel_scopes)

    def _delay_for(self, tab: CronTab) -> float:
        return tab.next(now=datetime.now(self.tz))  # type: ignore

    async def health_check(self) -> None:
        """
        Periodically stores info about the worker in Redis.
        """
        while True:
            await sleep(self._delay_for(self._health_tab))
            ttl = int(self._delay_for(self._health_tab)) + 5
            await self.redis.set(f"{self._health_key}:{self.id}", str(self), ex=ttl)

    async def signal_handler(self, scope: CancelScope) -> None:
        """
        Gracefully shutdown the worker when a signal is received.
        Doesn't work on Windows!
        """
        with open_signal_receiver(signal.SIGINT, signal.SIGTERM) as signals:
            async for signum in signals:
                logger.info(
                    f"received signal {signum.name}, shutting down worker {self.id}"
                )
                self._block_new_tasks = True
                scope.cancel()
                return

    async def status_by_id(self, task_id: str) -> TaskStatus:
        """
        Fetch the current status of the given task.

        :param task_id: ID of the task to check

        :return: status of the task
        """

        def key(mid: str) -> str:
            return self.prefix + mid + task_id

        async with self.redis.pipeline(transaction=True) as pipe:
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
        done, running, data, dependencies = await gather(*commands)

        if done:
            return TaskStatus.DONE
        elif running:
            return TaskStatus.RUNNING
        score = any(r for r in await gather(*delayed))
        if score or dependencies:
            return TaskStatus.SCHEDULED
        elif data:
            return TaskStatus.QUEUED
        return TaskStatus.NOT_FOUND

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
        with fail_after(to_seconds(timeout)):
            async with self.redis.pubsub(
                channels=[self._channel_key + task_id], ignore_subscribe_messages=True
            ) as pubsub:
                if not (raw := await self.redis.get(result_key)):
                    msg = await pubsub.__anext__()
                    raw = msg["data"]  # type: ignore
        data = self.deserialize(raw)
        return TaskResult(
            fn_name=data["f"],
            enqueue_time=data["et"],
            success=data["s"],
            start_time=data["st"],
            finish_time=data["ft"],
            tries=data["t"],
            worker_id=data["w"],
            _result=data["r"],
        )

    async def abort_by_id(
        self, task_id: str, timeout: timedelta | int | None = 5
    ) -> bool:
        """
        Notify workers that the task should be aborted, then wait for confirmation.

        :param task_id: ID of the task to abort
        :param timeout:
            how long to wait to confirm abortion was successful. None means wait
            forever, 0 means don't wait at all.

        :return: whether the task was aborted successfully
        """
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.sadd(self._abort_key, [task_id])
            delayed = [
                pipe.zrem(self.queue_key + priority, [task_id])
                for priority in self.priorities
            ]
        if any(await gather(*delayed)):
            # task was in delayed queue, we need to handle deps
            async with self.redis.pipeline(transaction=True) as pipe:
                pipe.delete([self.prefix + REDIS_TASK + task_id])
                command = pipe.fcall(
                    "fail_dependents",
                    keys=[
                        self.prefix + REDIS_DEPENDENTS,
                        self.prefix + REDIS_DEPENDENCIES,
                        task_id,
                    ],
                )
            if res := await command:
                await self.fail_task_dependents(res)  # type: ignore
            return True
        if timeout is not None and not timeout:  # check for 0, works with timedelta
            return False
        try:
            result = await self.result_by_id(task_id, timeout=timeout)
            return not result.success and isinstance(result.exception, StreaqCancelled)
        except TimeoutError:
            return False

    async def info_by_id(self, task_id: str) -> TaskInfo | None:
        """
        Fetch info about a previously enqueued task.

        :param task_id: ID of the task to get info for

        :return: task info, unless task has finished or doesn't exist
        """

        def key(mid: str) -> str:
            return self.prefix + mid + task_id

        async with self.redis.pipeline(transaction=False) as pipe:
            delayed = [
                pipe.zscore(self.queue_key + priority, task_id)
                for priority in self.priorities
            ]
            commands = (
                pipe.get(key(REDIS_RESULT)),
                pipe.get(key(REDIS_TASK)),
                pipe.get(key(REDIS_RETRY)),
                pipe.smembers(key(REDIS_DEPENDENCIES)),
                pipe.smembers(key(REDIS_DEPENDENTS)),
            )
        result, raw, try_count, dependencies, dependents = await gather(*commands)
        if result or not raw:  # if result exists or task data doesn't
            return None
        data = self.deserialize(raw)
        res = await gather(*delayed)
        score = next((r for r in res if r), None)
        dt = datetime.fromtimestamp(score / 1000, tz=self.tz) if score else None
        return TaskInfo(
            fn_name=data["f"],
            enqueue_time=data["t"],
            tries=int(try_count or 0),
            scheduled=dt,
            dependencies=dependencies,
            dependents=dependents,
        )

    def serialize(self, data: Any) -> Any:
        """
        Wrap serializer to append signature as last 32 bytes if applicable.
        """
        try:
            serialized = self.serializer(data)
        except Exception as e:
            raise StreaqError(f"Failed to serialize data: {data}") from e
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
                data = data_bytes
            except IndexError as e:
                raise StreaqError("Missing signature for task data!") from e
        try:
            return self.deserializer(data)
        except Exception as e:
            raise StreaqError(f"Failed to deserialize data: {data}") from e

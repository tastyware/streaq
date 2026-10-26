import json
import os
import pickle
import secrets
import signal
import sys
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any
from uuid import uuid4

import anyio.lowlevel
import pytest
from anyio import TaskHandle, create_task_group, fail_after, open_process, sleep
from coredis import ClusterConnectionPool, ConnectionPool, RedisCluster
from coredis.connection import TCPLocation

from streaq import StreaqError, StreaqRetry, TaskStatus, Worker
from streaq.constants import REDIS_HEALTH
from streaq.task import Task, TaskResult
from tests.conftest import run_worker

NAME_STR = "Freddy"
pytestmark = pytest.mark.anyio


async def test_worker_redis(worker: Worker[None]):
    async with worker:
        await worker.redis.ping()


@dataclass
class WorkerContext:
    name: str


@asynccontextmanager
async def deps() -> AsyncGenerator[WorkerContext]:
    yield WorkerContext(NAME_STR)


async def test_lifespan(redis_url: str):
    worker = Worker(redis_url=redis_url, lifespan=deps, queue_name=uuid4().hex)

    @worker.task
    async def foobar() -> bool:
        return worker.context.name == NAME_STR

    async with run_worker(worker):
        task = await foobar.enqueue()
        res = await task.result(3)
        assert res.success and res.result


async def test_health_check(redis_url: str):
    worker = Worker(
        redis_url=redis_url,
        redis_kwargs={"decode_responses": True},
        queue_name=uuid4().hex,
    )
    async with run_worker(worker):
        await sleep(1)
        health_key = f"{worker._prefix}{REDIS_HEALTH}:{worker.id}"
        worker_health = await worker.redis.get(health_key)
        assert worker_health is not None


async def test_queue_size(worker: Worker[None]):
    async with worker:
        assert await worker.queue_size() == 0


def raise_error(*args: Any, **kwargs: Any) -> Any:
    raise Exception("Couldn't serialize/deserialize!")


async def test_bad_serializer(redis_url: str):
    worker = Worker(redis_url=redis_url, serializer=raise_error, queue_name=uuid4().hex)

    @worker.task
    async def foobar() -> None:
        print("This can't print!")

    async with worker:
        with pytest.raises(StreaqError):
            await foobar.enqueue()


async def test_bad_deserializer(redis_url: str):
    worker = Worker(
        redis_url=redis_url, deserializer=raise_error, queue_name=uuid4().hex
    )

    @worker.task
    async def foobar() -> None:
        print("This can't print!")

    async with run_worker(worker):
        task = await foobar.enqueue()
        with pytest.raises(StreaqError):
            await task.result(3)


async def test_custom_serializer(worker: Worker[None]):
    worker.serializer = json.dumps
    worker.deserializer = json.loads

    @worker.task
    async def foobar() -> None:
        pass

    async with run_worker(worker):
        task = await foobar.enqueue()
        assert (await task.result(3)).success


async def test_uninitialized_worker(worker: Worker[None]):
    @worker.task
    async def foobar() -> None:
        print(worker.context)

    with pytest.raises(StreaqError):
        await foobar()
    with pytest.raises(StreaqError):
        await worker.redis.ping()
    with pytest.raises(StreaqError):
        await foobar.enqueue()


async def test_active_tasks(worker: Worker[None]):
    @worker.task
    async def foo() -> None:
        await sleep(10)

    n_tasks = 5
    async with run_worker(worker):
        tasks = [foo.enqueue() for _ in range(n_tasks)]
        await worker.enqueue_many(tasks)
        await sleep(3)
        assert worker.running() >= n_tasks


async def test_reclaim_backed_up(redis_url: str):
    queue_name = uuid4().hex
    worker = Worker(
        concurrency=2, redis_url=redis_url, queue_name=queue_name, idle_timeout=1
    )
    worker2 = Worker(redis_url=redis_url, queue_name=queue_name, idle_timeout=1)

    async def foo() -> None:
        await sleep(3)

    registered = worker.task()(foo)
    worker2.task()(foo)

    async with create_task_group() as tg:
        # run first worker which will pick up all tasks
        await tg.start(worker.run_async)
        # enqueue tasks
        tasks = [registered.enqueue() for _ in range(4)]
        await worker.enqueue_many(tasks)
        # run second worker which will pick up prefetched tasks
        await tg.start(worker2.run_async)

        results: list[TaskHandle[TaskResult[None]]] = []
        async with create_task_group() as tg2:
            for task in tasks:
                results.append(tg2.start_soon(task.result, 5))
        assert any(r.return_value.worker_id == worker2.id for r in results)

        tg.cancel_scope.cancel()


async def test_reclaim_idle_task(redis_url: str):
    worker2 = Worker(redis_url=redis_url, queue_name=uuid4().hex, idle_timeout=3)

    @worker2.task(name="foo")
    async def foo() -> None:
        await sleep(3)

    # get task ID
    task = foo.enqueue()
    # run separate worker which will enqueue and pick up task
    async with (
        await open_process(
            [sys.executable, "tests/failure.py", redis_url, task.id, worker2.queue_name]
        ) as process,
        worker2,
    ):
        while not await task.status():
            await sleep(1)
        # kill worker abruptly to disallow cleanup
        os.kill(process.pid, signal.SIGKILL)
        await process.wait()

    async with run_worker(worker2):
        assert (await task.result(10)).success


async def test_change_cron_schedule(redis_url: str):
    async def foo() -> None:
        pass

    worker = Worker(redis_url=redis_url, queue_name=uuid4().hex)
    foo1 = worker.cron("0 0 1 1 *")(foo)
    async with run_worker(worker):
        await sleep(2)
        assert worker.next_run(foo1.crontab) == int(  # type: ignore
            (await worker.redis.zscore(worker._cron_schedule_key, foo1.fn_name)) or 0
        )

    worker2 = Worker(redis_url=redis_url, queue_name=worker.queue_name)
    worker2.cron("1 0 1 1 *")(foo)  # 1 minute later
    async with run_worker(worker2):
        await sleep(2)
        assert worker.next_run(foo1.crontab) != int(  # type: ignore
            (await worker2.redis.zscore(worker._cron_schedule_key, foo1.fn_name)) or 0
        )


async def test_signed_data(redis_url: str):
    worker = Worker(
        redis_url=redis_url,
        queue_name=uuid4().hex,
        signing_secret=secrets.token_urlsafe(32),
    )

    @worker.task
    async def foo() -> str:
        return "bar"

    async with run_worker(worker):
        task = await foo.enqueue()
        res = await task.result(3)
        assert res.success and res.result == "bar"


async def test_sign_non_binary_data(redis_url: str):
    worker = Worker(
        redis_url=redis_url,
        queue_name=uuid4().hex,
        signing_secret=secrets.token_urlsafe(32),
        serializer=json.dumps,
    )

    @worker.task
    async def foo() -> str:
        return "bar"

    async with worker:
        with pytest.raises(StreaqError):
            await foo.enqueue()


async def test_corrupt_signed_data(redis_url: str):
    worker = Worker(
        redis_url=redis_url,
        queue_name=uuid4().hex,
        handle_signals=False,
        signing_secret=secrets.token_urlsafe(32),
    )

    @worker.task
    async def foo() -> str:
        return "bar"

    async with worker:
        task = await foo.enqueue()
        await worker.redis.set(
            worker._task_key + task.id, pickle.dumps({"f": "This is an attack!"})
        )

    async with run_worker(worker):
        res = await task.result(5)
        assert not res.success and isinstance(res.exception, StreaqError)


async def test_enqueue_many(worker: Worker[None]):
    @worker.task
    async def foobar(val: int) -> int:
        await sleep(1)
        return val

    async with worker:
        tasks = [foobar.enqueue(i) for i in range(10)]
        delayed = foobar.enqueue(1).start(delay=1)
        cron = foobar.enqueue(1).start(schedule="0 0 1 1 *")
        tasks.extend([delayed, cron])
        await worker.enqueue_many(tasks)
        assert await worker.queue_size() >= 10


async def test_bad_depends_worker(worker: Worker[None]):
    with pytest.raises(StreaqError):
        print(worker.context)
    with pytest.raises(StreaqError):
        print(worker.context)


async def test_custom_worker_id(redis_url: str):
    worker_id = uuid4().hex
    worker = Worker(redis_url=redis_url, queue_name=uuid4().hex, id=worker_id)

    assert worker.id == worker_id


async def test_burst_after_execution(worker: Worker[None]):
    @worker.task
    async def sleeper(time: int) -> None:
        await sleep(time)

    worker.burst = True
    async with worker:
        tasks = [sleeper.enqueue(1) for _ in range(10)]
        await worker.enqueue_many(tasks)
    with fail_after(3):
        await worker.run_async()
    async with worker:
        results: list[TaskHandle[TaskResult[None]]] = []
        async with create_task_group() as tg:
            for task in tasks:
                results.append(tg.start_soon(task.result, 3))
        assert all(r.return_value.success for r in results)


def test_connection_pool(redis_url: str):
    pool = ConnectionPool.from_url(redis_url, decode_responses=True)
    worker = Worker(redis_pool=pool, queue_name=uuid4().hex)
    worker2 = Worker(redis_pool=pool, queue_name=worker.queue_name)
    assert worker._redis.connection_pool is worker2._redis.connection_pool


def test_connection_pool_illegal(redis_url: str):
    pool = ConnectionPool.from_url(redis_url, decode_responses=False, max_connections=4)
    with pytest.raises(StreaqError):
        _ = Worker(redis_pool=pool, queue_name=uuid4().hex)


def test_cluster_connection_pool():
    pool = ClusterConnectionPool(
        startup_nodes=[TCPLocation("cluster-1", 7000)], decode_responses=True
    )
    worker = Worker(redis_pool=pool, queue_name=f"{{{uuid4().hex}}}")
    worker2 = Worker(redis_pool=pool, queue_name=worker.queue_name)
    assert worker._redis.connection_pool is worker2._redis.connection_pool


async def test_duplicate_tasks(worker: Worker[None]):
    @worker.task(name="foobar")
    async def foobar() -> None: ...

    with pytest.raises(StreaqError):

        @worker.task(name="foobar")
        async def barfoo() -> None: ...


async def test_include_worker(redis_url: str, worker: Worker[None]):
    if worker._sentinel:
        worker2 = Worker(
            sentinel_nodes=[
                ("localhost", 26379),
                ("localhost", 26380),
                ("localhost", 26381),
            ],
            sentinel_master="mymaster",
            queue_name=worker.queue_name,
        )
    elif isinstance(worker._redis, RedisCluster):
        worker2 = Worker(
            cluster_nodes=[("localhost", 7000)], queue_name=worker.queue_name
        )
    else:
        worker2 = Worker(redis_url=redis_url, queue_name=worker.queue_name)

    @worker2.task
    async def foobar() -> None:
        await anyio.lowlevel.checkpoint()

    worker.include(worker2)
    async with run_worker(worker):
        task = await foobar.enqueue()
        res = await task.result(3)
        assert res.success


async def test_include_duplicate(redis_url: str, worker: Worker):
    if worker._sentinel:
        worker2 = Worker(
            sentinel_nodes=[
                ("localhost", 26379),
                ("localhost", 26380),
                ("localhost", 26381),
            ],
            sentinel_master="mymaster",
            queue_name=worker.queue_name,
        )
    elif isinstance(worker._redis, RedisCluster):
        worker2 = Worker(
            cluster_nodes=[("localhost", 7000)], queue_name=worker.queue_name
        )
    else:
        worker2 = Worker(redis_url=redis_url, queue_name=worker.queue_name)

    @worker.task(name="foobar")
    async def foobar() -> None: ...

    @worker2.task(name="foobar")
    async def barfoo() -> None: ...

    with pytest.raises(StreaqError):
        worker.include(worker2)


async def test_grace_period(redis_url: str):
    pool = ConnectionPool.from_url(redis_url, decode_responses=True)
    worker = Worker(redis_pool=pool, queue_name=uuid4().hex, grace_period=3)

    @worker.task
    async def foobar() -> None:
        await sleep(3)

    async with pool, create_task_group() as tg:
        await tg.start(worker.run_async)
        task = await foobar.enqueue()
        await sleep(1)
        os.kill(os.getpid(), signal.SIGINT)
        res = await task.result(5)
        assert res.success


async def test_grace_period_no_new_tasks(redis_url: str):
    pool = ConnectionPool.from_url(redis_url, decode_responses=True)
    worker = Worker(redis_pool=pool, queue_name=uuid4().hex, grace_period=5)

    @worker.task
    async def foobar() -> None:
        await sleep(3)

    async with pool:
        task = foobar.enqueue().start(delay=1)
        async with create_task_group() as tg:
            await tg.start(worker.run_async)
            await foobar.enqueue()
            await sleep(1)
            await task
            os.kill(os.getpid(), signal.SIGINT)
        async with worker:
            assert await task.status() == TaskStatus.SCHEDULED


async def test_no_grace_period(worker: Worker):
    @worker.task()
    async def sleeper(time: int) -> None:
        await sleep(time)

    with fail_after(3):
        async with run_worker(worker):
            tasks = [sleeper.enqueue(5) for _ in range(48)]
            await worker.enqueue_many(tasks)
            await sleep(1)


async def test_get_tasks_by_status_scheduled(worker: Worker):

    @worker.task()
    async def delayed_task(x: int) -> int:
        return x * 2

    async with worker:
        # Schedule tasks for future execution
        task1 = await delayed_task.enqueue(1).start(delay=timedelta(hours=1))
        task2 = await delayed_task.enqueue(2).start(delay=timedelta(hours=2))

        # Retrieve scheduled tasks
        scheduled = await worker.get_tasks_by_status(TaskStatus.SCHEDULED)
        task_ids = [t.task_id for t in scheduled]
        assert task1.id in task_ids
        assert task2.id in task_ids

        # Check task properties
        task = next(t for t in scheduled if t.task_id == task1.id)
        assert "delayed_task" in task.fn_name
        assert task.args[0] == 1
        assert task.status == TaskStatus.SCHEDULED
        assert task.scheduled is not None


async def test_get_tasks_by_status_scheduled_with_limit(worker: Worker):

    @worker.task()
    async def delayed_task() -> None:
        pass

    async with worker:
        # Schedule multiple tasks
        for _ in range(5):
            await delayed_task.enqueue().start(delay=timedelta(hours=1))

        # Test limit
        scheduled = await worker.get_tasks_by_status(TaskStatus.SCHEDULED, limit=2)
        assert len(scheduled) <= 2


async def test_get_tasks_by_status_queued(worker: Worker):
    @worker.task()
    async def foobar() -> None: ...

    async with worker:
        tasks = [foobar.enqueue() for _ in range(4)]
        await worker.enqueue_many(tasks)
        queued = await worker.get_tasks_by_status(TaskStatus.QUEUED)
        assert len(queued) == 4


async def test_get_tasks_by_status_running(worker: Worker):
    @worker.task()
    async def slow_task() -> None:
        await sleep(10)

    async with run_worker(worker):
        task = await slow_task.enqueue()
        await sleep(1)  # Wait for task to start

        running = await worker.get_tasks_by_status(TaskStatus.RUNNING)
        assert len(running) >= 1
        found = next(r for r in running if r.task_id == task.id)
        assert found.status == TaskStatus.RUNNING


async def test_get_tasks_by_status_done(worker: Worker):
    @worker.task()
    async def quick_task(x: int) -> int:
        return x * 2

    async with run_worker(worker):
        task = await quick_task.enqueue(5)
        await task.result(3)

        completed = await worker.get_tasks_by_status(TaskStatus.DONE)
        assert len(completed) >= 1

        result = next((r for r in completed if "quick_task" in r.fn_name), None)
        assert result is not None
        assert result.success
        assert result.result == 10


async def test_get_tasks_by_status_empty_scheduled(worker: Worker):
    async with worker:
        scheduled = await worker.get_tasks_by_status(TaskStatus.SCHEDULED)
        assert scheduled == []


async def test_get_tasks_by_status_empty_done(worker: Worker):
    async with worker:
        completed = await worker.get_tasks_by_status(TaskStatus.DONE)
        assert completed == []


async def test_expire_anchored_at_due_time(worker: Worker):
    @worker.cron("* * * * *", expire=1)
    async def every_minute() -> None: ...

    async with run_worker(worker):
        task = await every_minute.enqueue().start(delay=2)
        result = await task.result(5)
        assert result.success


async def test_expire_stale_run(worker: Worker):
    @worker.cron("* * * * *", expire=1)
    async def every_minute() -> None: ...

    async with worker:
        task = await every_minute.enqueue().start(delay=1)
    await sleep(3)
    async with run_worker(worker):
        result = await task.result(5)
        assert not result.success
        assert "expired" in str(result.exception)


async def test_cron_no_expire(worker: Worker):
    @worker.cron("* * * * *")
    async def every_minute() -> None: ...

    async with create_task_group() as tg:
        await tg.start(worker.run_async)
        # simulate previously enqueued with old timestamp
        task = await every_minute.enqueue().start(schedule=datetime(2022, 2, 22))
        result = await task.result(5)
        assert result.success
        tg.cancel_scope.cancel()


async def test_dlq_parks(worker: Worker):
    worker.dlq_ttl = 60

    @worker.task
    async def raises() -> None:
        raise ValueError("nope")

    @worker.task(timeout=1)
    async def times_out() -> None:
        await sleep(3)

    @worker.task(max_tries=1)
    async def retries() -> None:
        raise StreaqRetry("again", delay=0)

    async with run_worker(worker):
        tasks = [
            await raises.enqueue(),
            await times_out.enqueue(),
            await retries.enqueue(),
            await worker.enqueue_unsafe("missing"),
        ]
        for task in tasks:
            assert not (await task.result(5)).success
            assert await task.status() == TaskStatus.DEAD
            info = await task.info()
            assert info and info.status == TaskStatus.DEAD
        dead = await worker.get_tasks_by_status(TaskStatus.DEAD)
        assert {r.task_id for r in dead} == {t.id for t in tasks}


async def test_dlq_parks_bad_payload(worker: Worker):
    worker.dlq_ttl = 60

    @worker.task
    async def foobar() -> None:
        pass

    # write the payload in a format the worker can't read
    worker.serializer, worker.deserializer = json.dumps, json.loads
    async with worker:
        task = await foobar.enqueue()
    worker.serializer, worker.deserializer = pickle.dumps, pickle.loads
    async with run_worker(worker):
        res = await task.result(5)
        assert not res.success and isinstance(res.exception, StreaqError)
        assert await task.status() == TaskStatus.DEAD


async def test_dlq_doesnt_park(worker: Worker):
    worker.dlq_ttl = 60

    @worker.task(expire=1)
    async def expires() -> None:
        pass

    @worker.task
    async def sleeper(time: int) -> None:
        await sleep(time)

    @worker.task
    async def boom() -> None:
        raise ValueError("nope")

    async with worker:
        expired = await expires.enqueue()
    await sleep(2)
    async with run_worker(worker):
        running = await sleeper.enqueue(5)
        delayed = await sleeper.enqueue(0).start(delay=10)
        fails = boom.enqueue()
        fallback = await fails.otherwise(sleeper, forward=False)(0)
        await sleep(1)
        assert await running.abort(3)
        assert await delayed.abort(3)
        assert (await fallback.result(5)).success
        tasks: list[Task[..., None]] = [expired, running, delayed, fails]
        for task in tasks:
            assert not (await task.result(5)).success
            assert await task.status() == TaskStatus.DONE
        dead = await worker.get_tasks_by_status(TaskStatus.DEAD)
        assert not {r.task_id for r in dead} & {t.id for t in tasks}


async def test_dlq_holds_child(worker: Worker):
    worker.dlq_ttl = 60
    calls: list[int] = []

    @worker.task
    async def flaky() -> int:
        calls.append(1)
        if len(calls) == 1:
            raise ValueError("nope")
        return 1

    @worker.task
    async def double(val: int) -> int:
        return val * 2

    async with run_worker(worker):
        parent = flaky.enqueue()
        child = await parent.then(double)()
        await parent.result(5)
        assert await parent.status() == TaskStatus.DEAD
        assert await child.status() == TaskStatus.DEPENDS
        # requeueing the parent resumes the chain
        assert await worker.requeue_by_id(parent.id)
        assert (await child.result(5)).result == 2


async def test_dlq_purge_fails_child(worker: Worker):
    worker.dlq_ttl = 60

    @worker.task
    async def boom() -> int:
        raise ValueError("nope")

    @worker.task
    async def double(val: int) -> int:
        return val * 2

    async with run_worker(worker):
        parent = boom.enqueue()
        child = await parent.then(double)()
        await parent.result(5)
        assert await worker.purge_by_id(parent.id)
        res = await child.result(5)
        assert not res.success and "Dependency failed" in str(res.exception)


async def test_dlq_sweep_fails_child(worker: Worker):
    worker.dlq_ttl = 1
    worker.idle_timeout = 1000

    @worker.task
    async def boom() -> int:
        raise ValueError("nope")

    @worker.task
    async def double(val: int) -> int:
        return val * 2

    async with run_worker(worker):
        parent = boom.enqueue()
        child = await parent.then(double)()
        res = await child.result(5)
        assert not res.success and "Dependency failed" in str(res.exception)


async def test_failed_result_kept_for_dlq_ttl(worker: Worker):
    worker.dlq_ttl = 3

    @worker.task(ttl=1)
    async def boom() -> None:
        raise ValueError("nope")

    async with run_worker(worker):
        task = await boom.enqueue()
        await task.result(5)
        await sleep(2)
        assert await task.status() == TaskStatus.DEAD


async def test_failed_result_kept_for_ttl(worker: Worker):
    @worker.task(ttl=1)
    async def boom() -> None:
        raise ValueError("nope")

    async with run_worker(worker):
        task = await boom.enqueue()
        assert await task.status() is not None
        await task.result(5)
        await sleep(2)
        assert await task.status() is None


async def test_dlq_ttl_none_persists(worker: Worker):
    worker.dlq_ttl = None

    @worker.task(expire=1)
    async def boom() -> None:
        raise ValueError("nope")

    async with run_worker(worker):
        task = await boom.enqueue()
        await task.result(5)
        # payload would have expired here without the persist
        await sleep(2)
        assert await task.status() == TaskStatus.DEAD


async def test_dead_sweep(worker: Worker):
    worker.dlq_ttl = 1
    worker.idle_timeout = 1000

    @worker.task
    async def boom() -> None:
        raise ValueError("nope")

    async with run_worker(worker):
        task = await boom.enqueue()
        await task.result(5)
        assert await worker.get_tasks_by_status(TaskStatus.DEAD)
        await sleep(3)
        # the result outlives dlq_ttl, so only the sweep can empty the listing
        assert await task.status() == TaskStatus.DONE
        assert await worker.get_tasks_by_status(TaskStatus.DEAD) == []


async def test_get_tasks_by_status_dead_empty(worker: Worker):
    worker.dlq_ttl = 60
    async with worker:
        assert await worker.get_tasks_by_status(TaskStatus.DEAD) == []


async def test_requeue(worker: Worker):
    worker.dlq_ttl = 60
    calls: list[int] = []

    @worker.task
    async def flaky() -> int:
        calls.append(flaky.context.tries)
        if len(calls) == 1:
            raise ValueError("first time")
        return flaky.context.tries

    async with run_worker(worker):
        task = await flaky.enqueue()
        assert not (await task.result(5)).success
        assert await worker.requeue_by_id(task.id)
        res = await task.result(5)
        # retry count was reset
        assert res.success and res.result == 1
        assert await task.status() == TaskStatus.DONE


async def test_requeue_concurrent(worker: Worker):
    worker.dlq_ttl = 60
    calls: list[int] = []

    @worker.task
    async def flaky() -> None:
        calls.append(1)
        if len(calls) == 1:
            raise ValueError("first time")

    async with run_worker(worker):
        task = await flaky.enqueue()
        await task.result(5)
        handles: list[TaskHandle[bool]] = []
        async with create_task_group() as tg:
            for _ in range(2):
                handles.append(tg.start_soon(worker.requeue_by_id, task.id))
        assert sorted(h.return_value for h in handles) == [False, True]
        assert (await task.result(5)).success
        await sleep(1)
        assert len(calls) == 2


async def test_requeue_chained_link_keeps_args(worker: Worker):
    worker.dlq_ttl = 60
    calls: list[int] = []

    @worker.task
    async def double(val: int) -> int:
        return val * 2

    @worker.task
    async def flaky_add(val: int) -> int:
        calls.append(val)
        if len(calls) == 1:
            raise ValueError("first time")
        return val + 1

    async with run_worker(worker):
        child = await double.enqueue(2).then(flaky_add)()
        assert not (await child.result(5)).success
        assert await worker.requeue_by_id(child.id)
        res = await child.result(5)
        assert res.success and res.result == 5
        assert calls == [4, 4]


async def test_purge(worker: Worker):
    worker.dlq_ttl = 60

    @worker.task
    async def boom() -> None:
        raise ValueError("nope")

    async with run_worker(worker):
        task = await boom.enqueue()
        await task.result(5)
        assert await worker.purge_by_id(task.id)
        assert await task.status() is None
        assert await worker.get_tasks_by_status(TaskStatus.DEAD) == []


async def test_requeue_purge_not_dead(worker: Worker):
    worker.dlq_ttl = 60

    @worker.task
    async def foobar() -> None:
        pass

    async with run_worker(worker):
        task = await foobar.enqueue()
        assert (await task.result(5)).success
        for task_id in (task.id, "nonexistent"):
            assert not await worker.requeue_by_id(task_id)
            assert not await worker.purge_by_id(task_id)
        assert await task.status() == TaskStatus.DONE


async def test_abort_and_failure_counters(worker: Worker):
    @worker.task
    async def boom() -> None:
        raise ValueError("nope")

    @worker.task
    async def sleeper(time: int) -> None:
        await sleep(time)

    async with run_worker(worker):
        failed = await boom.enqueue()
        running = await sleeper.enqueue(5)
        delayed = await sleeper.enqueue(0).start(delay=10)
        await failed.result(5)
        await sleep(1)
        assert await running.abort(3)
        assert await delayed.abort(3)
        assert worker.counters["failed"] == 1
        assert worker.counters["aborted"] == 2


async def test_enqueue_many_rejects_chains(worker: Worker):
    @worker.task
    async def foobar(val: int) -> int:
        return val

    async with worker:
        root = foobar.enqueue(1)
        tail = root.then(foobar)()
        for task in (root, tail):
            with pytest.RaisesGroup(StreaqError, flatten_subgroups=True):
                await worker.enqueue_many([task])

import json
import os
import pickle
import secrets
import signal
import subprocess
import sys
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, AsyncIterator
from uuid import uuid4

import pytest
from anyio import create_task_group, sleep
from coredis import RedisCluster
from coredis.typing import Node

from streaq.constants import REDIS_TASK
from streaq.task import TaskStatus
from streaq.types import StreaqError, WorkerDepends
from streaq.utils import gather
from streaq.worker import Worker
from tests.conftest import run_worker

NAME_STR = "Freddy"
pytestmark = pytest.mark.anyio


async def test_worker_redis(worker: Worker):
    async with worker:
        await worker.redis.ping()


@dataclass
class WorkerContext:
    name: str


@asynccontextmanager
async def deps() -> AsyncIterator[WorkerContext]:
    yield WorkerContext(NAME_STR)


async def test_lifespan(redis_url: str):
    worker = Worker(redis_url=redis_url, lifespan=deps, queue_name=uuid4().hex)

    @worker.task
    async def foobar(ctx: WorkerContext = WorkerDepends()) -> str:
        return ctx.name

    async with run_worker(worker):
        task = await foobar.enqueue()
        res = await task.result(3)
        assert res.success and res.result == NAME_STR


async def test_health_check(redis_url: str):
    worker = Worker(
        redis_url=redis_url,
        redis_kwargs={"decode_responses": True},
        health_crontab="* * * * * * *",
        queue_name=uuid4().hex,
    )
    async with run_worker(worker):
        await sleep(2)
        worker_health = await worker.redis.get(f"{worker._health_key}:{worker.id}")
        redis_health = await worker.redis.get(worker._health_key + ":redis")
        assert worker_health is not None
        assert redis_health is not None


async def test_queue_size(worker: Worker):
    async with worker:
        assert await worker.queue_size() == 0


def raise_error(*arg, **kwargs) -> Any:
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

    worker.burst = True
    async with run_worker(worker):
        task = await foobar.enqueue()
        with pytest.raises(StreaqError):
            await task.result(3)


async def test_custom_serializer(worker: Worker):
    worker.serializer = json.dumps
    worker.deserializer = json.loads

    @worker.task
    async def foobar() -> None:
        pass

    async with run_worker(worker):
        task = await foobar.enqueue()
        assert (await task.result(3)).success


async def test_uninitialized_worker(worker: Worker):
    @worker.task
    async def foobar(ctx: Any = WorkerDepends()) -> None:
        print(ctx.nonexistent)

    with pytest.raises(StreaqError):
        await foobar()
    with pytest.raises(StreaqError):
        await worker.redis.ping()
    with pytest.raises(StreaqError):
        await foobar.enqueue()


async def test_active_tasks(worker: Worker):
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

        results = await gather(*[t.result(5) for t in tasks])
        assert any(r.worker_id == worker2.id for r in results)
        tg.cancel_scope.cancel()


async def test_reclaim_idle_task(redis_url: str):
    worker2 = Worker(redis_url=redis_url, queue_name="reclaim", idle_timeout=3)

    @worker2.task(name="foo")
    async def foo() -> None:
        await sleep(3)

    # get task ID
    task = foo.enqueue()
    # run separate worker which will enqueue and pick up task
    worker = subprocess.Popen([sys.executable, "tests/failure.py", redis_url, task.id])
    async with worker2:
        while (await task.status()) == TaskStatus.NOT_FOUND:
            await sleep(1)
    # kill worker abruptly to disallow cleanup
    os.kill(worker.pid, signal.SIGKILL)
    worker.wait()

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
            (await worker.redis.zscore(worker.cron_schedule_key, foo1.fn_name)) or 0
        )

    worker2 = Worker(redis_url=redis_url, queue_name=worker.queue_name)
    worker2.cron("1 0 1 1 *")(foo)  # 1 minute later
    async with run_worker(worker2):
        await sleep(2)
        assert worker.next_run(foo1.crontab) != int(  # type: ignore
            (await worker2.redis.zscore(worker.cron_schedule_key, foo1.fn_name)) or 0
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
            task.task_key(REDIS_TASK), pickle.dumps({"f": "This is an attack!"})
        )

    async with run_worker(worker):
        res = await task.result(5)
        assert not res.success and isinstance(res.exception, StreaqError)


async def test_enqueue_many(worker: Worker):
    @worker.task
    async def foobar(val: int) -> int:
        await sleep(1)
        return val

    async with worker:
        tasks = [foobar.enqueue(i) for i in range(10)]
        delayed = foobar.enqueue(1).start(delay=1)
        depends = foobar.enqueue(1).start(after=delayed.id)
        cron = foobar.enqueue(1).start(schedule="0 0 1 1 *")
        tasks.extend([delayed, depends, cron])
        await worker.enqueue_many(tasks)
        assert await worker.queue_size() >= 10


async def test_bad_depends_worker():
    with pytest.raises(StreaqError):
        ctx = WorkerDepends()
        print(ctx.nonexistent)


async def test_custom_worker_id(redis_url: str):
    worker_id = uuid4().hex
    worker = Worker(redis_url=redis_url, queue_name=uuid4().hex, id=worker_id)

    assert worker.id == worker_id


def test_connection_pool(redis_url: str):
    from coredis import ConnectionPool

    pool = ConnectionPool.from_url(redis_url, decode_responses=True)
    worker = Worker(redis_pool=pool, queue_name=uuid4().hex)
    worker2 = Worker(redis_pool=pool, queue_name=worker.queue_name)
    assert worker._redis.connection_pool is worker2._redis.connection_pool


def test_connection_pool_illegal(redis_url: str):
    from coredis import ConnectionPool

    pool = ConnectionPool.from_url(redis_url, decode_responses=False, max_connections=4)
    with pytest.raises(StreaqError):
        _ = Worker(redis_pool=pool, queue_name=uuid4().hex)


def test_cluster_connection_pool():
    from coredis import ClusterConnectionPool

    pool = ClusterConnectionPool(
        startup_nodes=[Node(host="cluster-1", port=7000)], decode_responses=True
    )
    worker = Worker(redis_pool=pool, queue_name=f"{{{uuid4().hex}}}")
    worker2 = Worker(redis_pool=pool, queue_name=worker.queue_name)
    assert worker._redis.connection_pool is worker2._redis.connection_pool


async def test_duplicate_tasks(worker: Worker):
    @worker.task(name="foobar")
    async def foobar(): ...

    with pytest.raises(StreaqError):

        @worker.task(name="foobar")
        async def barfoo(): ...


async def test_include_worker(redis_url: str, worker: Worker):
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

    @worker2.task()
    async def foobar() -> None:
        await sleep(0)

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


async def test_grace_period(worker: Worker):
    @worker.task
    async def foobar() -> None:
        await sleep(3)

    worker.grace_period = 3
    async with create_task_group() as tg:
        await tg.start(worker.run_async)
        task = await foobar.enqueue()
        await sleep(1)
        os.kill(os.getpid(), signal.SIGINT)
        res = await task.result(5)
        assert res.success


async def test_grace_period_no_new_tasks(worker: Worker):
    @worker.task
    async def foobar() -> None:
        await sleep(3)

    worker.grace_period = 5
    async with create_task_group() as tg:
        await tg.start(worker.run_async)
        await foobar.enqueue()
        await sleep(1)
        os.kill(os.getpid(), signal.SIGINT)
    async with worker:
        task = await foobar.enqueue()
        await sleep(1)
        assert await task.status() == TaskStatus.QUEUED

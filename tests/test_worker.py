import asyncio
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
from anyio import create_task_group, move_on_after

from streaq.constants import REDIS_TASK
from streaq.task import TaskStatus
from streaq.utils import StreaqError
from streaq.worker import Worker

NAME_STR = "Freddy"
pytestmark = pytest.mark.anyio


async def test_worker_redis(redis_url: str):
    worker = Worker(redis_url=redis_url, queue_name=uuid4().hex)
    await worker.redis.ping()


@dataclass
class WorkerContext:
    name: str


@asynccontextmanager
async def deps() -> AsyncIterator[WorkerContext]:
    yield WorkerContext(NAME_STR)


async def test_lifespan(redis_url: str):
    worker = Worker(redis_url=redis_url, lifespan=deps, queue_name=uuid4().hex)

    @worker.task()
    async def foobar() -> str:
        return worker.context.name

    @worker.task(timeout=1)
    async def foobar2() -> None:
        await asyncio.sleep(3)

    async with worker:
        res = await foobar.run()
        assert res == NAME_STR
        with pytest.raises(TimeoutError):
            await foobar2.run()


async def test_health_check(redis_url: str):
    worker = Worker(
        redis_url=redis_url,
        health_crontab="* * * * * * *",
        queue_name=uuid4().hex,
    )
    async with create_task_group() as tg:
        tg.start_soon(worker.run_async)
        await asyncio.sleep(2)
        worker_health = await worker.redis.get(f"{worker._health_key}:{worker.id}")
        redis_health = await worker.redis.get(worker._health_key + ":redis")
        assert worker_health is not None
        assert redis_health is not None
        tg.cancel_scope.cancel()


async def test_queue_size(worker: Worker):
    assert await worker.queue_size() == 0


def raise_error(*arg, **kwargs) -> Any:
    raise Exception("Couldn't serialize/deserialize!")


async def test_bad_serializer(redis_url: str):
    worker = Worker(redis_url=redis_url, serializer=raise_error, queue_name=uuid4().hex)

    @worker.task()
    async def foobar() -> None:
        print("This can't print!")

    async with worker:
        with pytest.raises(StreaqError):
            await foobar.enqueue()


async def test_bad_deserializer(redis_url: str):
    worker = Worker(
        redis_url=redis_url, deserializer=raise_error, queue_name=uuid4().hex
    )

    @worker.task()
    async def foobar() -> None:
        print("This can't print!")

    worker.burst = True
    async with worker:
        task = await foobar.enqueue()

    await worker.run_async()
    with pytest.raises(StreaqError):
        await task.result(3)


async def test_custom_serializer(worker: Worker):
    worker.serializer = json.dumps
    worker.deserializer = json.loads

    @worker.task()
    async def foobar() -> None:
        pass

    async with worker:
        task = await foobar.enqueue()

    async with create_task_group() as tg:
        tg.start_soon(worker.run_async)
        assert (await task.result(3)).success
        tg.cancel_scope.cancel()


async def test_uninitialized_worker(worker: Worker):
    @worker.task()
    async def foobar() -> None:
        print(worker.context)

    with pytest.raises(StreaqError):
        await foobar.run()
    with pytest.raises(StreaqError):
        await foobar.enqueue()


async def test_active_tasks(worker: Worker):
    @worker.task()
    async def foo() -> None:
        await asyncio.sleep(3)

    n_tasks = 5
    async with worker:
        tasks = [foo.enqueue() for _ in range(n_tasks)]
        await worker.enqueue_many(tasks)

    async with create_task_group() as tg:
        tg.start_soon(worker.run_async)
        await asyncio.sleep(1)
        assert worker.active == n_tasks
        tg.cancel_scope.cancel()


async def test_handle_signal(worker: Worker):
    @worker.task()
    async def foo() -> None:
        await asyncio.sleep(3)

    async with create_task_group() as tg:
        tg.start_soon(worker.run_async)
        await asyncio.sleep(1)
        os.kill(os.getpid(), signal.SIGINT)

    async with worker:
        task = await foo.enqueue()
        assert await task.status() == TaskStatus.QUEUED


async def test_reclaim_backed_up(redis_url: str):
    queue_name = uuid4().hex
    worker = Worker(
        concurrency=2, redis_url=redis_url, queue_name=queue_name, idle_timeout=2
    )
    worker2 = Worker(redis_url=redis_url, queue_name=queue_name)

    async def foo() -> None:
        await asyncio.sleep(3)

    registered = worker.task()(foo)
    worker2.task()(foo)

    # enqueue tasks
    async with worker:
        tasks = [registered.enqueue() for _ in range(4)]
        await worker.enqueue_many(tasks)
    async with create_task_group() as tg:
        # run first worker which will pick up all tasks
        tg.start_soon(worker.run_async)
        await asyncio.sleep(1)
        # run second worker which will pick up prefetched tasks
        tg.start_soon(worker2.run_async)

        results = await asyncio.gather(*[t.result(8) for t in tasks])
        assert any(r.worker_id == worker2.id for r in results)
        tg.cancel_scope.cancel()


async def test_reclaim_idle_task(redis_url: str):
    worker2 = Worker(redis_url=redis_url, queue_name="reclaim")

    @worker2.task(timeout=3)
    async def foo() -> None:
        await asyncio.sleep(2)

    # enqueue task
    async with worker2:
        task = await foo.enqueue()
    # run separate worker which will pick up task
    worker = subprocess.Popen([sys.executable, "tests/failure.py", redis_url])
    await asyncio.sleep(1)
    # kill worker abruptly to disallow cleanup
    os.kill(worker.pid, signal.SIGKILL)
    worker.wait()

    async with create_task_group() as tg:
        tg.start_soon(worker2.run_async)
        assert (await task.result(8)).success
        tg.cancel_scope.cancel()


async def test_change_cron_schedule(redis_url: str):
    async def foo() -> None:
        pass

    worker = Worker(redis_url=redis_url, queue_name=uuid4().hex)
    foo1 = worker.cron("0 0 1 1 *")(foo)
    with move_on_after(2):
        await worker.run_async()
    task1 = foo1.enqueue()
    info = await task1.info()
    assert info and foo1.schedule() == info.scheduled

    worker2 = Worker(redis_url=redis_url, queue_name=worker.queue_name)
    foo2 = worker2.cron("1 0 1 1 *")(foo)  # 1 minute later
    async with create_task_group() as tg:
        tg.start_soon(worker2.run_async)
        await asyncio.sleep(2)
        tg.cancel_scope.cancel()
    task2 = foo2.enqueue()
    info2 = await task2.info()
    assert info2 and foo2.schedule() == info2.scheduled
    assert foo1.schedule() != foo2.schedule()


async def test_signed_data(redis_url: str):
    worker = Worker(
        redis_url=redis_url,
        queue_name=uuid4().hex,
        signing_secret=secrets.token_urlsafe(32),
    )

    @worker.task()
    async def foo() -> str:
        return "bar"

    async with create_task_group() as tg:
        tg.start_soon(worker.run_async)
        async with worker:
            task = await foo.enqueue()
            res = await task.result(3)
            assert res.success and res.result == "bar"
        tg.cancel_scope.cancel()


async def test_sign_non_binary_data(redis_url: str):
    worker = Worker(
        redis_url=redis_url,
        queue_name=uuid4().hex,
        signing_secret=secrets.token_urlsafe(32),
        serializer=json.dumps,
    )

    @worker.task()
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

    @worker.task()
    async def foo() -> str:
        return "bar"

    async with worker:
        task = await foo.enqueue()
        await worker.redis.set(
            task.task_key(REDIS_TASK), pickle.dumps({"f": "This is an attack!"})
        )

    async with create_task_group() as tg:
        tg.start_soon(worker.run_async)
        res = await task.result(5)
        assert not res.success and isinstance(res.result, StreaqError)
        tg.cancel_scope.cancel()


async def test_enqueue_many(worker: Worker):
    @worker.task()
    async def foobar(val: int) -> int:
        await asyncio.sleep(1)
        return val

    async with worker:
        tasks = [foobar.enqueue(i) for i in range(10)]
        await worker.enqueue_many(tasks)
    assert await worker.queue_size() >= len(tasks)


async def test_invalid_task_context(worker: Worker):
    with pytest.raises(StreaqError):
        worker.task_context()

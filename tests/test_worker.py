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
from signal import Signals
from typing import Any, AsyncIterator
from uuid import uuid4

import pytest

from streaq.constants import REDIS_TASK
from streaq.task import TaskStatus
from streaq.utils import StreaqError
from streaq.worker import Worker

NAME_STR = "Freddy"


async def test_worker_redis(redis_url: str):
    worker = Worker(redis_url=redis_url, queue_name=uuid4().hex)
    await worker.redis.ping()


@dataclass
class WorkerContext:
    name: str


@asynccontextmanager
async def deps(worker: Worker[WorkerContext]) -> AsyncIterator[WorkerContext]:
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
        with pytest.raises(asyncio.TimeoutError):
            await foobar2.run()


async def test_health_check(redis_url: str):
    worker = Worker(
        redis_url=redis_url,
        health_crontab="* * * * * * *",
        queue_name=uuid4().hex,
        handle_signals=False,
    )
    await worker.redis.flushdb()
    worker.loop.create_task(worker.run_async())
    await asyncio.sleep(2)
    worker_health = await worker.redis.get(f"{worker._health_key}:{worker.id}")
    redis_health = await worker.redis.get(worker._health_key + ":redis")
    assert worker_health is not None
    assert redis_health is not None
    await worker.close()


async def test_queue_size(redis_url: str):
    worker = Worker(queue_name=uuid4().hex, redis_url=redis_url)
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

    task = await foobar.enqueue()
    worker.loop.create_task(worker.run_async())
    assert (await task.result(3)).success


async def test_uninitialized_worker(redis_url: str):
    worker = Worker(redis_url=redis_url, queue_name=uuid4().hex)

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
    for _ in range(n_tasks):
        await foo.enqueue()
    worker.loop.create_task(worker.run_async())
    await asyncio.sleep(1)
    assert worker.active == n_tasks


async def test_handle_signal(worker: Worker):
    @worker.task()
    async def foo() -> None:
        await asyncio.sleep(3)

    worker._handle_signals = True
    worker.loop.create_task(worker.run_async())
    await asyncio.sleep(1)
    worker.handle_signal(Signals.SIGINT)
    await asyncio.sleep(1)
    task = await foo.enqueue()
    assert await task.status() == TaskStatus.QUEUED


async def test_reclaim_idle_task(redis_url: str):
    worker2 = Worker(
        redis_url=redis_url,
        queue_name="reclaim",
    )

    @worker2.task(timeout=3)
    async def foo() -> None:
        await asyncio.sleep(2)

    # enqueue task
    async with worker2:
        task = await foo.enqueue()
    # run separate worker which will pick up task
    worker1 = subprocess.Popen([sys.executable, "tests/failure.py", redis_url])
    await asyncio.sleep(1)
    # kill worker abruptly to disallow cleanup
    os.kill(worker1.pid, signal.SIGKILL)
    worker1.wait()

    worker2.loop.create_task(worker2.run_async())
    assert (await task.result(8)).success
    await worker2.close()


async def test_change_cron_schedule(redis_url: str):
    async def foo() -> None:
        pass

    worker1 = Worker(
        redis_url=redis_url,
        queue_name=uuid4().hex,
        handle_signals=False,
    )
    foo1 = worker1.cron("0 0 1 1 *")(foo)
    worker1.loop.create_task(worker1.run_async())
    await asyncio.sleep(2)
    await worker1.close()
    task1 = foo1.enqueue()
    assert foo1.schedule() == (await task1.info()).scheduled

    worker2 = Worker(
        redis_url=redis_url,
        queue_name=worker1.queue_name,
        handle_signals=False,
    )
    foo2 = worker2.cron("1 0 1 1 *")(foo)  # 1 minute later
    worker2.loop.create_task(worker2.run_async())
    await asyncio.sleep(2)
    await worker2.close()
    task2 = foo2.enqueue()
    assert foo2.schedule() == (await task2.info()).scheduled
    assert foo1.schedule() != foo2.schedule()


async def test_signed_data(redis_url: str):
    worker = Worker(
        redis_url=redis_url,
        queue_name=uuid4().hex,
        handle_signals=False,
        signing_secret=secrets.token_urlsafe(32),
    )

    @worker.task()
    async def foo() -> str:
        return "bar"

    worker.loop.create_task(worker.run_async())
    async with worker:
        task = await foo.enqueue()
        res = await task.result(3)
        assert res.success and res.result == "bar"


async def test_sign_non_binary_data(redis_url: str):
    worker = Worker(
        redis_url=redis_url,
        queue_name=uuid4().hex,
        handle_signals=False,
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
            task._task_key(REDIS_TASK), pickle.dumps({"f": "This is an attack!"})
        )
        worker.loop.create_task(worker.run_async())
        res = await task.result(5)
        assert not res.success and isinstance(res.result, StreaqError)


async def test_enqueue_many(redis_url: str):
    worker = Worker(redis_url=redis_url, queue_name=uuid4().hex)

    @worker.task()
    async def foobar(val: int) -> int:
        await asyncio.sleep(1)
        return val

    tasks = [foobar.enqueue(i) for i in range(10)]
    async with worker:
        await worker.enqueue_many(tasks)
    assert await worker.queue_size() >= len(tasks)


async def test_invalid_task_context(redis_url: str):
    worker = Worker(redis_url=redis_url, queue_name=uuid4().hex)

    with pytest.raises(StreaqError):
        worker.task_context()

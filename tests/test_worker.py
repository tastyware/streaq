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

from streaq.constants import REDIS_TASK
from streaq.utils import StreaqError, gather
from streaq.worker import Worker

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

    @worker.task()
    async def foobar() -> str:
        return worker.context.name

    async with create_task_group() as tg:
        await tg.start(worker.run_async)
        task = await foobar.enqueue()
        res = await task.result(3)
        assert res.success and res.result == NAME_STR
        tg.cancel_scope.cancel()


async def test_health_check(redis_url: str):
    worker = Worker(
        redis_url=redis_url,
        redis_kwargs={"decode_responses": True},
        health_crontab="* * * * * * *",
        queue_name=uuid4().hex,
    )
    async with create_task_group() as tg:
        await tg.start(worker.run_async)
        await sleep(2)
        worker_health = await worker.redis.get(f"{worker._health_key}:{worker.id}")
        redis_health = await worker.redis.get(worker._health_key + ":redis")
        assert worker_health is not None
        assert redis_health is not None
        tg.cancel_scope.cancel()


async def test_queue_size(worker: Worker):
    async with worker:
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
    async with create_task_group() as tg:
        await tg.start(worker.run_async)
        task = await foobar.enqueue()
        with pytest.raises(StreaqError):
            await task.result(3)


async def test_custom_serializer(worker: Worker):
    worker.serializer = json.dumps
    worker.deserializer = json.loads

    @worker.task()
    async def foobar() -> None:
        pass

    async with create_task_group() as tg:
        await tg.start(worker.run_async)
        task = await foobar.enqueue()
        assert (await task.result(3)).success
        tg.cancel_scope.cancel()


async def test_uninitialized_worker(worker: Worker):
    @worker.task()
    async def foobar() -> None:
        print(worker.context)

    with pytest.raises(StreaqError):
        await foobar.run()
    with pytest.raises(StreaqError):
        await worker.redis.ping()


async def test_active_tasks(worker: Worker):
    @worker.task()
    async def foo() -> None:
        await sleep(10)

    n_tasks = 5
    tasks = [foo.enqueue() for _ in range(n_tasks)]
    async with create_task_group() as tg:
        await tg.start(worker.run_async)
        await worker.enqueue_many(tasks)
        await sleep(3)
        assert worker.active >= n_tasks
        tg.cancel_scope.cancel()


async def test_handle_signal(worker: Worker):
    @worker.task()
    async def foo() -> None:
        await sleep(3)

    async with create_task_group() as tg:
        await tg.start(worker.run_async)
        await foo.enqueue()
        await sleep(1)
        assert worker.active > 0
        os.kill(os.getpid(), signal.SIGINT)
        await sleep(1)
        assert worker.active == 0


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

    # enqueue tasks
    tasks = [registered.enqueue() for _ in range(4)]
    async with create_task_group() as tg:
        # run first worker which will pick up all tasks
        await tg.start(worker.run_async)
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
        await sleep(2)

    # enqueue task
    task = foo.enqueue()
    # run separate worker which will pick up task
    worker = subprocess.Popen([sys.executable, "tests/failure.py", redis_url, task.id])
    await sleep(1)
    # kill worker abruptly to disallow cleanup
    os.kill(worker.pid, signal.SIGKILL)
    worker.wait()

    async with create_task_group() as tg:
        await tg.start(worker2.run_async)
        assert (await task.result(8)).success
        tg.cancel_scope.cancel()


async def test_change_cron_schedule(redis_url: str):
    async def foo() -> None:
        pass

    worker = Worker(redis_url=redis_url, queue_name=uuid4().hex)
    foo1 = worker.cron("0 0 1 1 *")(foo)
    async with create_task_group() as tg:
        await tg.start(worker.run_async)
        await sleep(2)
        task1 = foo1.enqueue()
        info = await task1.info()
        assert info and foo1.schedule() == info.scheduled
        tg.cancel_scope.cancel()

    worker2 = Worker(redis_url=redis_url, queue_name=worker.queue_name)
    foo2 = worker2.cron("1 0 1 1 *")(foo)  # 1 minute later
    async with create_task_group() as tg:
        await tg.start(worker2.run_async)
        await sleep(2)
        task2 = foo2.enqueue()
        info2 = await task2.info()
        assert info2 and foo2.schedule() == info2.scheduled
        assert foo1.schedule() != foo2.schedule()
        tg.cancel_scope.cancel()


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
        await tg.start(worker.run_async)
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
        await tg.start(worker.run_async)
        res = await task.result(5)
        assert not res.success and isinstance(res.exception, StreaqError)
        tg.cancel_scope.cancel()


async def test_enqueue_many(worker: Worker):
    @worker.task()
    async def foobar(val: int) -> int:
        await sleep(1)
        return val

    async with worker:
        tasks = [foobar.enqueue(i) for i in range(10)]
        delayed = foobar.enqueue(1).start(delay=1)
        depends = foobar.enqueue(1).start(after=delayed.id)
        tasks.extend([delayed, depends])
        await worker.enqueue_many(tasks)
        assert await worker.queue_size() >= 10


async def test_invalid_task_context(worker: Worker):
    with pytest.raises(StreaqError):
        worker.task_context()


async def test_custom_worker_id(redis_url: str):
    worker_id = uuid4().hex
    worker = Worker(redis_url=redis_url, queue_name=uuid4().hex, id=worker_id)

    assert worker.id == worker_id

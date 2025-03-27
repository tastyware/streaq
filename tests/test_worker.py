import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from signal import Signals
from typing import AsyncIterator
from uuid import uuid4

import pytest

from streaq.task import TaskStatus
from streaq.types import WrappedContext
from streaq.utils import StreaqError
from streaq.worker import Worker

NAME_STR = "Freddy"


async def test_worker_redis(redis_url: str):
    worker = Worker(redis_url=redis_url)
    await worker.redis.ping()


@dataclass
class Context:
    name: str


@asynccontextmanager
async def deps(worker: Worker) -> AsyncIterator[Context]:
    yield Context(NAME_STR)


async def test_lifespan(redis_url: str):
    worker = Worker(redis_url=redis_url, lifespan=deps)

    @worker.task()
    async def foobar(ctx: WrappedContext[Context]) -> str:
        return ctx.deps.name

    @worker.task(timeout=1)
    async def foobar2(ctx: WrappedContext[Context]) -> None:
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
        queue_name="test",
        handle_signals=False,
        with_scheduler=True,
    )
    await worker.redis.flushdb()
    worker.loop.create_task(worker.run_async())
    await asyncio.sleep(2)
    worker_health = await worker.redis.hget(worker._health_key, worker.id)  # type: ignore
    redis_health = await worker.redis.hget(worker._health_key, "redis")  # type: ignore
    assert worker_health is not None
    assert redis_health is not None
    await worker.close()


async def test_queue_size(redis_url: str):
    worker = Worker(queue_name=uuid4().hex, redis_url=redis_url)
    assert await worker.queue_size() == 0


def raise_error(*arg, **kwargs) -> None:
    raise Exception("Couldn't serialize/deserialize!")


async def test_bad_serializer(redis_url: str):
    worker = Worker(redis_url=redis_url, serializer=raise_error)  # type: ignore

    @worker.task()
    async def foobar(ctx: WrappedContext[None]) -> None:
        print("This can't print!")

    async with worker:
        with pytest.raises(StreaqError):
            await foobar.enqueue()


async def test_bad_deserializer(redis_url: str):
    worker = Worker(redis_url=redis_url, deserializer=raise_error)

    @worker.task()
    async def foobar(ctx: WrappedContext[None]) -> None:
        print("This can't print!")

    worker.burst = True
    async with worker:
        task = await foobar.enqueue()

    await worker.run_async()
    with pytest.raises(StreaqError):
        await task.result(3)


async def test_uninitialized_worker(redis_url: str):
    worker = Worker(redis_url=redis_url)

    @worker.task()
    async def foobar(ctx: WrappedContext[None]) -> None:
        pass

    with pytest.raises(StreaqError):
        await foobar.run()
    with pytest.raises(StreaqError):
        await foobar.enqueue()


async def test_active_tasks(worker: Worker):
    @worker.task()
    async def foo(ctx: WrappedContext[None]) -> None:
        await asyncio.sleep(3)

    n_tasks = 5
    for _ in range(n_tasks):
        await foo.enqueue()
    worker.loop.create_task(worker.run_async())
    await asyncio.sleep(1)
    assert worker.active == n_tasks


async def test_handle_signal(worker: Worker):
    @worker.task()
    async def foo(ctx: WrappedContext[None]) -> None:
        await asyncio.sleep(3)

    worker._handle_signals = True
    worker.loop.create_task(worker.run_async())
    await asyncio.sleep(1)
    worker.handle_signal(Signals.SIGINT)
    await asyncio.sleep(1)
    task = await foo.enqueue()
    assert await task.status() == TaskStatus.QUEUED

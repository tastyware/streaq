import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import timedelta
from typing import AsyncIterator

import pytest

from streaq.constants import REDIS_HEALTH, REDIS_PREFIX, REDIS_QUEUE
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


async def test_worker_lifespan(redis_url: str):
    worker = Worker(redis_url=redis_url, worker_lifespan=deps)

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


async def test_health_check(worker: Worker):
    worker.health_check_interval = timedelta(milliseconds=500)

    async def get_health_after_delay() -> str | None:
        await asyncio.sleep(1)
        return await worker.redis.hget(
            REDIS_PREFIX + worker.queue_name + REDIS_HEALTH, worker.id
        )  # type: ignore

    done, _ = await asyncio.wait(
        [
            asyncio.ensure_future(get_health_after_delay()),
            asyncio.ensure_future(worker.run_async()),
        ],
        return_when=asyncio.FIRST_COMPLETED,
    )
    for finished in done:
        assert finished.result() is not None


async def test_redis_health_check(worker: Worker):
    worker.health_check_interval = timedelta(milliseconds=500)

    async def get_health_after_delay() -> str | None:
        await asyncio.sleep(1)
        return await worker.redis.hget(
            REDIS_PREFIX + worker.queue_name + REDIS_HEALTH, "redis"
        )  # type: ignore

    done, _ = await asyncio.wait(
        [
            asyncio.ensure_future(get_health_after_delay()),
            asyncio.ensure_future(worker.run_async()),
        ],
        return_when=asyncio.FIRST_COMPLETED,
    )
    for finished in done:
        assert finished.result() is not None


async def test_queue_size(redis_url: str):
    worker = Worker(redis_url=redis_url)
    await worker.redis.delete(REDIS_PREFIX + worker.queue_name + REDIS_QUEUE)
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

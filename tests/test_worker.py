import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import timedelta
from typing import AsyncIterator

from streaq.constants import REDIS_HEALTH
from streaq.types import WrappedContext
from streaq.worker import Worker

NAME_STR = "Freddy"


async def test_worker_redis(redis_url: str):
    worker = Worker(redis_url=redis_url)
    await worker.redis.ping()


@dataclass
class Context:
    name: str


@asynccontextmanager
async def deps() -> AsyncIterator[Context]:
    yield Context(NAME_STR)


async def test_worker_lifespan(redis_url: str):
    worker = Worker(redis_url=redis_url, worker_lifespan=deps)

    @worker.task()
    async def foobar(ctx: WrappedContext[Context]) -> str:
        return ctx.deps.name

    async with worker:
        res = await foobar.run()
        assert res == NAME_STR

    # TODO: clear out redis


async def test_health_check(redis_url: str):
    worker = Worker(
        redis_url=redis_url, health_check_interval=timedelta(milliseconds=500)
    )

    async def get_health_after_delay() -> str | None:
        await asyncio.sleep(1)
        return await worker.redis.hget(worker.queue_name + REDIS_HEALTH, worker.id)  # type: ignore

    done, _ = await asyncio.wait(
        [get_health_after_delay(), worker.run_async()],
        return_when=asyncio.FIRST_COMPLETED,
    )
    for finished in done:
        assert finished.result() is not None


async def test_redis_health_check(redis_url: str):
    worker = Worker(redis_url=redis_url)

    async def get_health_after_delay() -> str | None:
        await asyncio.sleep(1)
        return await worker.redis.hget(worker.queue_name + REDIS_HEALTH, "redis")  # type: ignore

    done, _ = await asyncio.wait(
        [get_health_after_delay(), worker.run_async()],
        return_when=asyncio.FIRST_COMPLETED,
    )
    for finished in done:
        assert finished.result() is not None

from contextlib import asynccontextmanager
from typing import AsyncGenerator, Literal
from uuid import uuid4

from anyio import create_task_group
from pytest import fixture

from streaq import Worker


@fixture(scope="session")
def redis_url() -> str:
    return "redis://redis-master:6379"


@fixture(scope="function")
def sentinel_worker(anyio_backend: Literal["asyncio", "trio"]) -> Worker:
    return Worker(
        sentinel_nodes=[
            ("sentinel-1", 26379),
            ("sentinel-2", 26379),
            ("sentinel-3", 26379),
        ],
        sentinel_master="mymaster",
        queue_name=uuid4().hex,
        anyio_backend=anyio_backend,
    )


@fixture(scope="function")
def normal_worker(anyio_backend: Literal["asyncio", "trio"], redis_url: str) -> Worker:
    return Worker(
        redis_url=redis_url, queue_name=uuid4().hex, anyio_backend=anyio_backend
    )


@fixture(params=["direct", "sentinel"], ids=["redis", "sentinel"])
def worker(request, normal_worker: Worker, sentinel_worker: Worker) -> Worker:
    return normal_worker if request.param == "direct" else sentinel_worker


@asynccontextmanager
async def run_worker(_worker: Worker) -> AsyncGenerator[None, None]:
    async with create_task_group() as tg:
        await tg.start(_worker.run_async)
        yield
        tg.cancel_scope.cancel()

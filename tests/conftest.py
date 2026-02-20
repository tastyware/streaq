import os
import signal
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from uuid import uuid4

import pytest
from anyio import create_task_group

from streaq import Worker


@pytest.fixture(scope="session")
def redis_url() -> str:
    return "redis://redis-master:6379"


@pytest.fixture(
    params=[
        pytest.param(("asyncio", {"use_uvloop": False}), id="asyncio"),
        pytest.param(("asyncio", {"use_uvloop": True}), id="asyncio+uvloop"),
        pytest.param(("trio", {}), id="trio"),
    ]
)
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return request.param


@pytest.fixture(scope="function")
def sentinel() -> Worker:
    return Worker(
        sentinel_nodes=[
            ("sentinel-1", 26379),
            ("sentinel-2", 26379),
            ("sentinel-3", 26379),
        ],
        sentinel_master="mymaster",
        queue_name=uuid4().hex,
    )


@pytest.fixture(scope="function")
def cluster() -> Worker:
    return Worker(cluster_nodes=[("cluster-1", 7000)], queue_name=f"{{{uuid4().hex}}}")


@pytest.fixture(scope="function")
def basic(redis_url: str) -> Worker:
    return Worker(redis_url=redis_url, queue_name=uuid4().hex)


@pytest.fixture(
    params=["basic", "sentinel", "cluster"], ids=["basic", "sentinel", "cluster"]
)
def worker(request: pytest.FixtureRequest) -> Worker:
    return request.getfixturevalue(request.param)


@asynccontextmanager
async def run_worker(_worker: Worker) -> AsyncGenerator[None, None]:
    async with create_task_group() as tg:
        await tg.start(_worker.run_async)
        yield
        os.kill(os.getpid(), signal.SIGINT)

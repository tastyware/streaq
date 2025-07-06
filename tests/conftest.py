from typing import Any, AsyncGenerator, Generator
from uuid import uuid4

from pytest import fixture
from testcontainers.redis import RedisContainer

from streaq import Worker


@fixture(scope="session")
def redis_container() -> Generator[RedisContainer, Any, None]:
    with RedisContainer() as container:
        yield container
        container.get_client().flushdb()


@fixture(scope="session")
def redis_url(redis_container: RedisContainer) -> Generator[str, None, None]:
    yield f"redis://{redis_container.get_container_host_ip()}:{redis_container.port}"


@fixture(scope="function")
async def worker(redis_url: str) -> AsyncGenerator[Worker, None]:
    w = Worker(redis_url=redis_url, queue_name=uuid4().hex, handle_signals=False)
    async with w:
        yield w
    await w.close()

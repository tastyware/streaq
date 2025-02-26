from typing import Any, AsyncGenerator, Generator

from pytest import fixture
from testcontainers.redis import RedisContainer

from streaq import Worker


@fixture(scope="session")
def redis_container() -> Generator[RedisContainer, Any, None]:
    with RedisContainer() as container:
        container.get_client().select(13)
        yield container


@fixture(scope="function")
def redis_url(redis_container: RedisContainer) -> Generator[str, None, None]:
    yield f"redis://{redis_container.get_container_host_ip()}:{redis_container.port}"
    redis_container.get_client().flushdb()


@fixture(scope="function")
async def worker(redis_url: str) -> AsyncGenerator[Worker, None]:
    w = Worker(redis_url=redis_url)
    yield w
    await w.close()

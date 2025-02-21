from typing import Generator

from pytest import fixture
from testcontainers.redis import RedisContainer

from streaq import Worker


# Run all tests with asyncio only
@fixture(scope="session")
def aiolib() -> str:
    return "asyncio"


@fixture(scope="session")
def redis_container() -> Generator[RedisContainer, None, None]:
    with RedisContainer("redis:latest") as redis:
        yield redis


@fixture(scope="session")
def redis_url(redis_container: RedisContainer) -> str:
    return f"redis://{redis_container.get_container_host_ip()}:{redis_container.port}"


@fixture(scope="session")
async def worker(redis_url: str, aiolib: str) -> Worker:
    return Worker(redis_url=redis_url)

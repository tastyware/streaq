from typing import Any, Generator
from uuid import uuid4

from pytest import fixture
from testcontainers.redis import RedisContainer

from streaq import Worker


@fixture(scope="session")
def redis_container() -> Generator[RedisContainer, Any, None]:
    with RedisContainer() as container:
        yield container


@fixture(scope="session")
def redis_url(redis_container: RedisContainer) -> Generator[str, None, None]:
    host = redis_container.get_container_host_ip()
    port = redis_container.get_exposed_port(redis_container.port)
    yield f"redis://{host}:{port}"


@fixture(scope="function")
def worker(redis_url: str) -> Worker:
    return Worker(redis_url=redis_url, queue_name=uuid4().hex)

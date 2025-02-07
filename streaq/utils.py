import time
from datetime import datetime, timedelta
from importlib import import_module
from typing import Any, Callable

from redis.asyncio import Redis


class StreaqError(Exception):
    pass


class StreaqRetry(RuntimeError):
    """
    Special exception to retry the job (if ``max_retries`` hasn't been reached).

    :param defer: duration to wait before rerunning the job
    """

    def __init__(self, defer: timedelta | int | None = None):
        self.defer_score: int | None = to_ms(defer) if defer is not None else None

    def __repr__(self) -> str:
        return f"<Retry defer {(self.defer_score or 0) / 1000:0.2f}s>"

    def __str__(self) -> str:
        return repr(self)


def import_string(dotted_path: str) -> Any:
    """
    Taken from pydantic.utils.
    """

    try:
        module_path, class_name = dotted_path.strip(" ").rsplit(".", 1)
    except ValueError as e:
        raise ImportError(f"'{dotted_path}' doesn't look like a module path") from e

    module = import_module(module_path)
    try:
        return getattr(module, class_name)
    except AttributeError as e:
        raise ImportError(
            f"Module '{module_path}' does not define a '{class_name}' attribute"
        ) from e


def to_seconds(timeout: timedelta | int) -> int:
    if isinstance(timeout, timedelta):
        return round(timeout.total_seconds())
    return timeout


def to_ms(timeout: timedelta | int) -> int:
    if isinstance(timeout, timedelta):
        return round(timeout.total_seconds() * 1000)
    return timeout * 1000


def now_ms() -> int:
    return round(time.time() * 1000)


def datetime_ms(dt: datetime) -> int:
    return round(dt.timestamp() * 1000)


async def log_redis_info(redis: Redis, log_func: Callable[[str], Any]) -> None:
    async with redis.pipeline(transaction=False) as pipe:
        pipe.info(section="Server")
        pipe.info(section="Memory")
        pipe.info(section="Clients")
        pipe.dbsize()
        info_server, info_memory, info_clients, key_count = await pipe.execute()

    redis_version = info_server.get("redis_version", "?")
    mem_usage = info_memory.get("used_memory_human", "?")
    clients_connected = info_clients.get("connected_clients", "?")

    log_func(
        f"redis_version={redis_version} "
        f"mem_usage={mem_usage} "
        f"clients_connected={clients_connected} "
        f"db_keys={key_count}"
    )

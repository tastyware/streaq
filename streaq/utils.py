import time
from datetime import datetime, timedelta
from functools import partial, wraps
from importlib import import_module
from typing import Any, Callable, Coroutine

from anyio.abc import CapacityLimiter
from anyio.to_thread import run_sync

from streaq.types import P, R


class StreaqError(Exception):
    pass


def import_string(dotted_path: str) -> Any:
    """
    Taken from pydantic.utils. Import and return the object at a path.
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


def to_seconds(timeout: timedelta | int) -> float:
    if isinstance(timeout, timedelta):
        return timeout.total_seconds()
    return float(timeout)


def to_ms(timeout: timedelta | int) -> int:
    if isinstance(timeout, timedelta):
        return round(timeout.total_seconds() * 1000)
    return timeout * 1000


def now_ms() -> int:
    return round(time.time() * 1000)


def datetime_ms(dt: datetime) -> int:
    return round(dt.timestamp() * 1000)


def default_log_config(verbose: bool) -> dict[str, Any]:
    """
    Setup default config. for dictConfig.

    :param verbose: level: DEBUG if True, INFO if False
    :return: dict suitable for ``logging.config.dictConfig``
    """
    log_level = "DEBUG" if verbose else "INFO"
    return {
        "version": 1,
        "disable_existing_loggers": False,
        "handlers": {
            "streaq.standard": {
                "level": log_level,
                "class": "logging.StreamHandler",
                "formatter": "streaq.standard",
            }
        },
        "formatters": {
            "streaq.standard": {
                "format": "%(asctime)s: %(message)s",
                "datefmt": "%H:%M:%S",
            }
        },
        "loggers": {"streaq": {"handlers": ["streaq.standard"], "level": log_level}},
    }


def asyncify(
    fn: Callable[P, R], limiter: CapacityLimiter
) -> Callable[P, Coroutine[Any, Any, R]]:
    """
    Taken from asyncer v0.0.8

    Take a blocking function and create an async one that receives the same
    positional and keyword arguments, and that when called, calls the original
    function in a worker thread using `anyio.to_thread.run_sync()`.

    If the task waiting for its completion is cancelled, the thread will still
    run its course but its result will be ignored.

    Example usage::

        def do_work(arg1, arg2, kwarg1="", kwarg2="") -> str:
            return "stuff"

        result = await to_thread.asyncify(do_work)("spam", "ham", kwarg1="a", kwarg2="b")
        print(result)

    :param fn: a blocking regular callable (e.g. a function)
    :param limiter: a CapacityLimiter instance to limit the number of concurrent
        threads running the blocking function.

    :return:
        An async function that takes the same positional and keyword arguments as the
        original one, that when called runs the same original function in a thread
        worker and returns the result.
    """

    @wraps(fn)
    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        call = partial(fn, *args, **kwargs)
        return await run_sync(call, abandon_on_cancel=True, limiter=limiter)

    return wrapper

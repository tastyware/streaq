import time
from datetime import datetime, timedelta, tzinfo
from functools import partial, wraps
from importlib import import_module
from logging import Formatter
from typing import Any, Awaitable, Callable, TypeVar, overload

from anyio import CapacityLimiter, create_task_group
from anyio.to_thread import run_sync

from streaq.types import P, R, TypedCoroutine


class TimezoneFormatter(Formatter):
    def __init__(
        self,
        fmt: str | None = None,
        datefmt: str | None = None,
        tz: tzinfo | None = None,
        **kwargs: Any,
    ):
        """
        Like a normal formatter, but with a timezone.
        """
        super().__init__(fmt, datefmt, **kwargs)
        self.tz = tz

    def converter(self, *args: Any) -> time.struct_time:
        return datetime.now(self.tz).timetuple()


def import_string(dotted_path: str) -> Any:
    """
    Taken from pydantic.utils. Import and return the object at a path.
    """

    try:
        module_path, class_name = dotted_path.strip(" ").rsplit(":", 1)
    except ValueError as e:
        raise ImportError(f"'{dotted_path}' doesn't look like a module path") from e

    module = import_module(module_path)
    try:
        return getattr(module, class_name)
    except AttributeError as e:
        raise ImportError(
            f"Module '{module_path}' does not define a '{class_name}' attribute"
        ) from e


def to_seconds(timeout: timedelta | int | None) -> float | None:
    if isinstance(timeout, timedelta):
        return timeout.total_seconds()
    return float(timeout) if timeout is not None else None


def to_ms(timeout: timedelta | int | float) -> int:
    if isinstance(timeout, timedelta):
        return round(timeout.total_seconds() * 1000)
    return round(timeout * 1000)


def now_ms() -> int:
    """
    Get current time in milliseconds.
    """
    return round(time.time() * 1000)


def datetime_ms(dt: datetime) -> int:
    return round(dt.timestamp() * 1000)


def to_tuple(val: Any) -> tuple[Any, ...]:
    """
    Turn the given value into a tuple of one element, unless it's already a tuple, in
    which case it's left untouched.
    """
    return val if isinstance(val, tuple) else (val,)  # type: ignore


def default_log_config(tz: tzinfo, verbose: bool) -> dict[str, Any]:
    """
    Setup default config. for dictConfig.

    :param tz: timezone for logs
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
                "()": TimezoneFormatter,
                "format": "[%(levelname)s] %(asctime)s: %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
                "tz": tz,
            }
        },
        "loggers": {"streaq": {"handlers": ["streaq.standard"], "level": log_level}},
    }


def asyncify(
    fn: Callable[P, R], limiter: CapacityLimiter | None = None
) -> Callable[P, TypedCoroutine[R]]:
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

        result = await to_thread.asyncify(do_work)(
            "spam",
            "ham",
            kwarg1="a",
            kwarg2="b"
        )
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


T1 = TypeVar("T1")
T2 = TypeVar("T2")
T3 = TypeVar("T3")
T4 = TypeVar("T4")
T5 = TypeVar("T5")


@overload
async def gather(
    awaitable1: Awaitable[T1], awaitable2: Awaitable[T2], /
) -> tuple[T1, T2]: ...


@overload
async def gather(
    awaitable1: Awaitable[T1], awaitable2: Awaitable[T2], awaitable3: Awaitable[T3], /
) -> tuple[T1, T2, T3]: ...


@overload
async def gather(
    awaitable1: Awaitable[T1],
    awaitable2: Awaitable[T2],
    awaitable3: Awaitable[T3],
    awaitable4: Awaitable[T4],
    /,
) -> tuple[T1, T2, T3, T4]: ...


@overload
async def gather(
    awaitable1: Awaitable[T1],
    awaitable2: Awaitable[T2],
    awaitable3: Awaitable[T3],
    awaitable4: Awaitable[T4],
    awaitable5: Awaitable[T5],
    /,
) -> tuple[T1, T2, T3, T4, T5]: ...


@overload
async def gather(*awaitables: Awaitable[T1]) -> tuple[T1, ...]: ...


async def gather(*awaitables: Awaitable[Any]) -> tuple[Any, ...]:
    """
    anyio-compatible implementation of asyncio.gather that runs tasks in a task group
    and collects the results.
    """
    if not awaitables:
        return ()
    results: list[Any] = [None] * len(awaitables)

    async def runner(awaitable: Awaitable[Any], i: int) -> None:
        results[i] = await awaitable

    async with create_task_group() as tg:
        for i, awaitable in enumerate(awaitables):
            tg.start_soon(runner, awaitable, i)
    return tuple(results)

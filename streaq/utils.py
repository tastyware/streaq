import time
from datetime import datetime, timedelta
from importlib import import_module
from typing import Any


class StreaqError(Exception):
    pass


class StreaqRetry(RuntimeError):
    """
    Special exception to retry the job (if ``max_retries`` hasn't been reached).

    :param delay: duration to wait before rerunning the job
    """

    def __init__(self, delay: timedelta | int = 0):
        self.delay: int = to_ms(delay)


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

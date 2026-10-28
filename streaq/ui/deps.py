from collections.abc import Callable
from pathlib import Path
from traceback import format_exception
from typing import Any

from fastapi import HTTPException, status
from fastapi.templating import Jinja2Templates

from streaq import Worker


def humanize(count: int) -> str:
    """
    Shorten large counts for display, eg 1234567890 -> 1.235B.
    """
    if count < 1000:
        return str(count)
    value = float(count)
    for unit in ("K", "M", "B"):
        value /= 1000
        # check after rounding, so 999999999 shows as 1.000B instead of 1000.000M
        if round(value, 3) < 1000:
            return f"{value:.3f}{unit}"
    return f"{value / 1000:.3f}T"


BASE_DIR = Path(__file__).parent / "templates"
templates = Jinja2Templates(directory=str(BASE_DIR))
templates.env.filters["humanize"] = humanize
templates.env.autoescape = True


def get_worker() -> Worker[Any]:
    raise HTTPException(
        status_code=status.HTTP_412_PRECONDITION_FAILED,
        detail="get_worker dependency not implemented!",
    )


def get_result_formatter() -> Callable[[Any], str]:
    return str


def get_exception_formatter() -> Callable[[BaseException], str]:
    def _format_exc(exc: BaseException) -> str:
        return "".join(format_exception(exc))

    return _format_exc

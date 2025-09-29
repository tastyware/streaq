from pathlib import Path
from textwrap import shorten
from traceback import format_exception
from typing import Any, Callable

from fastapi import HTTPException, status
from fastapi.templating import Jinja2Templates

from streaq import Worker

BASE_DIR = Path(__file__).parent / "templates"
templates = Jinja2Templates(directory=str(BASE_DIR))


def get_worker() -> Worker[Any]:
    raise HTTPException(
        status_code=status.HTTP_412_PRECONDITION_FAILED,
        detail="get_worker dependency not implemented!",
    )


def get_result_formatter() -> Callable[[Any], str]:
    def _format_result(result: Any) -> str:
        return shorten(str(result), width=80, placeholder="…")

    return _format_result


def get_exception_formatter() -> Callable[[BaseException], str]:
    def _format_exc(exc: BaseException) -> str:
        return "".join(format_exception(exc))

    return _format_exc

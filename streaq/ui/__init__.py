import os
import sys
from typing import Any, AsyncGenerator, cast

from streaq import Worker
from streaq.ui.deps import get_exception_formatter, get_result_formatter, get_worker
from streaq.ui.tasks import router
from streaq.utils import import_string

__all__ = [
    "get_worker",
    "get_result_formatter",
    "get_exception_formatter",
    "router",
]


def run_web(host: str, port: int, worker_path: str) -> None:  # pragma: no cover
    import uvicorn
    from fastapi import FastAPI

    async def _get_worker() -> AsyncGenerator[Worker[Any], None]:
        sys.path.append(os.getcwd())
        worker = cast(Worker[Any], import_string(worker_path))
        yield worker

    app = FastAPI()
    app.dependency_overrides[get_worker] = _get_worker
    app.include_router(router)
    uvicorn.run(app, host=host, port=port)

from typing import Any, AsyncGenerator

from streaq import Worker
from streaq.ui.deps import get_worker
from streaq.ui.tasks import router

__all__ = ["get_worker", "router"]


def run_web(host: str, port: int, worker: Worker[Any]) -> None:  # pragma: no cover
    import uvicorn
    from fastapi import FastAPI

    async def _get_worker() -> AsyncGenerator[Worker[Any], None]:
        yield worker

    app = FastAPI()
    app.dependency_overrides[get_worker] = _get_worker
    app.include_router(router)
    uvicorn.run(app, host=host, port=port)

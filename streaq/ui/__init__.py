from typing import Any, AsyncGenerator

from fastapi import status
from fastapi.responses import RedirectResponse

from streaq import Worker
from streaq.ui.deps import get_worker
from streaq.ui.tasks import router


@router.get("/")
async def get_root() -> RedirectResponse:
    return RedirectResponse("/queue", status_code=status.HTTP_303_SEE_OTHER)


def run_web(host: str, port: int, worker: Worker[Any]) -> None:
    import uvicorn
    from fastapi import FastAPI

    async def _get_worker() -> AsyncGenerator[Worker[Any], None]:
        yield worker

    app = FastAPI()
    app.dependency_overrides[get_worker] = _get_worker
    app.include_router(router)
    uvicorn.run(app, host=host, port=port)

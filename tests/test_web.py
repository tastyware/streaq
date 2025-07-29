import asyncio

import pytest
from anyio import create_task_group
from fastapi import FastAPI, HTTPException
from httpx import ASGITransport, AsyncClient

from streaq.ui import router
from streaq.ui.deps import get_worker
from streaq.worker import Worker

pytestmark = pytest.mark.anyio


async def test_no_override():
    with pytest.raises(HTTPException):
        _ = await get_worker()


@pytest.mark.parametrize("prefix", ["", "/streaq"])
async def test_get_pages(worker: Worker, prefix: str):
    app = FastAPI()
    app.include_router(router, prefix=prefix)
    worker.concurrency = 1
    worker.prefetch = 0

    @worker.task()
    async def sleeper(time: int) -> None:
        await asyncio.sleep(time)

    async def _get_worker():
        yield worker

    # queue up some tasks
    async with worker:
        scheduled = await sleeper.enqueue(10).start(delay=5)
        done = await sleeper.enqueue(0)
        running = await sleeper.enqueue(10)
        queued = await sleeper.enqueue(10)

    app.dependency_overrides[get_worker] = _get_worker
    async with create_task_group() as tg:
        tg.start_soon(worker.run_async)
        await asyncio.sleep(2)
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            # endpoints
            res = await client.get(f"{prefix}/")
            assert res.status_code == 303
            res = await client.get(f"{prefix}/queue")
            assert res.status_code == 200
            res = await client.patch(
                f"{prefix}/queue",
                data={
                    "functions": ["redis_health_check"],
                    "statuses": ["queued", "running", "done"],
                },
            )
            assert res.status_code == 200

            res = await client.get(f"{prefix}/task/{done.id}")
            assert res.status_code == 200
            res = await client.get(f"{prefix}/task/{running.id}")
            assert res.status_code == 200
            res = await client.get(f"{prefix}/task/{queued.id}")
            assert res.status_code == 200
            res = await client.delete(f"{prefix}/task/{scheduled.id}")
            assert res.status_code == 200
            assert res.headers["HX-Redirect"] == f"{prefix}/queue"
        # cleanup worker
        tg.cancel_scope.cancel()

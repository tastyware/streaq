import asyncio

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from streaq.ui import router
from streaq.ui.deps import get_worker
from streaq.worker import Worker


@pytest.mark.parametrize("prefix", ["", "/streaq"])
async def test_get_pages(worker: Worker, prefix: str):
    app = FastAPI()
    app.include_router(router, prefix=prefix)

    @worker.task()
    async def sleeper(time: int) -> None:
        await asyncio.sleep(time)

    async def _get_worker():
        yield worker

    app.dependency_overrides[get_worker] = _get_worker
    worker.loop.create_task(worker.run_async())

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        # queue up some tasks
        tasks = [sleeper.enqueue(i) for i in range(10)]
        for t in tasks[:5]:
            t.delay = 3
        await worker.enqueue_many(tasks)
        # endpoints
        res = await client.get(f"{prefix}/")
        assert res.status_code == 303
        res = await client.get(f"{prefix}/queue")
        assert res.status_code == 200
        res = await client.patch(f"{prefix}/queue")
        assert res.status_code == 200

        short = await sleeper.enqueue(0)
        long = await sleeper.enqueue(5)
        res = await client.get(f"{prefix}/task/{long.id}")
        assert res.status_code == 200
        res = await client.delete(f"{prefix}/task/{long.id}")
        assert res.status_code == 200
        assert res.headers["HX-Redirect"] == f"{prefix}/queue"
        res = await client.get(f"{prefix}/task/{short.id}")
        assert res.status_code == 200

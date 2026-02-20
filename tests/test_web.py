import pytest
from anyio import sleep
from fastapi import FastAPI, HTTPException
from httpx import ASGITransport, AsyncClient

from streaq import TaskStatus, Worker
from streaq.ui import router
from streaq.ui.deps import get_worker
from streaq.utils import gather
from tests.conftest import run_worker

pytestmark = pytest.mark.anyio


async def test_no_override():
    with pytest.raises(HTTPException):
        _ = get_worker()


async def test_get_pages(worker: Worker):
    # no lifespan because we already handle that
    app = FastAPI()
    prefix = "/streaq"
    app.include_router(router, prefix=prefix)
    worker.concurrency = 1
    worker.prefetch = 1

    @worker.task
    async def sleeper(time: int) -> None:
        await sleep(time)

    @worker.task
    async def fails() -> None:
        raise Exception("Oh no!")

    app.dependency_overrides[get_worker] = lambda: worker
    async with run_worker(worker):
        # queue up some tasks
        failed = fails.enqueue()
        scheduled = sleeper.enqueue(10).start(delay=5)
        done = sleeper.enqueue(0)
        running = sleeper.enqueue(10)
        queued = sleeper.enqueue(10)
        await worker.enqueue_many([failed, scheduled, done, running, queued])
        await gather(done.result(2), failed.result(2))  # make sure tasks are done
        while await running.status() != TaskStatus.RUNNING:
            await sleep(1)
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
                    "sort": "desc",
                },
            )
            assert res.status_code == 200
            # test fetching tasks in various statuses
            res = await gather(
                *[
                    client.get(f"{prefix}/task/{done.id}"),
                    client.get(f"{prefix}/task/{running.id}"),
                    client.get(f"{prefix}/task/{queued.id}"),
                    client.get(f"{prefix}/task/{scheduled.id}"),
                    client.get(f"{prefix}/task/{failed.id}"),
                ]
            )
            assert all(r.status_code == 200 for r in res)
            # test aborting a task manually, redirect, and bad ID
            res = await client.delete(f"{prefix}/task/{scheduled.id}")
            assert res.status_code == 200
            assert res.headers["HX-Redirect"] == f"{prefix}/queue"
            res = await client.get(f"{prefix}/task/nonexistent")
            assert res.status_code == 404

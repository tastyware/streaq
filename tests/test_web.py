from typing import Any

import anyio.lowlevel
import pytest
from anyio import TaskHandle, create_task_group, sleep
from fastapi import FastAPI, HTTPException
from httpx import ASGITransport, AsyncClient

from streaq import TaskStatus, Worker
from streaq.ui import router
from streaq.ui.deps import get_worker
from tests.conftest import run_worker

pytestmark = pytest.mark.anyio


def test_no_override():
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

    @worker.cron("* * * * *")
    async def cronjob() -> None:
        await anyio.lowlevel.checkpoint()

    app.dependency_overrides[get_worker] = lambda: worker
    async with run_worker(worker):
        # queue up some tasks
        failed = fails.enqueue()
        scheduled = sleeper.enqueue(10).start(delay=5)
        done = sleeper.enqueue(0)
        running = sleeper.enqueue(10)
        queued = sleeper.enqueue(10)
        await worker.enqueue_many([failed, scheduled, done, running, queued])
        # make sure tasks are done
        async with create_task_group() as tg:
            tg.start_soon(done.result, 2)
            tg.start_soon(failed.result, 2)
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
            for status in TaskStatus:
                res = await client.patch(
                    f"{prefix}/queue",
                    data={"functions": [sleeper.fn_name], "status": status.value},
                )
                assert res.status_code == 200
            # test fetching tasks in various statuses
            handles: list[TaskHandle[Any]] = []
            async with create_task_group() as tg:
                handles.extend(
                    tg.create_task(t)
                    for t in [
                        client.get(f"{prefix}/task/{done.id}"),
                        client.get(f"{prefix}/task/{running.id}"),
                        client.get(f"{prefix}/task/{queued.id}"),
                        client.get(f"{prefix}/task/{scheduled.id}"),
                        client.get(f"{prefix}/task/{failed.id}"),
                    ]
                )
            assert all(h.return_value.status_code == 200 for h in handles)
            # test aborting a task manually, redirect, and bad ID
            res = await client.delete(f"{prefix}/task/{scheduled.id}")
            assert res.status_code == 200
            assert res.headers["HX-Redirect"] == f"{prefix}/queue"
            res = await client.get(f"{prefix}/task/nonexistent")
            assert res.status_code == 404
            # test getting workers page
            res = await client.get(f"{prefix}/workers")
            assert res.status_code == 200
            # test getting cron page
            res = await client.get(f"{prefix}/cron")
            assert res.status_code == 200
            # test aborting cron job
            res = await client.delete(f"{prefix}/cron/{cronjob.fn_name}")
            assert res.status_code == 200
            # test aborting nonexistent cron job
            res = await client.delete(f"{prefix}/cron/nonexistent")
            assert res.status_code == 404


async def test_dead_task_routes(worker: Worker):
    app = FastAPI()
    prefix = "/streaq"
    app.include_router(router, prefix=prefix)
    app.dependency_overrides[get_worker] = lambda: worker
    worker.dlq_ttl = 60

    @worker.task
    async def fails() -> None:
        raise Exception("Oh no!")

    async with run_worker(worker):
        requeued = await fails.enqueue()
        purged = await fails.enqueue()
        await requeued.result(5)
        await purged.result(5)
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            # queue page renders the DEAD card when the DLQ is on
            res = await client.get(f"{prefix}/queue")
            assert res.status_code == 200
            res = await client.get(f"{prefix}/task/{requeued.id}")
            assert res.status_code == 200
            res = await client.post(f"{prefix}/task/{requeued.id}/requeue")
            assert res.status_code == 200
            res = await client.delete(f"{prefix}/task/{purged.id}/purge")
            assert res.status_code == 200
            # no longer dead
            res = await client.post(f"{prefix}/task/{purged.id}/requeue")
            assert res.status_code == 404
            res = await client.delete(f"{prefix}/task/{purged.id}/purge")
            assert res.status_code == 404

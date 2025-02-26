import asyncio

import pytest

from streaq import StreaqError, WrappedContext, Worker
from streaq.task import TaskStatus


async def test_result_timeout(worker: Worker):
    @worker.task(ttl=0)
    async def task1(ctx: WrappedContext[None]) -> bool:
        return False

    async with worker:
        task = await task1.enqueue()
        done, _ = await asyncio.wait(
            [task.result(5), worker.run_async()],
            return_when=asyncio.FIRST_COMPLETED,
        )
        for finished in done:
            with pytest.raises(StreaqError):
                finished.result()


async def test_task_timeout(worker: Worker):
    @worker.task(timeout=1)
    async def task2(ctx: WrappedContext[None]) -> None:
        await asyncio.sleep(5)

    async with worker:
        task = await task2.enqueue()
        done, _ = await asyncio.wait(
            [task.result(), worker.run_async()],
            return_when=asyncio.FIRST_COMPLETED,
        )
        for finished in done:
            result = finished.result()
            assert result is not None
            assert not result.success
            assert isinstance(result.result, asyncio.TimeoutError)


async def test_task_status(worker: Worker):
    @worker.task()
    async def task3(ctx: WrappedContext[None]) -> None:
        await asyncio.sleep(3)

    async with worker:
        task = task3.enqueue()
        assert await task.status() == TaskStatus.PENDING
        await task.start()
        task2 = await task3.enqueue().start(delay=5)
        assert await task.status() == TaskStatus.QUEUED
        await asyncio.wait(
            [task.result(), worker.run_async()],
            return_when=asyncio.FIRST_COMPLETED,
        )
        assert await task.status() == TaskStatus.DONE
        assert await task2.status() == TaskStatus.SCHEDULED


async def test_task_status_running(worker: Worker):
    @worker.task()
    async def task4(ctx: WrappedContext[None]) -> None:
        await asyncio.sleep(3)

    async with worker:
        task = await task4.enqueue().start()

        async def get_status_after_delay() -> tuple[TaskStatus, bool]:
            await asyncio.sleep(1)
            status = await task.status()
            aborted = await task.abort()
            return status, aborted

        done, _ = await asyncio.wait(
            [get_status_after_delay(), worker.run_async()],
            return_when=asyncio.FIRST_COMPLETED,
        )
        for finished in done:
            result = finished.result()
            assert result is not None
            assert result[0] == TaskStatus.RUNNING
            assert result[1]


async def test_task_cron(worker: Worker):
    @worker.cron("30 9 1 1 *")
    async def cron1(ctx: WrappedContext[None]) -> None:
        pass

    @worker.cron("* * * * * * *")  # once/second
    async def cron2(ctx: WrappedContext[None]) -> bool:
        return True

    async with worker:
        schedule = cron1.schedule()
        assert schedule.day == 1 and schedule.month == 1
        assert await cron2.run()
        task = cron2.enqueue()
        done, _ = await asyncio.wait(
            [task.result(3), worker.run_async()],
            return_when=asyncio.FIRST_COMPLETED,
        )
        for finished in done:
            with pytest.raises(StreaqError):
                finished.result()


async def test_task_info(redis_url: str):
    worker = Worker(redis_url=redis_url)

    @worker.task()
    async def task5(ctx: WrappedContext[None]) -> None:
        pass

    async with worker:
        task = await task5.enqueue().start(delay=5)
        task2 = await task5.enqueue()
        assert (await task.info()).scheduled is not None
        assert (await task2.info()).scheduled is None

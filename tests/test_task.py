import asyncio
import time

import pytest

from streaq import StreaqError, WrappedContext, Worker
from streaq.task import StreaqRetry, TaskStatus


async def test_result_timeout(worker: Worker):
    @worker.task(ttl=0)
    async def foobar(ctx: WrappedContext[None]) -> bool:
        return False

    async with worker:
        task = await foobar.enqueue()
        worker.loop.create_task(worker.run_async())
        with pytest.raises(StreaqError):
            await task.result(3)


async def test_task_timeout(worker: Worker):
    @worker.task(timeout=1)
    async def foobar(ctx: WrappedContext[None]) -> None:
        await asyncio.sleep(5)

    async with worker:
        task = await foobar.enqueue()
        worker.loop.create_task(worker.run_async())
        res = await task.result(3)
        assert not res.success
        assert isinstance(res.result, asyncio.TimeoutError)


async def test_task_status(worker: Worker):
    @worker.task()
    async def foobar(ctx: WrappedContext[None]) -> None:
        await asyncio.sleep(1)

    async with worker:
        task = foobar.enqueue()
        assert await task.status() == TaskStatus.PENDING
        await task.start()
        task2 = await foobar.enqueue().start(delay=5)
        assert await task.status() == TaskStatus.QUEUED
        worker.loop.create_task(worker.run_async())
        await task.result(3)
        assert await task.status() == TaskStatus.DONE
        assert await task2.status() == TaskStatus.SCHEDULED


async def test_task_status_running(worker: Worker):
    @worker.task()
    async def foobar(ctx: WrappedContext[None]) -> None:
        await asyncio.sleep(3)

    async with worker:
        task = await foobar.enqueue().start()
        worker.loop.create_task(worker.run_async())
        await asyncio.sleep(1)
        assert await task.status() == TaskStatus.RUNNING
        assert await task.abort()


async def test_task_cron(worker: Worker):
    @worker.cron("30 9 1 1 *")
    async def cron1(ctx: WrappedContext[None]) -> None:
        pass

    @worker.cron("* * * * * * *")  # once/second
    async def cron2(ctx: WrappedContext[None]) -> None:
        pass

    async with worker:
        schedule = cron1.schedule()
        assert schedule.day == 1 and schedule.month == 1
        await cron2.run()
        task = cron2.enqueue()
        worker.loop.create_task(worker.run_async())
        with pytest.raises(StreaqError):
            await task.result(3)


async def test_task_info(redis_url: str):
    worker = Worker(redis_url=redis_url)

    @worker.task()
    async def foobar(ctx: WrappedContext[None]) -> None:
        pass

    async with worker:
        task = await foobar.enqueue().start(delay=5)
        task2 = await foobar.enqueue()
        assert (await task.info()).scheduled is not None
        assert (await task2.info()).scheduled is None


async def test_task_retry(worker: Worker):
    @worker.task()
    async def foobar(ctx: WrappedContext[None]) -> int:
        if ctx.tries < 3:
            raise StreaqRetry("Retrying!")
        return ctx.tries

    async with worker:
        task = await foobar.enqueue()
        worker.loop.create_task(worker.run_async())
        res = await task.result(6)
        assert res is not None
        assert res.success
        assert res.result == 3


async def test_task_retry_with_delay(worker: Worker):
    @worker.task()
    async def foobar(ctx: WrappedContext[None]) -> int:
        if ctx.tries == 1:
            raise StreaqRetry("Retrying!", delay=3)
        return ctx.tries

    async with worker:
        task = await foobar.enqueue()
        worker.loop.create_task(worker.run_async())
        with pytest.raises(asyncio.TimeoutError):
            await task.result(3)
        res = await task.result(1)
        assert res is not None
        assert res.success
        assert res.result == 2


async def test_task_failure(worker: Worker):
    @worker.task()
    async def foobar(ctx: WrappedContext[None]) -> None:
        raise Exception("That wasn't supposed to happen!")

    async with worker:
        task = await foobar.enqueue()
        worker.loop.create_task(worker.run_async())
        res = await task.result(3)
        assert not res.success
        assert isinstance(res.result, Exception)


async def test_task_retry_no_delay(worker: Worker):
    @worker.task()
    async def foobar(ctx: WrappedContext[None]) -> bool:
        if ctx.tries == 1:
            raise StreaqRetry("Retrying!", delay=0)
        return True

    async with worker:
        task = await foobar.enqueue()
        worker.loop.create_task(worker.run_async())
        res = await task.result(3)
        assert res is not None
        assert res.success
        assert res.result


async def test_task_max_retries(worker: Worker):
    @worker.task()
    async def foobar(ctx: WrappedContext[None]) -> None:
        raise StreaqRetry("Retrying!", delay=0)

    async with worker:
        task = await foobar.enqueue()
        worker.loop.create_task(worker.run_async())
        res = await task.result(3)
        assert res is not None
        assert not res.success
        assert isinstance(res.result, StreaqError)


async def test_task_failed_abort(worker: Worker):
    @worker.task()
    async def foobar(ctx: WrappedContext[None]) -> bool:
        return True

    worker.burst = True
    async with worker:
        task = await foobar.enqueue()
        await worker.run_async()
        result = await task.result(3)
        assert result.success
        assert result.result
        aborted = await task.abort()
        assert not aborted


async def test_task_nonexistent_or_finished_dependency(worker: Worker):
    @worker.task()
    async def foobar(ctx: WrappedContext[None]) -> None:
        pass

    async with worker:
        task = await foobar.enqueue().start(after="nonexistent")
        worker.loop.create_task(worker.run_async())
        result = await task.result(3)
        assert result.success


async def test_task_dependency(worker: Worker):
    @worker.task()
    async def foobar(ctx: WrappedContext[None]) -> None:
        await asyncio.sleep(1)

    async with worker:
        task = await foobar.enqueue().start(delay=1)
        task2 = await foobar.enqueue().start(after=task.id)
        worker.loop.create_task(worker.run_async())
        assert await task2.status() == TaskStatus.PENDING
        await task.result(3)
        result = await task2.result(3)
        assert result.success


async def test_task_dependency_multiple(worker: Worker):
    @worker.task()
    async def foobar(ctx: WrappedContext[None]) -> None:
        await asyncio.sleep(1)

    async with worker:
        task = await foobar.enqueue().start()
        task2 = await foobar.enqueue().start(after=task.id)
        task3 = await foobar.enqueue().start(after=[task.id, task2.id])
        worker.loop.create_task(worker.run_async())
        assert await task2.status() == TaskStatus.PENDING
        assert await task3.status() == TaskStatus.PENDING
        res1 = await task.result(3)
        assert res1.success
        assert await task3.status() == TaskStatus.PENDING
        res2 = await task2.result(3)
        assert res2.success
        res3 = await task3.result(3)
        assert res3.success


async def test_task_dependency_failed(worker: Worker):
    @worker.task()
    async def foobar(ctx: WrappedContext[None]) -> None:
        raise Exception("Oh no!")

    @worker.task()
    async def do_nothing(ctx: WrappedContext[None]) -> None:
        pass

    async with worker:
        task = await foobar.enqueue().start()
        dep = await do_nothing.enqueue().start(after=task.id)
        worker.loop.create_task(worker.run_async())
        res = await dep.result(3)
        assert not res.success
        assert isinstance(res.result, StreaqError)


async def test_sync_task(worker: Worker):
    @worker.task()
    def foobar(ctx: WrappedContext[None]) -> None:
        time.sleep(2)

    async with worker:
        task = await foobar.enqueue()
        task2 = await foobar.enqueue()
        worker.loop.create_task(worker.run_async())
        # this would time out if these were running sequentially
        results = await asyncio.gather(task.result(3), task2.result(3))
        assert all(res.success for res in results)

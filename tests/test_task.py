import asyncio

import pytest

from streaq import StreaqError, WrappedContext, Worker
from streaq.task import StreaqRetry, TaskResult, TaskStatus


async def test_result_timeout(worker: Worker):
    @worker.task(ttl=0)
    async def foobar(ctx: WrappedContext[None]) -> bool:
        return False

    worker.burst = True
    async with worker:
        task = await foobar.enqueue()
        res = await asyncio.gather(
            task.result(5), worker.run_async(), return_exceptions=True
        )
        assert isinstance(res[0], StreaqError)


async def test_task_timeout(worker: Worker):
    @worker.task(timeout=1)
    async def foobar(ctx: WrappedContext[None]) -> None:
        await asyncio.sleep(5)

    worker.burst = True
    async with worker:
        task = await foobar.enqueue()
        res = await asyncio.gather(task.result(), worker.run_async())
        assert not res[0].success
        assert isinstance(res[0].result, asyncio.TimeoutError)


async def test_task_status(worker: Worker):
    @worker.task()
    async def foobar(ctx: WrappedContext[None]) -> None:
        await asyncio.sleep(3)

    worker.burst = True
    async with worker:
        task = foobar.enqueue()
        assert await task.status() == TaskStatus.PENDING
        await task.start()
        task2 = await foobar.enqueue().start(delay=5)
        assert await task.status() == TaskStatus.QUEUED
        await asyncio.gather(task.result(), worker.run_async())
        assert await task.status() == TaskStatus.DONE
        assert await task2.status() == TaskStatus.SCHEDULED


async def test_task_status_running(worker: Worker):
    @worker.task()
    async def foobar(ctx: WrappedContext[None]) -> None:
        await asyncio.sleep(3)

    worker.burst = True
    async with worker:
        task = await foobar.enqueue().start()

        async def get_status_after_delay() -> tuple[TaskStatus, bool]:
            await asyncio.sleep(1)
            status = await task.status()
            aborted = await task.abort()
            return status, aborted

        res = await asyncio.gather(get_status_after_delay(), worker.run_async())
        status, aborted = res[0]
        assert status == TaskStatus.RUNNING
        assert aborted


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
        done, _ = await asyncio.wait(
            [
                asyncio.ensure_future(task.result(3)),
                asyncio.ensure_future(worker.run_async()),
            ],
            return_when=asyncio.FIRST_COMPLETED,
        )
        for finished in done:
            with pytest.raises(StreaqError):
                finished.result()


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
        done, _ = await asyncio.wait(
            [
                asyncio.ensure_future(task.result(6)),
                asyncio.ensure_future(worker.run_async()),
            ],
            return_when=asyncio.FIRST_COMPLETED,
        )
        for finished in done:
            res = finished.result()
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

        async def get_result_twice() -> TaskResult:
            with pytest.raises(asyncio.TimeoutError):
                await task.result(3)
            return await task.result(1)

        done, _ = await asyncio.wait(
            [
                asyncio.ensure_future(get_result_twice()),
                asyncio.ensure_future(worker.run_async()),
            ],
            return_when=asyncio.FIRST_COMPLETED,
        )
        for finished in done:
            res = finished.result()
            assert res is not None
            assert res.success
            assert res.result == 2


async def test_task_failure(worker: Worker):
    @worker.task()
    async def foobar(ctx: WrappedContext[None]) -> None:
        raise Exception("That wasn't supposed to happen!")

    worker.burst = True
    async with worker:
        task = await foobar.enqueue()
        res = await asyncio.gather(task.result(), worker.run_async())
        assert not res[0].success
        assert isinstance(res[0].result, Exception)


async def test_task_retry_no_delay(worker: Worker):
    @worker.task()
    async def foobar(ctx: WrappedContext[None]) -> bool:
        if ctx.tries == 1:
            raise StreaqRetry("Retrying!", delay=0)
        return True

    async with worker:
        task = await foobar.enqueue()
        done, _ = await asyncio.wait(
            [
                asyncio.ensure_future(task.result(3)),
                asyncio.ensure_future(worker.run_async()),
            ],
            return_when=asyncio.FIRST_COMPLETED,
        )
        for finished in done:
            res = finished.result()
            assert res is not None
            assert res.success
            assert res.result


async def test_task_max_retries(worker: Worker):
    @worker.task()
    async def foobar(ctx: WrappedContext[None]) -> None:
        raise StreaqRetry("Retrying!", delay=0)

    async with worker:
        task = await foobar.enqueue()
        done, _ = await asyncio.wait(
            [
                asyncio.ensure_future(task.result(3)),
                asyncio.ensure_future(worker.run_async()),
            ],
            return_when=asyncio.FIRST_COMPLETED,
        )
        for finished in done:
            res = finished.result()
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

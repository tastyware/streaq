import asyncio
import time
from datetime import datetime, timedelta
from typing import Callable, Coroutine

import pytest

from streaq import StreaqError, WrappedContext, Worker
from streaq.task import StreaqRetry, TaskPriority, TaskStatus


async def test_result_timeout(worker: Worker):
    @worker.task(ttl=0)
    async def foobar(ctx: WrappedContext[None]) -> bool:
        return False

    task = await foobar.enqueue()
    worker.loop.create_task(worker.run_async())
    with pytest.raises(StreaqError):
        await task.result(3)


async def test_task_timeout(worker: Worker):
    @worker.task(timeout=1)
    async def foobar(ctx: WrappedContext[None]) -> None:
        await asyncio.sleep(5)

    task = await foobar.enqueue()
    worker.loop.create_task(worker.run_async())
    res = await task.result(3)
    assert not res.success
    assert isinstance(res.result, asyncio.TimeoutError)


async def test_task_status(worker: Worker):
    @worker.task()
    async def foobar(ctx: WrappedContext[None]) -> None:
        await asyncio.sleep(1)

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
    async def cron2(ctx: WrappedContext[None]) -> bool:
        return True

    schedule = cron1.schedule()
    assert schedule.day == 1 and schedule.month == 1
    await cron1.run()
    task = cron2.enqueue()  # by not awaiting we just get the task obj
    worker.loop.create_task(worker.run_async())
    res = await task.result(3)
    assert res.result and res.success


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

    task = await foobar.enqueue()
    worker.loop.create_task(worker.run_async())
    res = await task.result(7)
    assert res.success
    assert res.result == 3


async def test_task_retry_with_delay(worker: Worker):
    @worker.task()
    async def foobar(ctx: WrappedContext[None]) -> int:
        if ctx.tries == 1:
            raise StreaqRetry("Retrying!", delay=3)
        return ctx.tries

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

    task = await foobar.enqueue().start(after="nonexistent")
    worker.loop.create_task(worker.run_async())
    with pytest.raises(asyncio.TimeoutError):
        await task.result(3)


async def test_task_dependency(worker: Worker):
    @worker.task()
    async def foobar(ctx: WrappedContext[None]) -> None:
        await asyncio.sleep(1)

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

    task = await foobar.enqueue()
    task2 = await foobar.enqueue()
    worker.loop.create_task(worker.run_async())
    # this would time out if these were running sequentially
    results = await asyncio.gather(task.result(3), task2.result(3))
    assert all(res.success for res in results)


async def test_unsafe_enqueue(redis_url: str, worker: Worker):
    @worker.task()
    async def foobar(ctx: WrappedContext[None], ret: int) -> int:
        return ret

    worker.loop.create_task(worker.run_async())
    worker2 = Worker(redis_url=redis_url, queue_name="test")

    async with worker2:
        task = await worker2.enqueue_unsafe(foobar.fn_name, 42)
        res = await task.result(3)
        assert res.success
        assert res.result == 42


async def test_chained_failed_dependencies(worker: Worker):
    @worker.task()
    async def foobar(ctx: WrappedContext[None]) -> None:
        raise Exception("Oh no!")

    @worker.task()
    async def child(ctx: WrappedContext[None]) -> None:
        pass

    task = await foobar.enqueue().start(delay=3)
    dep1 = await child.enqueue().start(after=task.id)
    dep2 = await child.enqueue().start(after=[task.id, dep1.id])
    worker.loop.create_task(worker.run_async())
    await asyncio.sleep(1)
    assert await task.abort(3)
    res1 = await dep1.result(3)
    res2 = await dep2.result(3)
    assert not res1.success and isinstance(res1.result, StreaqError)
    assert not res2.success and isinstance(res2.result, StreaqError)


async def test_task_priorities(redis_url: str):
    worker = Worker(redis_url=redis_url, queue_name="test", concurrency=4)

    @worker.task()
    async def foobar(ctx: WrappedContext[None]) -> None:
        await asyncio.sleep(1)

    async with worker:
        low = [foobar.enqueue() for _ in range(4)]
        high = [foobar.enqueue() for _ in range(4)]
        await asyncio.gather(  # enqueue all tasks
            *[t.start(priority=TaskPriority.LOW) for t in low],
            *[t.start(priority=TaskPriority.HIGH) for t in high],
        )
        worker.loop.create_task(worker.run_async())
        results = await asyncio.gather(*[t.result(3) for t in high])
        statuses = await asyncio.gather(*[t.status() for t in low])
        assert all(res.success for res in results)
        assert all(status != TaskStatus.DONE for status in statuses)
    await worker.close()


async def test_scheduled_task(worker: Worker):
    @worker.task()
    async def foobar(ctx: WrappedContext[None]) -> None:
        pass

    dt = datetime.now() + timedelta(seconds=1)
    task = await foobar.enqueue().start(schedule=dt)
    worker.loop.create_task(worker.run_async())
    assert await task.status() == TaskStatus.SCHEDULED
    res = await task.result(3)
    assert res.success


async def test_bad_start_params(redis_url: str):
    worker = Worker(redis_url=redis_url)

    @worker.task()
    async def foobar(ctx: WrappedContext[None]) -> None:
        pass

    with pytest.raises(StreaqError):
        await foobar.enqueue().start(delay=1, schedule=datetime.now())
    with pytest.raises(StreaqError):
        await foobar.enqueue().start(delay=1, after="foobar")
    with pytest.raises(StreaqError):
        await foobar.enqueue().start(schedule=datetime.now(), after="foobar")


async def test_enqueue_unique_task(redis_url: str):
    worker = Worker(redis_url=redis_url)

    @worker.task(unique=True)
    async def foobar(ctx: WrappedContext[None]) -> None:
        pass

    async with worker:
        task = await foobar.enqueue()
        task2 = await foobar.enqueue()
        assert task.id == task2.id
        res = await asyncio.gather(task.info(), task2.info())
        assert res[0] == res[1]


async def test_failed_abort(worker: Worker):
    @worker.task(ttl=0)
    async def foobar(ctx: WrappedContext[None]) -> None:
        pass

    task = await foobar.enqueue().start()
    worker.loop.create_task(worker.run_async())
    await asyncio.sleep(1)
    assert not await task.abort(1)


async def test_cron_run(redis_url: str):
    worker = Worker(redis_url=redis_url)

    @worker.cron("* * * * * * *")
    async def cron1(ctx: WrappedContext[None]) -> bool:
        return True

    @worker.cron("* * * * * * *", timeout=1)
    async def cron2(ctx: WrappedContext[None]) -> None:
        await asyncio.sleep(3)

    async with worker:
        assert await cron1.run()
        with pytest.raises(asyncio.TimeoutError):
            await cron2.run()


async def test_sync_cron(worker: Worker):
    @worker.cron("* * * * * * *")
    def cronjob(ctx: WrappedContext[None]) -> None:
        time.sleep(1)

    worker.loop.create_task(worker.run_async())
    res = await cronjob.enqueue().result(3)
    assert res.success and res.result is None


async def test_cron_multiple_runs(worker: Worker):
    key = "runs"

    @worker.cron("* * * * * * *")
    async def cronjob(ctx: WrappedContext[None]) -> None:
        await ctx.redis.incr(key)

    worker.loop.create_task(worker.run_async())
    await asyncio.sleep(3)
    runs = await worker.redis.get(key)
    assert int(runs) > 1


async def test_middleware(worker: Worker):
    @worker.task()
    async def foobar(ctx: WrappedContext[None]) -> int:
        return 2

    @worker.middleware
    def double(ctx: WrappedContext[None], task: Callable[..., Coroutine]):
        async def wrapper(*args, **kwargs):
            result = await task(*args, **kwargs)
            return result * 2

        return wrapper

    worker.loop.create_task(worker.run_async())
    task = await foobar.enqueue()
    res = await task.result(3)
    assert res.success
    assert res.result == 4


async def test_task_pipeline(worker: Worker):
    @worker.task()
    async def double(ctx: WrappedContext[None], val: int) -> int:
        return val * 2

    @worker.task()
    async def is_even(ctx: WrappedContext[None], val: int) -> bool:
        return val % 2 == 0

    worker.loop.create_task(worker.run_async())
    async with worker:
        task = await double.enqueue(2).then(double).then(is_even)
        res = await task.result(3)
        assert res.result and res.success

import asyncio
import time
from datetime import datetime, timedelta
from typing import Any
from uuid import uuid4

import pytest
from anyio import create_task_group

from streaq import StreaqError, Worker
from streaq.constants import REDIS_UNIQUE
from streaq.task import StreaqRetry, TaskStatus
from streaq.types import ReturnCoroutine

pytestmark = pytest.mark.anyio


async def test_result_timeout(worker: Worker):
    @worker.task()
    async def foobar() -> None:
        await asyncio.sleep(5)

    task = await foobar.enqueue()
    async with create_task_group() as tg:
        tg.start_soon(worker.run_async)
        with pytest.raises(TimeoutError):
            await task.result(3)
        tg.cancel_scope.cancel()


async def test_run_local(worker: Worker):
    @worker.task(timeout=3)
    async def foobar() -> bool:
        return True

    assert await foobar.run()


async def test_task_timeout(worker: Worker):
    @worker.task(timeout=1)
    async def foobar() -> None:
        await asyncio.sleep(5)

    task = await foobar.enqueue()
    async with create_task_group() as tg:
        tg.start_soon(worker.run_async)
        res = await task.result(3)
        assert not res.success
        assert isinstance(res.result, TimeoutError)
        tg.cancel_scope.cancel()


async def test_task_status(worker: Worker):
    @worker.task()
    async def foobar() -> None:
        await asyncio.sleep(1)

    task = foobar.enqueue()
    assert await task.status() == TaskStatus.NOT_FOUND
    await task.start()
    assert await task.status() == TaskStatus.QUEUED
    task2 = await foobar.enqueue().start(delay=5)
    async with create_task_group() as tg:
        tg.start_soon(worker.run_async)
        await task.result(3)
        assert await task.status() == TaskStatus.DONE
        assert await task2.status() == TaskStatus.SCHEDULED
        tg.cancel_scope.cancel()


async def test_task_status_running(worker: Worker):
    @worker.task()
    async def foobar() -> None:
        await asyncio.sleep(3)

    task = await foobar.enqueue().start()
    async with create_task_group() as tg:
        tg.start_soon(worker.run_async)
        await asyncio.sleep(1)
        assert await task.status() == TaskStatus.RUNNING
        tg.cancel_scope.cancel()


async def test_task_cron(worker: Worker):
    @worker.cron("30 9 1 1 *")
    async def cron1() -> bool:
        return True

    @worker.cron("* * * * * * *")  # once/second
    async def cron2() -> None:
        await asyncio.sleep(3)

    schedule = cron1.schedule()
    assert schedule.day == 1 and schedule.month == 1
    assert await cron1.run()
    async with create_task_group() as tg:
        tg.start_soon(worker.run_async)
        await asyncio.sleep(2)
        # this will be set if task is running
        assert await worker.redis.get(worker.prefix + REDIS_UNIQUE + cron2.fn_name)
        tg.cancel_scope.cancel()


async def test_task_info(worker: Worker):
    @worker.task()
    async def foobar() -> None:
        pass

    task = await foobar.enqueue().start(delay=5)
    task2 = await foobar.enqueue()
    info = await task.info()
    info2 = await task2.info()
    assert info and info.scheduled is not None
    assert info2 and info2.scheduled is None


async def test_task_retry(worker: Worker):
    @worker.task()
    async def foobar() -> int:
        ctx = worker.task_context()
        if ctx.tries < 3:
            raise StreaqRetry("Retrying!")
        return ctx.tries

    task = await foobar.enqueue()
    async with create_task_group() as tg:
        tg.start_soon(worker.run_async)
        res = await task.result(7)
        assert res.success
        assert res.result == 3
        tg.cancel_scope.cancel()


async def test_task_retry_with_delay(worker: Worker):
    @worker.task()
    async def foobar() -> int:
        ctx = worker.task_context()
        if ctx.tries == 1:
            raise StreaqRetry("Retrying!", delay=timedelta(seconds=3))
        return ctx.tries

    task = await foobar.enqueue()
    async with create_task_group() as tg:
        tg.start_soon(worker.run_async)
        with pytest.raises(TimeoutError):
            await task.result(3)
        res = await task.result(1)
        assert res is not None
        assert res.success
        assert res.result == 2
        tg.cancel_scope.cancel()


async def test_task_retry_with_schedule(worker: Worker):
    @worker.task()
    async def foobar() -> int:
        ctx = worker.task_context()
        if ctx.tries == 1:
            raise StreaqRetry(
                "Retrying!", schedule=datetime.now() + timedelta(seconds=3)
            )
        return ctx.tries

    task = await foobar.enqueue()
    async with create_task_group() as tg:
        tg.start_soon(worker.run_async)
        with pytest.raises(TimeoutError):
            await task.result(3)
        res = await task.result(1)
        assert res is not None
        assert res.success
        assert res.result == 2
        tg.cancel_scope.cancel()


async def test_task_failure(worker: Worker):
    @worker.task()
    async def foobar() -> None:
        raise Exception("That wasn't supposed to happen!")

    task = await foobar.enqueue()
    async with create_task_group() as tg:
        tg.start_soon(worker.run_async)
        res = await task.result(3)
        assert not res.success
        assert isinstance(res.result, Exception)
        tg.cancel_scope.cancel()


async def test_task_retry_no_delay(worker: Worker):
    @worker.task()
    async def foobar() -> bool:
        if worker.task_context().tries == 1:
            raise StreaqRetry("Retrying!", delay=0)
        return True

    task = await foobar.enqueue()
    async with create_task_group() as tg:
        tg.start_soon(worker.run_async)
        res = await task.result(3)
        assert res is not None
        assert res.success
        assert res.result
        tg.cancel_scope.cancel()


async def test_task_max_retries(worker: Worker):
    @worker.task()
    async def foobar() -> None:
        raise StreaqRetry("Retrying!", delay=0)

    task = await foobar.enqueue()
    async with create_task_group() as tg:
        tg.start_soon(worker.run_async)
        res = await task.result(3)
        assert res is not None
        assert not res.success
        assert isinstance(res.result, StreaqError)
        tg.cancel_scope.cancel()


async def test_task_failed_abort(worker: Worker):
    @worker.task()
    async def foobar() -> bool:
        return True

    worker.burst = True
    task = await foobar.enqueue()
    await worker.run_async()
    result = await task.result(3)
    assert result.success
    assert result.result
    assert not await task.abort()


async def test_task_nonexistent_or_finished_dependency(worker: Worker):
    @worker.task()
    async def foobar() -> None:
        pass

    task = await foobar.enqueue().start(after="nonexistent")
    async with create_task_group() as tg:
        tg.start_soon(worker.run_async)
        with pytest.raises(TimeoutError):
            await task.result(3)
        tg.cancel_scope.cancel()


async def test_task_dependency(worker: Worker):
    @worker.task()
    async def foobar() -> None:
        await asyncio.sleep(1)

    task = await foobar.enqueue().start(delay=1)
    task2 = await foobar.enqueue().start(after=task.id)
    async with create_task_group() as tg:
        tg.start_soon(worker.run_async)
        assert await task2.status() == TaskStatus.SCHEDULED
        await task.result(3)
        result = await task2.result(3)
        assert result.success
        tg.cancel_scope.cancel()


async def test_task_dependency_multiple(worker: Worker):
    @worker.task()
    async def foobar() -> None:
        await asyncio.sleep(1)

    task = await foobar.enqueue().start()
    task2 = await foobar.enqueue().start(after=task.id)
    task3 = await foobar.enqueue().start(after=[task.id, task2.id])
    async with create_task_group() as tg:
        tg.start_soon(worker.run_async)
        assert await task2.status() == TaskStatus.SCHEDULED
        assert await task3.status() == TaskStatus.SCHEDULED
        res1 = await task.result(3)
        assert res1.success
        assert await task3.status() == TaskStatus.SCHEDULED
        res2 = await task2.result(3)
        assert res2.success
        res3 = await task3.result(3)
        assert res3.success
        tg.cancel_scope.cancel()


async def test_task_dependency_failed(worker: Worker):
    @worker.task()
    async def foobar() -> None:
        raise Exception("Oh no!")

    @worker.task()
    async def do_nothing() -> None:
        pass

    task = await foobar.enqueue().start()
    dep = await do_nothing.enqueue().start(after=task.id)
    async with create_task_group() as tg:
        tg.start_soon(worker.run_async)
        res = await dep.result(3)
        assert not res.success
        assert isinstance(res.result, StreaqError)
        tg.cancel_scope.cancel()


async def test_sync_task(worker: Worker):
    @worker.task()
    def foobar() -> None:
        time.sleep(2)

    task = await foobar.enqueue()
    task2 = await foobar.enqueue()
    async with create_task_group() as tg:
        tg.start_soon(worker.run_async)
        # this would time out if these were running sequentially
        results = await asyncio.gather(task.result(3), task2.result(3))
        assert all(res.success for res in results)
        tg.cancel_scope.cancel()


async def test_unsafe_enqueue(worker: Worker):
    @worker.task()
    async def foobar(ret: int) -> int:
        return ret

    task = await worker.enqueue_unsafe(foobar.fn_name, 42)
    async with create_task_group() as tg:
        tg.start_soon(worker.run_async)
        res = await task.result(3)
        assert res.success
        assert res.result == 42
        tg.cancel_scope.cancel()


async def test_chained_failed_dependencies(worker: Worker):
    @worker.task()
    async def foobar() -> None:
        raise Exception("Oh no!")

    @worker.task()
    async def child() -> None:
        pass

    task = await foobar.enqueue().start(delay=timedelta(seconds=3))
    dep1 = await child.enqueue().start(after=task.id)
    dep2 = await child.enqueue().start(after=[task.id, dep1.id])
    async with create_task_group() as tg:
        tg.start_soon(worker.run_async)
        await asyncio.sleep(1)
        assert await task.abort(3)
        res1 = await dep1.result(3)
        res2 = await dep2.result(3)
        assert not res1.success and isinstance(res1.result, StreaqError)
        assert not res2.success and isinstance(res2.result, StreaqError)
        tg.cancel_scope.cancel()


async def test_task_priorities(redis_url: str):
    worker = Worker(
        redis_url=redis_url,
        queue_name=uuid4().hex,
        concurrency=4,
        priorities=["low", "high"],
    )

    @worker.task()
    async def foobar() -> None:
        await asyncio.sleep(1)

    low = [foobar.enqueue().start(priority="low") for _ in range(4)]
    high = [foobar.enqueue().start(priority="high") for _ in range(4)]
    await worker.enqueue_many(low + high)
    async with create_task_group() as tg:
        tg.start_soon(worker.run_async)
        results = await asyncio.gather(*[t.result(3) for t in high])
        statuses = await asyncio.gather(*[t.status() for t in low])
        assert all(res.success for res in results)
        assert all(status != TaskStatus.DONE for status in statuses)
        tg.cancel_scope.cancel()


async def test_scheduled_task(worker: Worker):
    @worker.task()
    async def foobar() -> None:
        pass

    dt = datetime.now() + timedelta(seconds=1)
    task = await foobar.enqueue().start(schedule=dt)
    async with create_task_group() as tg:
        tg.start_soon(worker.run_async)
        assert await task.status() == TaskStatus.SCHEDULED
        res = await task.result(3)
        assert res.success
        tg.cancel_scope.cancel()


async def test_bad_start_params(worker: Worker):
    @worker.task()
    async def foobar() -> None:
        pass

    with pytest.raises(StreaqError):
        await foobar.enqueue().start(delay=1, schedule=datetime.now())
    with pytest.raises(StreaqError):
        await foobar.enqueue().start(delay=1, after="foobar")
    with pytest.raises(StreaqError):
        await foobar.enqueue().start(schedule=datetime.now(), after="foobar")


async def test_enqueue_unique_task(worker: Worker):
    @worker.task(unique=True)
    async def foobar() -> None:
        await asyncio.sleep(1)

    task = await foobar.enqueue()
    task2 = await foobar.enqueue()
    async with create_task_group() as tg:
        tg.start_soon(worker.run_async)
        results = await asyncio.gather(task.result(), task2.result())
        assert any(isinstance(r.result, StreaqError) for r in results)
        assert any(r.result is None for r in results)
        tg.cancel_scope.cancel()


async def test_failed_abort(worker: Worker):
    @worker.task(ttl=0)
    async def foobar() -> None:
        pass

    task = await foobar.enqueue().start()
    async with create_task_group() as tg:
        tg.start_soon(worker.run_async)
        await asyncio.sleep(1)
        assert not await task.abort(1)
        tg.cancel_scope.cancel()


async def test_cron_run(worker: Worker):
    @worker.cron("* * * * * * *")
    async def cron1() -> bool:
        return True

    @worker.cron("* * * * * * *", timeout=1)
    async def cron2() -> None:
        await asyncio.sleep(3)

    assert await cron1.run()
    with pytest.raises(TimeoutError):
        await cron2.run()


async def test_sync_cron(worker: Worker):
    @worker.cron("* * * * * * *")
    def cronjob() -> None:
        time.sleep(3)

    async with create_task_group() as tg:
        tg.start_soon(worker.run_async)
        await asyncio.sleep(2)
        assert await worker.redis.get(worker.prefix + REDIS_UNIQUE + cronjob.fn_name)
        tg.cancel_scope.cancel()


async def test_cron_multiple_runs(worker: Worker):
    val = 0

    @worker.cron("* * * * * * *")
    async def cronjob() -> None:
        nonlocal val
        val += 1

    async with create_task_group() as tg:
        tg.start_soon(worker.run_async)
        await asyncio.sleep(5)
        assert val > 1
        tg.cancel_scope.cancel()


async def test_middleware(worker: Worker):
    @worker.task()
    async def foobar() -> int:
        return 2

    @worker.middleware
    def double(task: ReturnCoroutine) -> ReturnCoroutine:
        async def wrapper(*args, **kwargs) -> Any:
            result = await task(*args, **kwargs)
            return result * 2

        return wrapper

    task = await foobar.enqueue()
    async with create_task_group() as tg:
        tg.start_soon(worker.run_async)
        res = await task.result(3)
        assert res.success
        assert res.result == 4
        tg.cancel_scope.cancel()


async def test_task_pipeline(worker: Worker):
    @worker.task()
    async def double(val: int) -> int:
        return val * 2

    @worker.task()
    async def is_even(val: int) -> bool:
        return val % 2 == 0

    task = await double.enqueue(1).then(double).then(is_even)
    async with create_task_group() as tg:
        tg.start_soon(worker.run_async)
        res = await task.result(3)
        assert res.result and res.success
        tg.cancel_scope.cancel()


async def test_task_with_custom_name(worker: Worker):
    @worker.task(name="bar")
    async def foo() -> int:
        return 42

    async def foobar():
        return

    assert foo.fn_name == "bar"
    with pytest.raises(StreaqError):
        worker.task(name="bar")(foobar)

    @worker.task()
    async def bar():
        return 10

    task1 = await worker.enqueue_unsafe("bar")
    task2 = await worker.enqueue_unsafe(bar.fn_name)
    task3 = await worker.enqueue_unsafe(foo.fn.__qualname__)
    async with create_task_group() as tg:
        tg.start_soon(worker.run_async)
        res = await task1.result(3)
        assert res.result == 42
        res = await task2.result(3)
        assert res.result == 10
        res = await task3.result(3)
        assert not res.success
        tg.cancel_scope.cancel()


async def test_cron_with_custom_name(worker: Worker):
    @worker.cron("* * * * * * *", name="foo")
    async def cronjob() -> None:
        await asyncio.sleep(3)

    async def cronjob1() -> None:
        pass

    assert cronjob.fn_name == "foo"
    with pytest.raises(StreaqError):
        worker.cron("* * * * * * *", name="foo")(cronjob1)

    async with create_task_group() as tg:
        tg.start_soon(worker.run_async)
        await asyncio.sleep(2)
        assert await worker.redis.get(worker.prefix + REDIS_UNIQUE + cronjob.fn_name)
        tg.cancel_scope.cancel()


@pytest.mark.parametrize("ttl", [60, 0])
@pytest.mark.parametrize("wait", [1, 0])
async def test_abort(worker: Worker, ttl: int, wait: int):
    @worker.task(ttl=ttl)
    async def foobar() -> None:
        await asyncio.sleep(5)

    task = await foobar.enqueue()
    async with create_task_group() as tg:
        tg.start_soon(worker.run_async)
        await asyncio.sleep(wait)
        assert await task.abort(3)
        tg.cancel_scope.cancel()


async def test_task_expired(worker: Worker):
    @worker.task(expire=1)
    async def foobar() -> None:
        pass

    task = await foobar.enqueue()
    await asyncio.sleep(1)
    async with create_task_group() as tg:
        tg.start_soon(worker.run_async)
        res = await task.result(3)
        assert not res.success and isinstance(res.result, StreaqError)
        tg.cancel_scope.cancel()

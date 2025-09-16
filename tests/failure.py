import sys

import pytest
from anyio import create_task_group, run, sleep

from streaq import Worker


@pytest.mark.anyio
async def test_reclaim_idle_task(redis_url: str, task_id: str):
    worker1 = Worker(redis_url=redis_url, queue_name="reclaim", idle_timeout=3)

    @worker1.task(name="foo")
    async def foo() -> None:
        await sleep(2)

    async with create_task_group() as tg:
        await tg.start(worker1.run_async)
        task = foo.enqueue()
        task.id = task_id
        await task


if __name__ == "__main__":
    redis_url = sys.argv[1]
    task_id = sys.argv[2]
    run(test_reclaim_idle_task, redis_url, task_id)

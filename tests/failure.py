import sys

import pytest
from anyio import run, sleep

from streaq import Worker


@pytest.mark.anyio
async def test_reclaim_idle_task(redis_url: str):
    worker1 = Worker(redis_url=redis_url, queue_name="reclaim", idle_timeout=3)

    @worker1.task()
    async def foo() -> None:
        await sleep(2)

    await worker1.run_async()


if __name__ == "__main__":
    redis_url = sys.argv[1]
    run(test_reclaim_idle_task, redis_url)

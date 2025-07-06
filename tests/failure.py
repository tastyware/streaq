import asyncio
import sys

from streaq import Worker


async def test_reclaim_idle_task(redis_url: str):
    worker1 = Worker(redis_url=redis_url, queue_name="reclaim")

    @worker1.task(timeout=3)
    async def foo() -> None:
        await asyncio.sleep(2)

    await worker1.run_async()


if __name__ == "__main__":
    redis_url = sys.argv[1]
    asyncio.run(test_reclaim_idle_task(redis_url))

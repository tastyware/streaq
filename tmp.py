import asyncio
from zoneinfo import ZoneInfo

from streaq import Worker

worker = Worker(concurrency=4, tz=ZoneInfo("America/Bogota"))


@worker.task()
async def sleeper(time: int) -> None:
    await asyncio.sleep(time)


async def main():
    async with worker:
        # importantly, we're not using `await` here
        tasks = [sleeper.enqueue(i) for i in range(10)]
        await worker.enqueue_many(tasks)


if __name__ == "__main__":
    asyncio.run(main())

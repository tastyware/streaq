import asyncio
from zoneinfo import ZoneInfo

from streaq import Worker

worker = Worker(concurrency=4, tz=ZoneInfo("America/Bogota"))


@worker.task()
async def sleeper(time: int) -> None:
    await asyncio.sleep(time)


async def main():
    async with worker:
        # await asyncio.gather(*[sleeper.enqueue(i).start(delay=i) for i in range(10)])
        for _ in range(8):
            await sleeper.enqueue(5)


if __name__ == "__main__":
    asyncio.run(main())

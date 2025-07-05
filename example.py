import asyncio

from streaq import Worker

worker = Worker(redis_url="redis://localhost:6379")


@worker.task()
async def sleeper(time: int) -> int:
    await asyncio.sleep(time)
    return time


@worker.cron("* * * * mon-fri")  # every minute on weekdays
async def cronjob() -> None:
    print("Nobody respects the spammish repetition!")


async def main() -> None:
    async with worker:
        await sleeper.enqueue(3)
        # enqueue returns a task object that can be used to get results/info
        task = await sleeper.enqueue(1).start(delay=3)
        print(await task.info())
        print(await task.result(timeout=5))


if __name__ == "__main__":
    asyncio.run(main())

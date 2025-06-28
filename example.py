import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import AsyncIterator

from httpx import AsyncClient

from streaq import Worker


@dataclass
class WorkerContext:
    """
    Type safe way of defining the dependencies of your tasks.
    e.g. HTTP client, database connection, settings.
    """

    http_client: AsyncClient


@asynccontextmanager
async def lifespan(worker: Worker[WorkerContext]) -> AsyncIterator[WorkerContext]:
    async with AsyncClient() as http_client:
        yield WorkerContext(http_client)


worker = Worker(redis_url="redis://localhost:6379", lifespan=lifespan)


@worker.task(timeout=5)
async def fetch(url: str) -> int:
    # worker.context here is of type WorkerContext, enforced by static typing
    res = await worker.context.http_client.get(url)
    return len(res.text)


@worker.cron("* * * * mon-fri")
async def cronjob() -> None:
    print("It's a bird... It's a plane... It's CRON!")


async def main() -> None:
    async with worker:
        await fetch.enqueue("https://tastyware.dev/")
        # this will be run directly locally, not enqueued
        await fetch.run("https://github.com/python-arq/arq")
        # enqueue returns a task object that can be used to get results/info
        task = await fetch.enqueue("https://github.com/tastyware/streaq").start(delay=3)
        print(await task.info())
        print(await task.result(timeout=5))


if __name__ == "__main__":
    asyncio.run(main())

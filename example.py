import asyncio
from dataclasses import dataclass
from typing import AsyncIterator
from contextlib import asynccontextmanager
from zoneinfo import ZoneInfo

from httpx import AsyncClient
from streaq.types import WrappedContext
from streaq.worker import Worker


@dataclass
class Context:
    """
    Type safe way of defining the dependencies of your tasks.

    E.g. HTTP client, database connection, settings. etc.
    """

    http_client: AsyncClient


@asynccontextmanager
async def worker_lifespan() -> AsyncIterator[Context]:
    async with AsyncClient() as http_client:
        yield Context(http_client)


worker = Worker(worker_lifespan=worker_lifespan, tz=ZoneInfo("US/Eastern"))


@worker.task()
async def foo(ctx: WrappedContext[Context], url: str) -> int:
    # ctx.deps here is of type MyWorkerDeps, that's enforced by static typing
    # FunctionContext will also provide access to a redis connection, retry count,
    # even results of other jobs etc.
    r = await ctx.deps.http_client.get(url)
    return len(r.text)


@worker.task(timeout=5)
async def sleeper(ctx: WrappedContext[Context], time: int) -> None:
    await asyncio.sleep(time)
    print("slept!")


@worker.cron("* * * * mon-fri")
async def cronjob(ctx: WrappedContext[Context]) -> None:
    print("imma cron yay")


async def main() -> None:
    async with worker:
        # run the task directly, never sending it to a queue
        # result = await foo.run("https://www.google.com/")
        # these two are equivalent, param spec means the arguments are type safe
        for url in [
            "https://microsoft.com/",
            "https://apple.com/",
            "https://amazon.com/",
            "https://google.com/",
            "https://paypal.com/",
            "https://tesla.com/",
            "https://youtube.com/",
            "https://heroku.com/",
            "https://adobe.com/",
            "https://airbnb.com/",
            "https://github.com/",
            "https://tastytrade.com/",
            "https://chatgpt.com/",
            "https://pypi.org/",
            "https://compassion.org/",
            "https://cru.org/",
            "https://linkedin.com/",
        ]:
            await foo.enqueue(url)
        task = await foo.enqueue("https://heroku.com/").start(delay=5)
        print(task)
        print(await task.result(timeout=10))

        # test error handling
        task2 = await sleeper.enqueue(7)
        print(await task2.result())


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())

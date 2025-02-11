from dataclasses import dataclass
from typing import AsyncIterator
from contextlib import asynccontextmanager

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


@asynccontextmanager
async def job_lifespan(ctx: WrappedContext[Context]) -> AsyncIterator[None]:
    print(f"Starting job {ctx.task_id} in worker {ctx.worker_id}")
    yield
    print("Finished job")


worker = Worker(lifespan=worker_lifespan)


@worker.task(lifespan=job_lifespan, timeout=10)
async def foo(ctx: WrappedContext[Context], url: str) -> int:
    # ctx.deps here is of type MyWorkerDeps, that's enforced by static typing
    # FunctionContext will also provide access to a redis connection, retry count,
    # even results of other jobs etc.
    r = await ctx.deps.http_client.get(url)
    print(f"{url}: {r.text[:40]!r}...")
    return len(r.text)


@worker.task(lifespan=job_lifespan, unique=True)
async def unq(ctx: WrappedContext[Context]) -> None:
    await asyncio.sleep(1)


async def main() -> None:
    async with worker:
        task = await unq.enqueue()
        print(task)
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
        # task = await foo.enqueue("https://apple.com/").start()
        # await foo.enqueue("https://amazon.com/").start(delay=5)


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())

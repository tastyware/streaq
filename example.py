from dataclasses import dataclass
from typing import AsyncIterator
from contextlib import asynccontextmanager

from httpx import AsyncClient
from streaq.worker import WorkerContext, Worker


@dataclass
class Context:
    """
    Type safe way of defining the dependencies of the worker functions.

    E.g. HTTP client, database connection, settings. etc.
    """

    http_client: AsyncClient


@asynccontextmanager
async def worker_lifespan() -> AsyncIterator[Context]:
    async with AsyncClient() as http_client:
        yield Context(http_client)


@asynccontextmanager
async def job_lifespan(ctx: WorkerContext[Context]) -> AsyncIterator[None]:
    print(f"Starting job in worker {ctx.id}")
    yield
    print("Finished job")


worker = Worker(lifespan=worker_lifespan)


@worker.task(lifespan=job_lifespan, timeout=10)
async def foo(ctx: WorkerContext[Context], url: str) -> int:
    # ctx.deps here is of type MyWorkerDeps, that's enforced by static typing
    # FunctionContext will also provide access to a redis connection, retry count,
    # even results of other jobs etc.
    r = await ctx.deps.http_client.get(url)
    print(f"{url}: {r.text[:40]!r}...")
    return len(r.text)


async def main() -> None:
    async with worker:
        # run the task directly, never sending it to a queue
        result = await foo.run("https://www.google.com/")
        print(result)
        # these two are equivalent, param spec means the arguments are type safe
        await foo.enqueue(url="https://google.com/")
        await foo.enqueue(url="https://google.com/")


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())

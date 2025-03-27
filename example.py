import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import AsyncIterator, Callable, Coroutine

from httpx import AsyncClient
from streaq import Worker, WrappedContext
from streaq.utils import now_ms


@dataclass
class Context:
    """
    Type safe way of defining the dependencies of your tasks.
    e.g. HTTP client, database connection, settings.
    """

    http_client: AsyncClient


@asynccontextmanager
async def worker_lifespan(worker: Worker) -> AsyncIterator[Context]:
    async with AsyncClient() as http_client:
        yield Context(http_client)


worker = Worker(redis_url="redis://localhost:6379", worker_lifespan=worker_lifespan)


@worker.task(timeout=5)
async def fetch(ctx: WrappedContext[Context], url: str) -> str:
    # ctx.deps here is of type Context, enforced by static typing
    # ctx also provides access the Redis connection, retry count, etc.
    r = await ctx.deps.http_client.get(url)
    return f"{r.text:.32}"


@worker.task()
async def double(ctx: WrappedContext[Context], val: str) -> int:
    return len(val) * 2


@worker.task()
async def mul(ctx: WrappedContext[Context], val: int) -> list[int]:
    return [val * i for i in range(-2, 2)]


@worker.task()
async def add(ctx: WrappedContext[Context], val: int) -> int:
    return val + 1


@worker.task()
async def pos(ctx: WrappedContext[Context], val: int) -> bool:
    return val > 0


@worker.cron("* * * * mon-fri")
async def cronjob(ctx: WrappedContext[Context]) -> None:
    print("It's a bird... It's a plane... It's CRON!")


@worker.middleware
def timer(
    ctx: WrappedContext[Context], fn: Callable[..., Coroutine]
) -> Callable[..., Coroutine]:
    async def wrapper(*args, **kwargs):
        start_time = now_ms()
        result = await fn(*args, **kwargs)
        print(f"Executed function {ctx.task_id} in {now_ms() - start_time}ms")
        return result

    return wrapper


async def main():
    async with worker:
        await fetch.enqueue("https://tastyware.dev/")
        # this will be run directly locally, not enqueued
        await fetch.run("https://github.com/python-arq/arq")
        # enqueue returns a task object that can be used to get results/info
        task = await fetch.enqueue("https://github.com/tastyware/streaq").start(delay=3)
        print(await task.info())
        print(await task.result(timeout=5))

        """
        t4 = (
            await fetch.enqueue("https://github.com")
            .then(double)
            .then(mul)
            .map(add)
            .filter(pos)
        )
        """


if __name__ == "__main__":
    asyncio.run(main())

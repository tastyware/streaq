import asyncio
from typing import Awaitable

import typer

from streaq import Worker, WrappedContext


worker = Worker(concurrency=32)
N_TASKS = 20_000
sem = asyncio.Semaphore(32)


# control the number of simultaneous connections to Redis
async def sem_task(task: Awaitable):
    async with sem:
        return await task


@worker.task()
async def sleeper(ctx: WrappedContext[None], time: int) -> None:
    if time:
        await asyncio.sleep(time)


async def main(time: int):
    async with worker:
        await asyncio.gather(
            *[
                asyncio.create_task(sem_task(sleeper.enqueue(time).start()))
                for _ in range(N_TASKS)
            ]
        )


def run(time: int = 0):
    loop = asyncio.get_event_loop()
    start = loop.time()
    loop.run_until_complete(main(time))
    end = loop.time()
    print(f"enqueued {N_TASKS} tasks in {end - start:.2f}s")


if __name__ == "__main__":
    typer.run(run)

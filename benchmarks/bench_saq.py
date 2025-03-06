import asyncio
from typing import Awaitable

import typer

from saq import Queue
from streaq.utils import now_ms


N_TASKS = 20_000
sem = asyncio.Semaphore(32)
queue = Queue.from_url("redis://localhost:6379")


# control the number of simultaneous connections to Redis
async def sem_task(task: Awaitable):
    async with sem:
        return await task


async def sleeper(ctx, time: int) -> None:
    if time:
        await asyncio.sleep(time)


async def startup(ctx):
    ctx["start_time"] = now_ms()


async def shutdown(ctx):
    run_time = now_ms() - ctx["start_time"]
    print(f"finished in {run_time}ms")


settings = {
    "functions": [sleeper],
    "queue": queue,
    "concurrency": 32,
    "burst": True,
    "startup": startup,
    "shutdown": shutdown,
    "dequeue_timeout": 1,
}


async def main(time: int):
    await asyncio.gather(
        *[
            asyncio.create_task(sem_task(queue.enqueue("sleeper", time=time)))
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

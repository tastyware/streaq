import asyncio
import time as pytime
from typing import Awaitable

import typer
from arq import create_pool
from arq.connections import RedisSettings

N_TASKS = 20_000
sem = asyncio.Semaphore(32)
settings = RedisSettings()


# control the number of simultaneous connections to Redis
async def sem_task(task: Awaitable):
    async with sem:
        return await task


async def sleeper(ctx, time: int) -> None:
    if time:
        await asyncio.sleep(time)


class WorkerSettings:
    functions = [sleeper]
    redis_settings = settings
    max_jobs = 32
    burst = True


async def main(time: int):
    queue = await create_pool()
    await asyncio.gather(
        *[
            asyncio.create_task(sem_task(queue.enqueue_job("sleeper", time)))
            for _ in range(N_TASKS)
        ]
    )


def run(time: int = 0):
    loop = asyncio.get_event_loop()
    start = pytime.perf_counter()
    loop.run_until_complete(main(time))
    end = pytime.perf_counter()
    print(f"enqueued {N_TASKS} tasks in {end - start:.2f}s")


if __name__ == "__main__":
    typer.run(run)

import asyncio
import time as pytime

import typer

from streaq import Worker

worker = Worker(concurrency=32)
N_TASKS = 20_000


@worker.task()
async def sleeper(time: int) -> None:
    if time:
        await asyncio.sleep(time)


async def main(time: int):
    tasks = [sleeper.enqueue(time) for _ in range(N_TASKS)]
    await worker.enqueue_many(tasks)


def run(time: int = 0):
    loop = asyncio.get_event_loop()
    start = pytime.perf_counter()
    loop.run_until_complete(main(time))
    end = pytime.perf_counter()
    print(f"enqueued {N_TASKS} tasks in {end - start:.2f}s")


if __name__ == "__main__":
    typer.run(run)

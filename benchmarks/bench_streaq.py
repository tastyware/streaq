import anyio
import typer

from streaq import Worker

worker = Worker(concurrency=32)
N_TASKS = 20_000


@worker.task()
async def sleeper(time: int) -> None:
    if time:
        await anyio.sleep(time)


async def main(time: int):
    start = anyio.current_time()
    tasks = [sleeper.enqueue(time) for _ in range(N_TASKS)]
    async with worker:
        await worker.enqueue_many(tasks)
    end = anyio.current_time()
    print(f"enqueued {N_TASKS} tasks in {end - start:.2f}s")


def run(time: int = 0):
    anyio.run(main, time)


if __name__ == "__main__":
    typer.run(run)

import anyio
import typer

from streaq import Worker

worker = Worker(concurrency=32, anyio_kwargs={"use_uvloop": True})
N_TASKS = 20_000


@worker.task()
async def sleeper(time: int) -> None:
    if time:
        await anyio.sleep(time)


async def main(time: int):
    start = anyio.current_time()
    async with worker:
        tasks = [sleeper.enqueue(time) for _ in range(N_TASKS)]
        await worker.enqueue_many(tasks)
    end = anyio.current_time()
    print(f"enqueued {N_TASKS} tasks in {end - start:.2f}s")


def run(time: int = 0):
    anyio.run(main, time)


if __name__ == "__main__":
    typer.run(run)

import asyncio
import os
import sys
from multiprocessing import Process
from typing import Annotated, cast

from typer import Exit, Option, Typer

from streaq import VERSION
from streaq.utils import import_string
from streaq.worker import Worker

cli = Typer(context_settings={"help_option_names": ["-h", "--help"]})


def version_callback(value: bool):
    if value:
        print(f"asynQ v{VERSION}")
        raise Exit()


@cli.command()
def main(
    worker_path: str,
    workers: Annotated[
        int, Option("--workers", "-w", help="Number of worker processes to spin up")
    ] = 1,
    watch: Annotated[
        bool, Option(help="Whether to auto-reload workers upon changes detected")
    ] = False,
    version: Annotated[
        bool | None,
        Option(
            "--version", "-v", callback=version_callback, help="Show installed version"
        ),
    ] = None,
):
    sys.path.append(os.getcwd())
    worker = cast(Worker, import_string(worker_path))
    coroutine = worker.start()
    if workers > 1:
        for _ in range(workers - 1):
            Process(target=asyncio.run, args=(coroutine,)).start()
    asyncio.run(coroutine)


if __name__ == "__main__":
    cli()

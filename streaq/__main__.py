import logging.config
import os
import sys
from multiprocessing import Process
from typing import Annotated, cast

from typer import Exit, Option, Typer

from streaq import VERSION
from streaq.utils import default_log_config, import_string
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
    burst: Annotated[
        bool,
        Option(help="Whether to close workers when no tasks are left in the queue"),
    ] = False,
    watch: Annotated[
        bool, Option(help="Whether to auto-reload workers upon changes detected")
    ] = False,
    verbose: Annotated[
        bool, Option(help="Whether to use logging.DEBUG instead of logging.INFO")
    ] = False,
    version: Annotated[
        bool | None,
        Option(
            "--version", "-v", callback=version_callback, help="Show installed version"
        ),
    ] = None,
):
    logging.config.dictConfig(default_log_config(False))
    sys.path.append(os.getcwd())
    worker = cast(Worker, import_string(worker_path))
    if workers > 1:
        for _ in range(workers - 1):
            Process(target=worker.run_sync).start()
    worker.run_sync()


if __name__ == "__main__":
    cli()

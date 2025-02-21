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
        print(f"streaQ v{VERSION}")
        raise Exit()


@cli.command()
def main(
    worker_path: str,
    workers: Annotated[
        int, Option("--workers", "-w", help="Number of worker processes to spin up")
    ] = 1,
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
    logging.config.dictConfig(default_log_config(verbose))
    sys.path.append(os.getcwd())

    if workers > 1:
        for _ in range(workers - 1):
            Process(target=run_worker, args=(worker_path,)).start()
    run_worker(worker_path)


def run_worker(path: str):
    worker = cast(Worker, import_string(path))
    try:
        worker.run_sync()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    cli()

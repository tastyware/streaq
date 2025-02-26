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
        int, Option("--workers", help="Number of worker processes to spin up")
    ] = 1,
    burst: Annotated[
        bool,
        Option(
            "--burst", "-b", help="Whether to shut down worker when the queue is empty"
        ),
    ] = False,
    verbose: Annotated[
        bool,
        Option(
            "--verbose",
            "-v",
            help="Whether to use logging.DEBUG instead of logging.INFO",
        ),
    ] = False,
    version: Annotated[
        bool,
        Option("--version", callback=version_callback, help="Show installed version"),
    ] = False,
    watch: Annotated[
        bool,
        Option(
            "--watch", "-w", help="Whether to reload the worker upon changes detected"
        ),
    ] = False,
):
    logging.config.dictConfig(default_log_config(verbose))
    sys.path.append(os.getcwd())

    if workers == 1:
        run_worker(worker_path, burst)
    else:
        processes = [
            Process(target=run_worker, args=(worker_path, burst))
            for _ in range(workers)
        ]
        for p in processes:
            p.start()
        for p in processes:
            p.join()


def run_worker(path: str, burst: bool):
    worker = cast(Worker, import_string(path))
    worker.burst = burst
    try:
        worker.run_sync()
    except KeyboardInterrupt:
        pass

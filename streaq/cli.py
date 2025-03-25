import logging.config
import os
import sys
from multiprocessing import Process
from typing import Annotated, cast

from typer import Exit, Option, Typer
from watchfiles import run_process

from streaq import VERSION, logger
from streaq.utils import default_log_config, import_string
from streaq.worker import Worker

cli = Typer()


def version_callback(value: bool) -> None:
    if value:
        print(f"streaQ v{VERSION}")
        raise Exit()


@cli.command()
def main(
    worker_path: str,
    workers: Annotated[
        int, Option("--workers", "-w", help="Number of worker processes to spin up")
    ] = 1,
    burst: Annotated[
        bool,
        Option(
            "--burst", "-b", help="Whether to shut down worker when the queue is empty"
        ),
    ] = False,
    reload: Annotated[
        bool,
        Option(
            "--reload", "-r", help="Whether to reload the worker upon changes detected"
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
):
    processes = []
    if workers > 1:
        processes.extend(
            [
                Process(
                    target=run_worker,
                    args=(worker_path, burst, reload, verbose, False),
                )
                for _ in range(workers - 1)
            ]
        )
    for p in processes:
        p.start()
    # only one runs a scheduler if with_scheduler is None
    run_worker(worker_path, burst, reload, verbose, True)
    for p in processes:
        p.join()


def run_worker(path: str, burst: bool, watch: bool, verbose: bool, schedule: bool):
    """
    Run a worker with the given options.
    """
    if watch:
        run_process(
            ".",
            target=_run_worker,
            args=(path, burst, verbose, schedule),
            callback=lambda _: logger.info("changes detected, reloading"),
        )
    else:
        _run_worker(path, burst, verbose, schedule)


def _run_worker(path: str, burst: bool, verbose: bool, schedule: bool):
    sys.path.append(os.getcwd())
    logging.config.dictConfig(default_log_config(verbose))
    worker = cast(Worker, import_string(path))
    worker.burst = burst
    if worker.with_scheduler is None:
        worker.with_scheduler = schedule
    worker.run_sync()

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
    host: Annotated[
        str,
        Option("--host", "-h", help="Host for web UI process"),
    ] = "127.0.0.1",
    port: Annotated[
        int,
        Option("--port", "-p", help="Port for web UI process"),
    ] = 8001,
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
    web: Annotated[
        bool, Option("--web", help="Whether to run web UI to monitor tasks")
    ] = False,
):
    processes = []
    if web:
        from streaq.web import run_app

        processes.append(Process(target=run_app, args=(host, port)))
    if workers > 1:
        processes.extend(
            [
                Process(target=run_worker, args=(worker_path, burst, reload, verbose))
                for _ in range(workers - 1)
            ]
        )
    for p in processes:
        p.start()
    run_worker(worker_path, burst, reload, verbose)
    for p in processes:
        p.join()


def run_worker(path: str, burst: bool, watch: bool, verbose: bool):
    """
    Run a worker with the given options.
    """
    sys.path.append(os.getcwd())
    if watch:
        run_process(
            ".",
            target=run_worker_watch,
            args=(path, burst, verbose),
            callback=lambda _: logger.info("changes detected, reloading"),
        )
    else:
        logging.config.dictConfig(default_log_config(verbose))
        worker = cast(Worker, import_string(path))
        worker.burst = burst
        try:
            worker.run_sync()
        except KeyboardInterrupt:
            pass


def run_worker_watch(path: str, burst: bool, verbose: bool):
    """
    Run a worker, reloading when changes are detected in the local directory.
    """
    logging.config.dictConfig(default_log_config(verbose))
    sys.path.append(os.getcwd())
    worker = cast(Worker, import_string(path))
    worker.burst = burst
    try:
        worker.run_sync()
    except KeyboardInterrupt:
        pass

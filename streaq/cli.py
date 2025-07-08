import logging.config
import os
import sys
from multiprocessing import Process
from typing import Annotated, Any, cast

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
    web: Annotated[
        bool,
        Option(
            "--web", help="Run a web UI for monitoring tasks in a separate process."
        ),
    ] = False,
    host: Annotated[
        str, Option("--host", "-h", help="Host for the web UI server.")
    ] = "0.0.0.0",
    port: Annotated[
        int, Option("--port", "-p", help="Port for the web UI server.")
    ] = 8000,
) -> None:
    processes: list[Process] = []
    if web:  # pragma: no cover
        from streaq.ui import run_web

        sys.path.append(os.getcwd())
        worker = cast(Worker[Any], import_string(worker_path))
        logging.config.dictConfig(default_log_config(worker.tz, verbose))
        processes.append(
            Process(
                target=run_web,
                args=(host, port, worker),
            )
        )
    if workers > 1:
        processes.extend(
            [
                Process(
                    target=run_worker,
                    args=(worker_path, burst, reload, verbose),
                )
                for _ in range(workers - 1)
            ]
        )
    for p in processes:
        p.start()
    run_worker(worker_path, burst, reload, verbose)
    for p in processes:
        p.join()


def run_worker(path: str, burst: bool, watch: bool, verbose: bool) -> None:
    """
    Run a worker with the given options.
    """
    if watch:
        run_process(
            ".",
            target=_run_worker,
            args=(path, burst, verbose),
            callback=lambda _: logger.info("changes detected, reloading"),
        )
    else:
        _run_worker(path, burst, verbose)


def _run_worker(path: str, burst: bool, verbose: bool) -> None:
    sys.path.append(os.getcwd())
    worker = cast(Worker[Any], import_string(path))
    logging.config.dictConfig(default_log_config(worker.tz, verbose))
    worker.burst = burst
    worker.run_sync()

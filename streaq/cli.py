import logging.config
import os
import sys
from multiprocessing import Process
from typing import Annotated, Any, cast

from typer import Exit, Option, Typer
from watchfiles import run_process

from streaq import VERSION
from streaq.types import StreaqError
from streaq.utils import default_log_config, import_string
from streaq.worker import Worker

cli = Typer(no_args_is_help=True, pretty_exceptions_show_locals=False)


@cli.callback(invoke_without_command=True)
def version_callback(
    version: Annotated[
        bool, Option("--version", help="Show installed version")
    ] = False,
) -> None:
    if version:
        print(f"streaQ v{VERSION}")
        raise Exit()


@cli.command(help="Run one or more workers with the given options")
def run(
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
) -> None:
    for _ in range(workers - 1):
        Process(
            target=run_worker,
            args=(worker_path, burst, reload, verbose),
        ).start()
    run_worker(worker_path, burst, reload, verbose)


@cli.command(help="Run a web UI for monitoring with the given options")
def web(
    worker_path: str,
    host: Annotated[
        str, Option("--host", "-h", help="Host for the web UI server.")
    ] = "0.0.0.0",
    port: Annotated[
        int, Option("--port", "-p", help="Port for the web UI server.")
    ] = 8000,
) -> None:
    try:
        from streaq.ui import run_web
    except ModuleNotFoundError as e:  # pragma: no cover
        raise StreaqError(
            "web module not installed, try `pip install streaq[web]`"
        ) from e
    run_web(host, port, worker_path)


def run_worker(path: str, burst: bool, watch: bool, verbose: bool) -> None:
    """
    Run a worker with the given options.
    """
    if watch:
        run_process(
            ".",
            target=_run_worker,
            args=(path, burst, verbose),
            callback=lambda _: print("changes detected, reloading..."),
        )
    else:
        _run_worker(path, burst, verbose)


def _run_worker(path: str, burst: bool, verbose: bool) -> None:
    sys.path.append(os.getcwd())
    worker = cast(Worker[Any], import_string(path))
    logging.config.dictConfig(default_log_config(worker.tz, verbose))
    worker.burst = burst
    worker.run_sync()

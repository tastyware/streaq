import os
import sys
from multiprocessing import Process
from typing import Annotated

from typer import Exit, Option, Typer

from streaq import VERSION
from streaq.utils import import_string

cli = Typer(context_settings={"help_option_names": ["-h", "--help"]})


def version_callback(value: bool):
    if value:
        print(f"asynQ v{VERSION}")
        raise Exit()


def dummy():
    print("Fake worker started!")


@cli.command()
def main(
    worker_model: str,
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
    worker = import_string(worker_model)
    print(worker)
    if workers > 1:
        for _ in range(workers - 1):
            Process(target=dummy, args=()).start()
    dummy()


if __name__ == "__main__":
    cli()

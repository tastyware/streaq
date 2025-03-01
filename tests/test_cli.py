import sys
from typing import AsyncGenerator

from pytest import fixture
from typer.testing import CliRunner

from streaq import VERSION, Worker
from streaq.cli import cli

runner = CliRunner()
test_module = sys.modules["tests.test_cli"]


@fixture(scope="function")
async def worker_no_cleanup(redis_url: str) -> AsyncGenerator[Worker, None]:
    yield Worker(redis_url=redis_url)


def test_burst(worker_no_cleanup: Worker):
    setattr(test_module, "test_worker", worker_no_cleanup)
    result = runner.invoke(cli, ["tests.test_cli.test_worker", "--burst"])
    assert result.exit_code == 0


def test_multiple_workers(worker_no_cleanup: Worker):
    setattr(test_module, "test_worker", worker_no_cleanup)
    result = runner.invoke(
        cli, ["--burst", "--workers", "2", "tests.test_cli.test_worker"]
    )
    assert result.exit_code == 0


def test_verbose(worker_no_cleanup: Worker):
    setattr(test_module, "test_worker", worker_no_cleanup)
    result = runner.invoke(cli, ["tests.test_cli.test_worker", "--burst", "--verbose"])
    assert result.exit_code == 0
    assert "enqueuing" in result.stdout


def test_version(worker_no_cleanup: Worker):
    setattr(test_module, "test_worker", worker_no_cleanup)
    result = runner.invoke(cli, ["--version"])
    assert result.exit_code == 0
    assert VERSION in result.stdout

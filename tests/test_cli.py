import subprocess
import sys
from typing import AsyncGenerator
from uuid import uuid4

from pytest import fixture
from typer.testing import CliRunner

from streaq import VERSION, Worker
from streaq.cli import cli

runner = CliRunner()
test_module = sys.modules["tests.test_cli"]


@fixture(scope="function")
async def worker_no_cleanup(redis_url: str) -> AsyncGenerator[Worker, None]:
    yield Worker(redis_url=redis_url, queue_name=uuid4().hex)


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
    assert "main loop" in result.stderr


def test_version(worker_no_cleanup: Worker):
    setattr(test_module, "test_worker", worker_no_cleanup)
    result = runner.invoke(cli, ["--version"])
    assert result.exit_code == 0
    assert VERSION in result.stdout


def test_help(worker_no_cleanup: Worker):
    setattr(test_module, "test_worker", worker_no_cleanup)
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    assert "--help" in result.stdout


def test_main_entry_point():
    result = subprocess.run(
        [sys.executable, "-m", "streaq", "--help"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0
    assert "--help" in result.stdout

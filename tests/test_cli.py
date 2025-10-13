import os
import subprocess
import sys
from contextlib import redirect_stderr
from multiprocessing import get_context
from pathlib import Path
from typing import Any

import pytest
from anyio import sleep
from httpx import AsyncClient, ConnectError
from typer.testing import CliRunner

from streaq import VERSION, Worker
from streaq.cli import cli

pytestmark = pytest.mark.anyio
runner = CliRunner()
test_module = sys.modules["tests.test_cli"]


def test_burst(worker: Worker):
    setattr(test_module, "test_worker", worker)
    result = runner.invoke(cli, ["tests.test_cli.test_worker", "--burst"])
    assert result.exit_code == 0


def test_multiple_workers(worker: Worker):
    setattr(test_module, "test_worker", worker)
    result = runner.invoke(
        cli, ["--burst", "--workers", "2", "tests.test_cli.test_worker"]
    )
    assert result.exit_code == 0


def test_verbose(worker: Worker):
    setattr(test_module, "test_worker", worker)
    result = runner.invoke(cli, ["tests.test_cli.test_worker", "--burst", "--verbose"])
    assert result.exit_code == 0
    assert "enqueuing" in result.stderr


def test_version(worker: Worker):
    setattr(test_module, "test_worker", worker)
    result = runner.invoke(cli, ["--version"])
    assert result.exit_code == 0
    assert VERSION in result.stdout


def test_help(worker: Worker):
    setattr(test_module, "test_worker", worker)
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


def _run_in_tmp(path: Path, *args: Any) -> None:
    os.chdir(path)
    log_file = path / "log.txt"
    with open(log_file, "w+", buffering=1) as f, redirect_stderr(f):
        runner.invoke(cli, args=args)


async def test_web_cli(redis_url: str, free_tcp_port: int, tmp_path: Path):
    file = tmp_path / "web.py"
    file.write_text(
        f"""from uuid import uuid4
from streaq import Worker
worker = Worker(redis_url="{redis_url}", queue_name=uuid4().hex)"""
    )
    ctx = get_context("spawn")
    p = ctx.Process(
        target=_run_in_tmp,
        args=(tmp_path, "web.worker", "--web", "--port", str(free_tcp_port)),
    )
    p.start()

    async with AsyncClient() as client:
        for _ in range(5):
            try:
                res = await client.get(f"http://localhost:{free_tcp_port}/")
                assert res.status_code == 303
                break
            except ConnectError:
                await sleep(1)
        else:
            pytest.fail("Web CLI never listened on port!")
    p.kill()


async def test_watch_subprocess(redis_url: str, tmp_path: Path):
    file = tmp_path / "watch.py"
    file.write_text(
        f"""from uuid import uuid4
from streaq import Worker
worker = Worker(redis_url="{redis_url}", queue_name=uuid4().hex)"""
    )

    p = subprocess.Popen(
        [sys.executable, "-m", "streaq", "watch.worker", "--reload"],
        cwd=str(tmp_path),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    await sleep(2)
    with open(file, "a") as f:  # make change
        f.write("  # change from test")
    await sleep(1)

    p.terminate()
    out, err = p.communicate(timeout=3)
    text = (out + err).lower()
    assert "reload" in text and text.count("starting") > 1

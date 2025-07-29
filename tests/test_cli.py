import subprocess
import sys

import pytest
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


"""
@pytest.fixture(scope="function")
def worker_file(redis_url: str, tmp_path: Path):
    tmp = NamedTemporaryFile(dir=tmp_path, suffix=".py", mode="w+", delete=False)
    tmp.write(f""from uuid import uuid4
from streaq import Worker
worker = Worker(redis_url="{redis_url}", queue_name=uuid4().hex)"")
    tmp.close()
    yield tmp.name
    os.remove(tmp.name)


async def test_watch(worker_file: str, tmp_path: Path):
    file_name = worker_file.split("/")[-1][:-3]

    def run_subprocess():
        with pytest.raises(subprocess.TimeoutExpired) as e:
            subprocess.run(
                [sys.executable, "-m", "streaq", f"{file_name}.worker", "--reload"],
                capture_output=True,
                text=True,
                check=False,
                timeout=3,
                cwd=tmp_path,
            )
        return e

    async def modify_file():
        await asyncio.sleep(1)  # wait for startup
        with open(worker_file, "a") as f:
            f.write("  # change from test")

    res, _ = await asyncio.gather(asyncio.to_thread(run_subprocess), modify_file())
    assert str(res.value.stderr).count("starting") > 1


def find_free_port() -> int:  # Finds and returns an available TCP port.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


async def test_web_cli(worker_file: str):
    file_name = worker_file.split("/")[-1][:-3]

    def run_subprocess():
        with pytest.raises(subprocess.TimeoutExpired) as e:
            subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "streaq",
                    f"{file_name}.worker",
                    "--web",
                    "--port",
                    str(find_free_port()),
                ],
                capture_output=True,
                text=True,
                check=False,
                timeout=3,
            )
        return e

    async def modify_file():
        await asyncio.sleep(1)  # wait for startup
        return httpx.get("http://localhost:8000/")

    web, res = await asyncio.gather(asyncio.to_thread(run_subprocess), modify_file())
    assert "Uvicorn" in str(web.value.stderr)
    assert res.status_code == 303
"""

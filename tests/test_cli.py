from typer.testing import CliRunner

from streaq import VERSION, Worker
from streaq.cli import cli

runner = CliRunner()
test_worker = Worker()


def test_burst():
    result = runner.invoke(cli, ["tests.test_cli.test_worker", "-b"])
    assert result.exit_code == 0


def test_multiple_workers():
    result = runner.invoke(cli, ["-b", "--workers", "2", "tests.test_cli.test_worker"])
    assert result.exit_code == 0


def test_verbose():
    result = runner.invoke(cli, ["tests.test_cli.test_worker", "-bv"])
    assert result.exit_code == 0
    assert "enqueuing" in result.stdout


def test_version():
    result = runner.invoke(cli, ["--version"])
    assert result.exit_code == 0
    assert VERSION in result.stdout

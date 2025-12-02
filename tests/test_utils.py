import pytest

from streaq.utils import gather, import_string

pytestmark = pytest.mark.anyio


def test_bad_path():
    with pytest.raises(ImportError):
        _ = import_string("asdf")


def test_bad_worker_name():
    with pytest.raises(ImportError):
        _ = import_string("example:asdf")


async def test_useless_gather():
    assert not (await gather())

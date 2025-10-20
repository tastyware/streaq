import pytest

from streaq.utils import import_string


def test_bad_path():
    with pytest.raises(ImportError):
        _ = import_string("asdf")


def test_bad_worker_name():
    with pytest.raises(ImportError):
        _ = import_string("example:asdf")

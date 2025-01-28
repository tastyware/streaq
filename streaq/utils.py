from importlib import import_module
from typing import Any


def import_string(dotted_path: str) -> Any:
    """
    Taken from pydantic.utils.
    """

    try:
        module_path, class_name = dotted_path.strip(" ").rsplit(".", 1)
    except ValueError as e:
        raise ImportError(f"'{dotted_path}' doesn't look like a module path") from e

    module = import_module(module_path)
    try:
        return getattr(module, class_name)
    except AttributeError as e:
        raise ImportError(
            f"Module '{module_path}' does not define a '{class_name}' attribute"
        ) from e

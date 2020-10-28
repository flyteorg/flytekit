from contextlib import contextmanager
from unittest.mock import MagicMock

from flytekit import logger
from flytekit.annotated.task import PythonTask


@contextmanager
def task_mock(t: PythonTask) -> MagicMock:
    m = MagicMock()

    def _log(*args, **kwargs):
        logger.warning(f"Invoking mock method for task: '{t.name}'")
        return m(*args, **kwargs)

    _captured_fn = t.execute
    t.execute = _log
    yield m
    t.execute = _captured_fn

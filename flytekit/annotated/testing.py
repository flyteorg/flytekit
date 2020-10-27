from unittest.mock import MagicMock

from flytekit import logger
from flytekit.annotated.task import PythonTask


def task_mock(t: PythonTask) -> MagicMock:
    m = MagicMock()

    def _log(*args, **kwargs):
        logger.warning(f"Invoking mock method for task: '{t.name}'")
        return m(*args, **kwargs)

    t.execute = _log
    return m

from contextlib import contextmanager
from unittest.mock import MagicMock

from flytekit import logger
from flytekit.annotated.base_task import PythonTask


@contextmanager
def task_mock(t: PythonTask) -> MagicMock:
    """
    Use this method to mock a task declaration. It can mock any Task in Flytekit as long as it has a python native
    interface associated with it.

    The returned object is a MagicMock and allows to perform all such methods. This MagicMock, mocks the execute method
    on the PythonTask

    Usage:
        .. code-block:: python
            @task
            def t1(i: int) -> int:
               pass

            with task_mock(t1) as m:
               m.side_effect = lambda x: x
               t1(10)
               # The mock is valid only within this context
    """
    m = MagicMock()

    def _log(*args, **kwargs):
        logger.warning(f"Invoking mock method for task: '{t.name}'")
        return m(*args, **kwargs)

    _captured_fn = t.execute
    t.execute = _log
    yield m
    t.execute = _captured_fn

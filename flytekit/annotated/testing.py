from contextlib import contextmanager
from typing import Union
from unittest.mock import MagicMock

from flytekit import logger
from flytekit.annotated.base_task import PythonTask
from flytekit.annotated.reference_entity import ReferenceEntity
from flytekit.annotated.workflow import Workflow


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

    if not isinstance(t, PythonTask) and not isinstance(t, Workflow) and not isinstance(t, ReferenceEntity):
        raise Exception("Can only be used for tasks")

    m = MagicMock()

    def _log(*args, **kwargs):
        logger.warning(f"Invoking mock method for task: '{t.name}'")
        return m(*args, **kwargs)

    _captured_fn = t.execute
    t.execute = _log
    yield m
    t.execute = _captured_fn


def patch(target: Union[PythonTask, Workflow, ReferenceEntity]):
    if (
        not isinstance(target, PythonTask)
        and not isinstance(target, Workflow)
        and not isinstance(target, ReferenceEntity)
    ):

        raise Exception("Can only use mocks on tasks/workflows declared in Python.")

    def wrapper(test_fn):
        def new_test(*args, **kwargs):
            logger.warning(f"Invoking mock method for target: '{target.name}'")
            m = MagicMock()
            saved = target.execute
            target.execute = m
            results = test_fn(m, *args, **kwargs)
            target.execute = saved
            return results

        return new_test

    return wrapper

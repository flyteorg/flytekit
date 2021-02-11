import functools
from typing import Callable, Optional

from flytekit import TaskMetadata
from flytekit.core import task
from flytekit.core.python_function_task import PythonFunctionTask
from flytekit.core.task import TaskPlugins


class _Dynamic(object):
    pass


class DynamicWorkflowTask(PythonFunctionTask[_Dynamic]):
    """
    Please use the dynamic decorator to create a dynamic task.
    TODO: Add usage for dynamic tasks
    """

    def __init__(
        self,
        task_config: _Dynamic,
        dynamic_workflow_function: Callable,
        metadata: Optional[TaskMetadata] = None,
        **kwargs,
    ):
        self._wf = None

        super().__init__(
            task_config=task_config,
            task_function=dynamic_workflow_function,
            metadata=metadata,
            task_type="dynamic-task",
            **kwargs,
        )


# Register dynamic workflow task
TaskPlugins.register_pythontask_plugin(_Dynamic, DynamicWorkflowTask)
dynamic = functools.partial(
    task.task, task_config=_Dynamic(), execution_mode=PythonFunctionTask.ExecutionBehavior.DYNAMIC
)
dynamic.__doc__ = """
Please first see the comments for :py:func:`flytekit.task` and :py:func:`flytekit.workflow`. This ``dynamic``
concept is an amalgamation of both and enables the user to pursue some :std:ref:`pretty incredible <cookbook:advanced_merge_sort>`
constructs.

In short, a task's function is run at execution time only, and a workflow function is run at compilation time only (local
execution notwithstanding). A dynamic workflow is modeled on the backend as a task, but at execution time, the function
body gets compiled like a workflow, as if the decorator changed from ``@task`` to ``@workflow``. This workflow is
compiled using the inputs to decorated function and the resulting workflow is passed back to the Flyte engine and is
run as a :std:doc:`subworkflow <auto_core_intermediate/subworkflows>`.  Simple usage

.. code-block::

    @dynamic
    def my_dynamic_subwf(a: int) -> (typing.List[str], int):
        s = []
        for i in range(a):
            s.append(t1(a=i))
        return s, 5
        
Note in the code block that we call the Python ``range`` operator on the input. This is typically not allowed in a
workflow but it is here. You can even express dependencies between tasks.

.. code-block::

    @dynamic
    def my_dynamic_subwf(a: int, b: int) -> int:
        x = t1(a=a)
        return t2(b=b, x=x)

See the :std:ref:`cookbook <cookbook:sphx_glr_auto_core_intermediate_subworkflows.py>` for a longer discussion.
"""

"""
Dynamic Workflows
-----------------------------
Dynamic workflows are one of the powerful aspects of Flyte. Please take a look at the :py:func:`flytekit.dynamic` documentation first to get started.


Declaring a dynamic workflow
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: flytekit.core.dynamic_workflow_task.DynamicWorkflowTask


Caveats when using a dynamic workflow
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Because of the dynamic nature of the workflow generated, it can easily be abused. Keep in mind that the workflow
that's compiled out of the decorated function needs to be processed like any other workflow. It's rare to see a
manually written workflow that has 5000 nodes for instance, but you can easily get there with a loop. Please keep,
dynamic workflows to under fifty tasks. For large-scale identical runs, we recommend the upcoming map task.

"""
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

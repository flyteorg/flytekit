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

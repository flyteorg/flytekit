import functools
from typing import Callable, Optional

from flytekit import TaskMetadata
from flytekit.annotated import task
from flytekit.annotated.python_function_task import PythonFunctionTask
from flytekit.annotated.task import TaskPlugins


class _Dynamic(object):
    pass


class DynamicWorkflowTask(PythonFunctionTask[_Dynamic]):
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

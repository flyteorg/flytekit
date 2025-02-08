"""
Slurm task.
"""

from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Union

from flytekit import FlyteContextManager, PythonFunctionTask
from flytekit.configuration import SerializationSettings
from flytekit.extend import TaskPlugins
from flytekit.extend.backend.base_agent import AsyncAgentExecutorMixin
from flytekit.image_spec import ImageSpec


@dataclass
class SlurmFunction(object):
    """Configure Slurm settings. Note that we focus on srun command now.

    Args:
        slurm_host: Slurm host name. We assume there's no default Slurm host now.
        srun_conf: Options of srun command.
        script: User-defined script where "{task.fn}" serves as a placeholder for the
            task function execution. Users should insert "{task.fn}" at the desired
            execution point within the script. If the script is not provided, the task
            function will be executed directly.
    """

    slurm_host: str
    srun_conf: Optional[Dict[str, str]] = None
    script: Optional[str] = None

    def __post_init__(self):
        if self.srun_conf is None:
            self.srun_conf = {}


class SlurmFunctionTask(AsyncAgentExecutorMixin, PythonFunctionTask[SlurmFunction]):
    """
    Actual Plugin that transforms the local python code for execution within a slurm context...
    """

    _TASK_TYPE = "slurm_fn"

    def __init__(
        self,
        task_config: SlurmFunction,
        task_function: Callable,
        container_image: Optional[Union[str, ImageSpec]] = None,
        **kwargs,
    ):
        super(SlurmFunctionTask, self).__init__(
            task_config=task_config,
            task_type=self._TASK_TYPE,
            task_function=task_function,
            container_image=container_image,
            **kwargs,
        )

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        return {
            "slurm_host": self.task_config.slurm_host,
            "srun_conf": self.task_config.srun_conf,
            "script": self.task_config.script,
        }

    def execute(self, **kwargs) -> Any:
        ctx = FlyteContextManager.current_context()
        if ctx.execution_state and ctx.execution_state.is_local_execution():
            # Mimic the propeller's behavior in local agent test
            return AsyncAgentExecutorMixin.execute(self, **kwargs)
        else:
            # Execute the task with a direct python function call
            return PythonFunctionTask.execute(self, **kwargs)


TaskPlugins.register_pythontask_plugin(SlurmFunction, SlurmFunctionTask)

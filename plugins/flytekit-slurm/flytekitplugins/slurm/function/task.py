from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from flytekit import FlyteContextManager, PythonFunctionTask
from flytekit.configuration import SerializationSettings
from flytekit.extend import TaskPlugins
from flytekit.extend.backend.base_connector import AsyncConnectorExecutorMixin
from flytekit.image_spec import ImageSpec


@dataclass
class SlurmFunctionConfig(object):
    """Configure Slurm settings. Note that we focus on sbatch command now.

    Args:
        ssh_config: Options of SSH client connection. For available options, please refer to
            <newly-added-ssh-utils-file>
        sbatch_config: Options of sbatch command. If not provided, defaults to an empty dict.
        script: User-defined script where "{task.fn}" serves as a placeholder for the
            task function execution. Users should insert "{task.fn}" at the desired
            execution point within the script. If the script is not provided, the task
            function will be executed directly.

    Attributes:
        ssh_config (Dict[str, Union[str, List[str], Tuple[str, ...]]]): SSH client configuration options.
        sbatch_config (Optional[Dict[str, str]]): Slurm sbatch command options.
        script (Optional[str]): Custom script template for task execution.
    """

    ssh_config: Dict[str, Union[str, List[str], Tuple[str, ...]]]
    sbatch_config: Optional[Dict[str, str]] = None
    script: Optional[str] = None

    def __post_init__(self):
        assert self.ssh_config["host"] is not None, "'host' must be specified in ssh_config."
        if self.sbatch_config is None:
            self.sbatch_config = {}


class SlurmFunctionTask(AsyncConnectorExecutorMixin, PythonFunctionTask[SlurmFunctionConfig]):
    """
    Actual Plugin that transforms the local python code for execution within a slurm context...
    """

    _TASK_TYPE = "slurm_fn"

    def __init__(
        self,
        task_config: SlurmFunctionConfig,
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
            "ssh_config": self.task_config.ssh_config,
            "sbatch_config": self.task_config.sbatch_config,
            "script": self.task_config.script,
        }

    def execute(self, **kwargs) -> Any:
        ctx = FlyteContextManager.current_context()
        if ctx.execution_state and ctx.execution_state.is_local_execution():
            # Mimic the propeller's behavior in local connector test
            return AsyncConnectorExecutorMixin.execute(self, **kwargs)
        else:
            # Execute the task with a direct python function call
            return PythonFunctionTask.execute(self, **kwargs)


TaskPlugins.register_pythontask_plugin(SlurmFunctionConfig, SlurmFunctionTask)

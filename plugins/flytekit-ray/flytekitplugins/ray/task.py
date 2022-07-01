from typing import Any, Callable

from ray.util.client import ray

from flytekit.core.context_manager import ExecutionParameters
from flytekit.core.python_function_task import PythonFunctionTask
from flytekit.extend import TaskPlugins


class RayConfig(object):
    def __init__(self, address: str = None, **kwargs):
        """
        :param str address: The address of the Ray cluster to connect to.
        :param dict kwargs: extra arguments for ray.init(). https://docs.ray.io/en/latest/ray-core/package-ref.html#ray-init
        """
        self.address = address
        self.extra_args = kwargs


class RayFunctionTask(PythonFunctionTask):
    """
    Actual Plugin that transforms the local python code for execution within Ray job
    """

    _RAY_TASK_TYPE = "ray"

    def __init__(self, task_config: RayConfig, task_function: Callable, **kwargs):
        if task_config is None:
            task_config = RayConfig()
        super().__init__(task_config=task_config, task_type=self._RAY_TASK_TYPE, task_function=task_function, **kwargs)
        self._task_config = task_config

    def pre_execute(self, user_params: ExecutionParameters) -> ExecutionParameters:
        ray.init(self._task_config.address, **self._task_config.extra_args)
        return user_params

    def post_execute(self, user_params: ExecutionParameters, rval: Any) -> Any:
        ray.shutdown()
        return rval


# Inject the Ray plugin into flytekits dynamic plugin loading system
TaskPlugins.register_pythontask_plugin(RayConfig, RayFunctionTask)

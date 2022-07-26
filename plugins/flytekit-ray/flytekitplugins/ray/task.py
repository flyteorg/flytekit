import base64
import json
from typing import Any, Callable, Optional, Dict

import ray

from flytekit.configuration import SerializationSettings
from flytekit.core.context_manager import ExecutionParameters
from flytekit.core.python_function_task import PythonFunctionTask
from flytekit.extend import TaskPlugins
from flytekit.models.core.resource import RayCluster, Resource

_RUNTIME = "runtime"


class RayConfig(object):
    def __init__(self, address: Optional[str] = None, ray_cluster: Optional[RayCluster] = None, runtime: dict = None, **kwargs):
        """
        :param str address: The address of the Ray cluster to connect to.
        :param dict kwargs: Extra arguments for ray.init(). https://docs.ray.io/en/latest/ray-core/package-ref.html#ray-init
        :param Optional[RayCluster] ray_cluster: Define Ray cluster spec.
        """
        if address and ray_cluster:
            raise ValueError("You cannot specify both address and ray_cluster")
        self._address = address
        self._extra_args = kwargs
        self._ray_cluster = ray_cluster
        self._resource = Resource(ray=ray_cluster)
        self._runtime = str(base64.b64encode(json.dumps(runtime).encode('UTF-8'))) if runtime else None

    @property
    def address(self) -> str:
        return self._address

    @property
    def ray_cluster(self) -> Optional[RayCluster]:
        return self._ray_cluster

    @property
    def runtime(self) -> Optional[str]:
        return self._runtime

    @property
    def extra_args(self) -> dict:
        return self._extra_args

    @property
    def resource(self) -> Resource:
        return self._resource


class RayFunctionTask(PythonFunctionTask):
    """
    Actual Plugin that transforms the local python code for execution within Ray job.
    """

    _RAY_TASK_TYPE = "ray"

    def __init__(self, task_config: RayConfig, task_function: Callable, **kwargs):
        if task_config is None:
            task_config = RayConfig()
        super().__init__(
            task_config=task_config,
            task_type=self._RAY_TASK_TYPE,
            task_function=task_function,
            resource=task_config.resource,
            **kwargs
        )
        self._task_config = task_config

    def pre_execute(self, user_params: ExecutionParameters) -> ExecutionParameters:
        ray.init(address=self._task_config.address, **self._task_config.extra_args)
        return user_params

    def post_execute(self, user_params: ExecutionParameters, rval: Any) -> Any:
        ray.shutdown()
        return rval


# Inject the Ray plugin into flytekits dynamic plugin loading system
TaskPlugins.register_pythontask_plugin(RayConfig, RayFunctionTask)

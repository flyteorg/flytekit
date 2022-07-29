import base64
import json
import typing
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

import ray
from flytekitplugins.ray.models import ClusterSpec, HeadGroupSpec, RayCluster, RayJob, WorkerGroupSpec
from google.protobuf.json_format import MessageToDict

from flytekit.configuration import SerializationSettings
from flytekit.core.context_manager import ExecutionParameters
from flytekit.core.python_function_task import PythonFunctionTask
from flytekit.extend import TaskPlugins


@dataclass
class HeadNodeConfig:
    ray_start_params: typing.Optional[typing.Dict[str, str]] = None


@dataclass
class WorkerNodeConfig:
    group_name: str
    replicas: int
    min_replicas: typing.Optional[int] = None
    max_replicas: typing.Optional[int] = None
    ray_start_params: typing.Optional[typing.Dict[str, str]] = None


@dataclass
class RayJobConfig:
    worker_node: WorkerNodeConfig
    head_node: typing.Optional[HeadNodeConfig] = HeadNodeConfig()
    runtime_env: typing.Optional[dict] = None
    address: typing.Optional[str] = None
    shutdown_after_job_finishes: typing.Optional[bool] = True
    ttl_seconds_after_finished: typing.Optional[bool] = 3600


class RayFunctionTask(PythonFunctionTask):
    """
    Actual Plugin that transforms the local python code for execution within Ray job.
    """

    _RAY_TASK_TYPE = "ray"

    def __init__(self, task_config: RayJobConfig, task_function: Callable, **kwargs):
        super().__init__(task_config=task_config, task_type=self._RAY_TASK_TYPE, task_function=task_function, **kwargs)
        self._task_config = task_config

    def pre_execute(self, user_params: ExecutionParameters) -> ExecutionParameters:
        ray.init(address=self._task_config.address)
        return user_params

    def post_execute(self, user_params: ExecutionParameters, rval: Any) -> Any:
        ray.shutdown()
        return rval

    def get_custom(self, settings: SerializationSettings) -> Optional[Dict[str, Any]]:
        cfg = self._task_config

        ray_job = RayJob(
            ray_cluster=RayCluster(
                ClusterSpec(
                    head_group_spec=HeadGroupSpec(cfg.head_node.ray_start_params),
                    worker_group_spec=[
                        WorkerGroupSpec(
                            cfg.worker_node.group_name,
                            cfg.worker_node.replicas,
                            cfg.worker_node.min_replicas,
                            cfg.worker_node.max_replicas,
                            cfg.worker_node.ray_start_params,
                        )
                    ],
                )
            ),
            # Use base64 to encode runtime_env dict and convert it to byte string
            runtime_env=base64.b64encode(json.dumps(cfg.runtime_env).encode()).decode(),
            shutdown_after_job_finishes=cfg.shutdown_after_job_finishes,
            ttl_seconds_after_finished=cfg.ttl_seconds_after_finished,
        )
        return MessageToDict(ray_job.to_flyte_idl())


# Inject the Ray plugin into flytekits dynamic plugin loading system
TaskPlugins.register_pythontask_plugin(RayJobConfig, RayFunctionTask)

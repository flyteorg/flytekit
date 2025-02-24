import base64
import json
import os
import typing
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

import yaml
from flytekitplugins.ray.models import (
    HeadGroupSpec,
    RayCluster,
    RayJob,
    WorkerGroupSpec,
)
from google.protobuf.json_format import MessageToDict

from flytekit import PodTemplate, Resources, lazy_module
from flytekit.configuration import SerializationSettings
from flytekit.core.context_manager import ExecutionParameters, FlyteContextManager
from flytekit.core.python_function_task import PythonFunctionTask
from flytekit.core.resources import pod_spec_from_resources
from flytekit.extend import TaskPlugins
from flytekit.models.task import K8sPod

ray = lazy_module("ray")
_RAY_HEAD_CONTAINER_NAME = "ray-head"
_RAY_WORKER_CONTAINER_NAME = "ray-worker"


@dataclass
class HeadNodeConfig:
    ray_start_params: typing.Optional[typing.Dict[str, str]] = None
    pod_template: typing.Optional[PodTemplate] = None
    requests: Optional[Resources] = None
    limits: Optional[Resources] = None

    def __post_init__(self):
        if self.pod_template:
            if self.requests and self.limits:
                raise ValueError("Cannot specify both pod_template and requests/limits")


@dataclass
class WorkerNodeConfig:
    group_name: str
    replicas: int
    min_replicas: typing.Optional[int] = None
    max_replicas: typing.Optional[int] = None
    ray_start_params: typing.Optional[typing.Dict[str, str]] = None
    pod_template: typing.Optional[PodTemplate] = None
    requests: Optional[Resources] = None
    limits: Optional[Resources] = None

    def __post_init__(self):
        if self.pod_template:
            if self.requests and self.limits:
                raise ValueError("Cannot specify both pod_template and requests/limits")


@dataclass
class RayJobConfig:
    worker_node_config: typing.List[WorkerNodeConfig]
    head_node_config: typing.Optional[HeadNodeConfig] = None
    enable_autoscaling: bool = False
    runtime_env: typing.Optional[dict] = None
    address: typing.Optional[str] = None
    shutdown_after_job_finishes: bool = False
    ttl_seconds_after_finished: typing.Optional[int] = None


class RayFunctionTask(PythonFunctionTask):
    """
    Actual Plugin that transforms the local python code for execution within Ray job.
    """

    _RAY_TASK_TYPE = "ray"

    def __init__(self, task_config: RayJobConfig, task_function: Callable, **kwargs):
        super().__init__(
            task_config=task_config,
            task_type=self._RAY_TASK_TYPE,
            task_function=task_function,
            **kwargs,
        )
        self._task_config = task_config

    def pre_execute(self, user_params: ExecutionParameters) -> ExecutionParameters:
        init_params = {"address": self._task_config.address}

        ctx = FlyteContextManager.current_context()
        if not ctx.execution_state.is_local_execution():
            working_dir = os.getcwd()
            init_params["runtime_env"] = {
                "working_dir": working_dir,
                "excludes": ["script_mode.tar.gz", "fast*.tar.gz", ".python_history"],
            }

        ray.init(**init_params)
        return user_params

    def get_custom(self, settings: SerializationSettings) -> Optional[Dict[str, Any]]:
        cfg = self._task_config
        # Deprecated: runtime_env is removed KubeRay >= 1.1.0. It is replaced by runtime_env_yaml
        runtime_env = base64.b64encode(json.dumps(cfg.runtime_env).encode()).decode() if cfg.runtime_env else None
        runtime_env_yaml = yaml.dump(cfg.runtime_env) if cfg.runtime_env else None

        head_group_spec = None
        if cfg.head_node_config:
            if cfg.head_node_config.requests or cfg.head_node_config.limits:
                head_pod_template = PodTemplate(
                    pod_spec=pod_spec_from_resources(
                        primary_container_name=_RAY_HEAD_CONTAINER_NAME,
                        requests=cfg.head_node_config.requests,
                        limits=cfg.head_node_config.limits,
                    )
                )
            else:
                head_pod_template = cfg.head_node_config.pod_template

            head_group_spec = HeadGroupSpec(
                cfg.head_node_config.ray_start_params,
                K8sPod.from_pod_template(head_pod_template) if head_pod_template else None,
            )

        worker_group_spec: typing.List[WorkerGroupSpec] = []
        for c in cfg.worker_node_config:
            if c.requests or c.limits:
                worker_pod_template = PodTemplate(
                    pod_spec=pod_spec_from_resources(
                        primary_container_name=_RAY_WORKER_CONTAINER_NAME,
                        requests=c.requests,
                        limits=c.limits,
                    )
                )
            else:
                worker_pod_template = c.pod_template
            k8s_pod = K8sPod.from_pod_template(worker_pod_template) if worker_pod_template else None
            worker_group_spec.append(
                WorkerGroupSpec(c.group_name, c.replicas, c.min_replicas, c.max_replicas, c.ray_start_params, k8s_pod)
            )

        ray_job = RayJob(
            ray_cluster=RayCluster(
                head_group_spec=head_group_spec,
                worker_group_spec=worker_group_spec,
                enable_autoscaling=(cfg.enable_autoscaling if cfg.enable_autoscaling else False),
            ),
            runtime_env=runtime_env,
            runtime_env_yaml=runtime_env_yaml,
            ttl_seconds_after_finished=cfg.ttl_seconds_after_finished,
            shutdown_after_job_finishes=cfg.shutdown_after_job_finishes,
        )
        return MessageToDict(ray_job.to_flyte_idl())


# Inject the Ray plugin into flytekits dynamic plugin loading system
TaskPlugins.register_pythontask_plugin(RayJobConfig, RayFunctionTask)

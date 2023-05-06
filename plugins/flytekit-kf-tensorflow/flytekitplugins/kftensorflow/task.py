"""
This Plugin adds the capability of running distributed tensorflow training to Flyte using backend plugins, natively on
Kubernetes. It leverages `TF Job <https://github.com/kubeflow/tf-operator>`_ Plugin from kubeflow.
"""
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, Optional

from flytekitplugins.kftensorflow import models
from google.protobuf.json_format import MessageToDict

from flytekit import PythonFunctionTask, Resources
from flytekit.configuration import SerializationSettings
from flytekit.core.resources import convert_resources_to_resource_model
from flytekit.extend import TaskPlugins


@dataclass
class RunPolicy:
    clean_pod_policy: models.CleanPodPolicy = None
    ttl_seconds_after_finished: Optional[int] = None
    active_deadline_seconds: Optional[int] = None
    backoff_limit: Optional[int] = None


@dataclass
class Chief:
    image: Optional[str] = None
    requests: Optional[Resources] = None
    limits: Optional[Resources] = None
    replicas: Optional[int] = 0
    restart_policy: Optional[models.RestartPolicy] = None


@dataclass
class PS:
    image: Optional[str] = None
    requests: Optional[Resources] = None
    limits: Optional[Resources] = None
    replicas: Optional[int] = None
    restart_policy: Optional[models.RestartPolicy] = None


@dataclass
class Worker:
    image: Optional[str] = None
    requests: Optional[Resources] = None
    limits: Optional[Resources] = None
    replicas: Optional[int] = 1
    restart_policy: Optional[models.RestartPolicy] = None


@dataclass
class TfJob:
    chief: Chief = field(default_factory=lambda: Chief())
    ps: PS = field(default_factory=lambda: PS())
    worker: Worker = field(default_factory=lambda: Worker())
    run_policy: Optional[RunPolicy] = field(default_factory=lambda: None)


class TensorflowFunctionTask(PythonFunctionTask[TfJob]):
    """
    Plugin that submits a TFJob (see https://github.com/kubeflow/tf-operator)
        defined by the code within the _task_function to k8s cluster.
    """

    _TF_JOB_TASK_TYPE = "tensorflow"

    def __init__(self, task_config: TfJob, task_function: Callable, **kwargs):
        super().__init__(
            task_type=self._TF_JOB_TASK_TYPE,
            task_config=task_config,
            task_function=task_function,
            task_type_version=1,
            **kwargs,
        )

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        chief = models.Chief(
            replicas=self.task_config.chief.replicas,
            image=self.task_config.chief.image,
            resources=convert_resources_to_resource_model(
                requests=self.task_config.chief.requests,
                limits=self.task_config.chief.limits,
            ),
            restart_policy=self.task_config.chief.restart_policy,
        )
        worker = models.Worker(
            replicas=self.task_config.worker.replicas,
            image=self.task_config.worker.image,
            resources=convert_resources_to_resource_model(
                requests=self.task_config.worker.requests,
                limits=self.task_config.worker.limits,
            ),
            restart_policy=self.task_config.worker.restart_policy,
        )
        ps = models.PS(
            replicas=self.task_config.ps.replicas,
            image=self.task_config.ps.image,
            resources=convert_resources_to_resource_model(
                requests=self.task_config.ps.requests,
                limits=self.task_config.ps.limits,
            ),
            restart_policy=self.task_config.ps.restart_policy,
        )
        run_policy = (
            models.RunPolicy(
                clean_pod_policy=self.task_config.run_policy.clean_pod_policy,
                ttl_seconds_after_finished=self.task_config.run_policy.ttl_seconds_after_finished,
                active_deadline_seconds=self.task_config.run_policy.active_deadline_seconds,
                backoff_limit=self.task_config.run_policy.backoff_limit,
            )
            if self.task_config.run_policy
            else None
        )

        job = models.TensorFlowJob(worker=worker, chief=chief, ps=ps, run_policy=run_policy)
        return MessageToDict(job.to_flyte_idl())


# Register the Tensorflow Plugin into the flytekit core plugin system
TaskPlugins.register_pythontask_plugin(TfJob, TensorflowFunctionTask)

"""
This Plugin adds the capability of running distributed tensorflow training to Flyte using backend plugins, natively on
Kubernetes. It leverages `TF Job <https://github.com/kubeflow/tf-operator>`_ Plugin from kubeflow.
"""
from dataclasses import dataclass
from typing import Any, Callable, Dict

from google.protobuf.json_format import MessageToDict

from flytekit import PythonFunctionTask
from flytekit.configuration import SerializationSettings
from flytekit.extend import TaskPlugins

from .models import TensorFlowJob


@dataclass
class TfJob(object):
    """
    Configuration for an executable `TF Job <https://github.com/kubeflow/tf-operator>`_. Use this
    to run distributed tensorflow training on k8s (with parameter server)

    Args:
        num_workers: integer determining the number of worker replicas spawned in the cluster for this job
        (in addition to 1 master).

        num_ps_replicas: Number of Parameter server replicas to use

        num_chief_replicas: Number of chief replicas to use

    """

    num_workers: int
    num_ps_replicas: int
    num_chief_replicas: int


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
            **kwargs,
        )

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        job = TensorFlowJob(
            workers_count=self.task_config.num_workers,
            ps_replicas_count=self.task_config.num_ps_replicas,
            chief_replicas_count=self.task_config.num_chief_replicas,
        )
        return MessageToDict(job.to_flyte_idl())


# Register the Tensorflow Plugin into the flytekit core plugin system
TaskPlugins.register_pythontask_plugin(TfJob, TensorflowFunctionTask)

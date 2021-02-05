"""
This Plugin adds the capability of running distributed pytorch training to Flyte using backend plugins, natively on
Kubernetes. It leverages `Pytorch Job <https://github.com/kubeflow/pytorch-operator>`_ Plugin from kubeflow.
"""
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

from google.protobuf.json_format import MessageToDict

from flytekit import PythonFunctionTask, Resources
from flytekit.extend import SerializationSettings, TaskPlugins
from flytekit.models import task as _task_model


@dataclass
class PyTorch(object):
    """
    Configuration for an executable `Pytorch Job <https://github.com/kubeflow/pytorch-operator>`_. Use this
    to run distributed pytorch training on k8s

    Args:
        num_workers: integer determining the number of worker replicas spawned in the cluster for this job
        (in addition to 1 master).

        per_replica_requests: [optional] lower-bound resources for each replica spawned for this job
        (i.e. both for (main)master and workers).  Default is set by platform-level configuration.

        per_replica_limits: [optional] upper-bound resources for each replica spawned for this job. If not specified
        the scheduled resource may not have all the resources
    """

    num_workers: int
    per_replica_requests: Optional[Resources] = None
    per_replica_limits: Optional[Resources] = None


class PyTorchFunctionTask(PythonFunctionTask[PyTorch]):
    """
    Plugin that submits a PyTorchJob (see https://github.com/kubeflow/pytorch-operator)
        defined by the code within the _task_function to k8s cluster.
    """

    _PYTORCH_TASK_TYPE = "pytorch"

    def __init__(self, task_config: PyTorch, task_function: Callable, **kwargs):
        super().__init__(
            task_config,
            task_function,
            task_type=self._PYTORCH_TASK_TYPE,
            requests=task_config.per_replica_requests,
            limits=task_config.per_replica_limits,
            **kwargs
        )

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        job = _task_model.PyTorchJob(workers_count=self.task_config.num_workers)
        return MessageToDict(job.to_flyte_idl())


# Register the Pytorch Plugin into the flytekit core plugin system
TaskPlugins.register_pythontask_plugin(PyTorch, PyTorchFunctionTask)

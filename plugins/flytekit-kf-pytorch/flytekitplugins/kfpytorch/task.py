"""
This Plugin adds the capability of running distributed pytorch training to Flyte using backend plugins, natively on
Kubernetes. It leverages `Pytorch Job <https://github.com/kubeflow/pytorch-operator>`_ Plugin from kubeflow.
"""
from dataclasses import dataclass
from typing import Any, Callable, Dict

from google.protobuf.json_format import MessageToDict

from flytekit import PythonFunctionTask
from flytekit.configuration import SerializationSettings
from flytekit.extend import TaskPlugins

from .models import PyTorchJob


@dataclass
class PyTorch(object):
    """
    Configuration for an executable `Pytorch Job <https://github.com/kubeflow/pytorch-operator>`_. Use this
    to run distributed pytorch training on k8s

    Args:
        num_workers: integer determining the number of worker replicas spawned in the cluster for this job
        (in addition to 1 master).

    """

    num_workers: int


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
            **kwargs,
        )

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        job = PyTorchJob(workers_count=self.task_config.num_workers)
        return MessageToDict(job.to_flyte_idl())


# Register the Pytorch Plugin into the flytekit core plugin system
TaskPlugins.register_pythontask_plugin(PyTorch, PyTorchFunctionTask)

"""
This Plugin adds the capability of running distributed pytorch training to Flyte using backend plugins, natively on
Kubernetes. It leverages `Pytorch Job <https://github.com/kubeflow/pytorch-operator>`_ Plugin from kubeflow.
"""
import os
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Union

import cloudpickle
from flyteidl.plugins.pytorch_pb2 import DistributedPyTorchTrainingTask
from google.protobuf.json_format import MessageToDict

import flytekit
from flytekit import PythonFunctionTask
from flytekit.configuration import SerializationSettings
from flytekit.extend import IgnoreOutputs, TaskPlugins

TORCH_IMPORT_ERROR_MESSAGE = "PyTorch is not installed. Please install `flytekitplugins-kfpytorch['elastic']`."


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


@dataclass
class Elastic(object):
    """
    Configuration for `torch elastic training <https://pytorch.org/docs/stable/elastic/run.html>`_.

    Use this to run single- or multi-node distributed pytorch elastic training on k8s.

    Single-node elastic training is executed in a k8s pod when `nnodes` is set to 1.
    Multi-node training is executed otherwise using a `Pytorch Job <https://github.com/kubeflow/training-operator>`_.

    Args:
        nnodes (Union[int, str]): Number of nodes, or the range of nodes in form <minimum_nodes>:<maximum_nodes>.
        nproc_per_node (Union[int, str]): Number of workers per node. Supported values are [auto, cpu, gpu, int].
        start_method (str): Multiprocessing start method to use when creating workers.
        monitor_interval (int): Interval, in seconds, to monitor the state of workers.
        max_restarts (int): Maximum number of worker group restarts before failing.
    """

    nnodes: Union[int, str] = 1
    nproc_per_node: Union[int, str] = "auto"
    start_method: str = "spawn"
    monitor_interval: int = 5
    max_restarts: int = 0


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
        job = DistributedPyTorchTrainingTask(workers=self.task_config.num_workers)
        return MessageToDict(job)


# Register the Pytorch Plugin into the flytekit core plugin system
TaskPlugins.register_pythontask_plugin(PyTorch, PyTorchFunctionTask)


def spawn_helper(fn: bytes, kwargs) -> Any:
    """Help to spawn worker processes.

    The purpose of this function is to 1) be pickleable so that it can be used with
    the multiprocessing start method `spawn` and 2) to call a cloudpickle-serialized
    function passed to it. This function itself doesn't have to be pickleable. Without
    such a helper task functions, which are not pickleable, couldn't be used with the
    start method `spawn`.

    Args:
        fn (bytes): Cloudpickle-serialized target function to be executed in the worker process.

    Returns:
        The return value of the received target function.
    """
    fn = cloudpickle.loads(fn)
    return_val = fn(**kwargs)
    return return_val


class PytorchElasticFunctionTask(PythonFunctionTask[Elastic]):
    """
    Plugin for distributed training with torch elastic/torchrun (see
    https://pytorch.org/docs/stable/elastic/run.html).
    """

    _ELASTIC_TASK_TYPE = "pytorch"
    _ELASTIC_TASK_TYPE_STANDALONE = "python-task"

    def __init__(self, task_config: Elastic, task_function: Callable, **kwargs):
        task_type = self._ELASTIC_TASK_TYPE_STANDALONE if task_config.nnodes == 1 else self._ELASTIC_TASK_TYPE

        super(PytorchElasticFunctionTask, self).__init__(
            task_config=task_config,
            task_type=task_type,
            task_function=task_function,
            **kwargs,
        )
        try:
            from torch.distributed import run
        except ImportError:
            raise ImportError(TORCH_IMPORT_ERROR_MESSAGE)
        self.min_nodes, self.max_nodes = run.parse_min_max_nnodes(str(self.task_config.nnodes))

        """
        c10d is the backend recommended by torch elastic.
        https://pytorch.org/docs/stable/elastic/run.html#note-on-rendezvous-backend

        For c10d, no backend server has to be deployed.
        https://pytorch.org/docs/stable/elastic/run.html#deployment
        Instead, the workers will use the master's address as the rendezvous point.
        """
        self.rdzv_backend = "c10d"

    def _execute(self, **kwargs) -> Any:
        """
        This helper method will be invoked to execute the task.


        Returns:
            The result of rank zero.
        """
        try:
            from torch.distributed import run
            from torch.distributed.launcher.api import LaunchConfig, elastic_launch
        except ImportError:
            raise ImportError(TORCH_IMPORT_ERROR_MESSAGE)

        if isinstance(self.task_config.nproc_per_node, str):
            nproc = run.determine_local_world_size(self.task_config.nproc_per_node)
        else:
            nproc = self.task_config.nproc_per_node

        config = LaunchConfig(
            run_id=flytekit.current_context().execution_id.name,
            min_nodes=self.min_nodes,
            max_nodes=self.max_nodes,
            nproc_per_node=nproc,
            rdzv_backend=self.rdzv_backend,  # rdzv settings
            rdzv_endpoint=os.environ.get("PET_RDZV_ENDPOINT", "localhost:0"),
            max_restarts=self.task_config.max_restarts,
            monitor_interval=self.task_config.monitor_interval,
            start_method=self.task_config.start_method,
        )

        if self.task_config.start_method == "spawn":
            """
            We use cloudpickle to serialize the non-pickleable task function.
            The torch elastic launcher then launches the spawn_helper function (which is pickleable)
            instead of the task function. This helper function, in the child-process, then deserializes
            the task function, again with cloudpickle, and executes it.
            """
            launcher_target_func = spawn_helper

            dumped_target_function = cloudpickle.dumps(self._task_function)
            launcher_args = (dumped_target_function, kwargs)
        elif self.task_config.start_method == "fork":
            """
            The torch elastic launcher doesn't support passing kwargs to the target function,
            only args. Flyte only works with kwargs. Thus, we create a closure which already has
            the task kwargs bound. We tell the torch elastic launcher to start this function in
            the child processes.
            """

            def fn_partial():
                """Closure of the task function with kwargs already bound."""
                return self._task_function(**kwargs)

            launcher_target_func = fn_partial
            launcher_args = ()

        else:
            raise Exception("Bad start method")

        out = elastic_launch(
            config=config,
            entrypoint=launcher_target_func,
        )(*launcher_args)

        # `out` is a dictionary of rank (not local rank) -> result
        # Rank 0 returns the result of the task function
        if 0 in out:
            return out[0]
        else:
            raise IgnoreOutputs()

    def execute(self, **kwargs) -> Any:
        """
        This method will be invoked to execute the task.

        Handles the exception scope for the `_execute` method.
        """
        from flytekit.exceptions import scopes as exception_scopes

        return exception_scopes.user_entry_point(self._execute)(**kwargs)

    def get_custom(self, settings: SerializationSettings) -> Optional[Dict[str, Any]]:
        if self.task_config.nnodes == 1:
            """
            Torch elastic distributed training is executed in a normal k8s pod so that this
            works without the kubeflow train operator.
            """
            return super().get_custom(settings)
        else:
            from flyteidl.plugins.pytorch_pb2 import ElasticConfig

            elastic_config = ElasticConfig(
                rdzv_backend=self.rdzv_backend,
                min_replicas=self.min_nodes,
                max_replicas=self.max_nodes,
                nproc_per_node=self.task_config.nproc_per_node,
                max_restarts=self.task_config.max_restarts,
            )
            job = DistributedPyTorchTrainingTask(
                workers=self.max_nodes,
                elastic_config=elastic_config,
            )
            return MessageToDict(job)


# Register the PytorchElastic Plugin into the flytekit core plugin system
TaskPlugins.register_pythontask_plugin(Elastic, PytorchElasticFunctionTask)

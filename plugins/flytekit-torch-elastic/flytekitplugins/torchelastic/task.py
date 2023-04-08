import typing
from dataclasses import dataclass
from typing import Any, Callable

import cloudpickle
from torch.distributed import run
from torch.distributed.launcher.api import LaunchConfig, elastic_launch

import flytekit
from flytekit import PythonFunctionTask
from flytekit.extend import TaskPlugins


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
    nnodes: typing.Union[int, str] = 1
    nproc_per_node: typing.Union[int, str] = "auto"
    start_method: str = "spawn"
    monitor_interval: int = 5
    max_restarts: int = 10


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

    _ELASTIC_TASK_TYPE = "torch-elastic"

    def __init__(self, task_config: Elastic, task_function: Callable, **kwargs):
        super(PytorchElasticFunctionTask, self).__init__(
            task_config=task_config,
            task_type=self._ELASTIC_TASK_TYPE,
            task_function=task_function,
            **kwargs,
        )

    def execute(self, **kwargs) -> Any:
        """
        This method will be invoked to execute the task. If you do decide to override this method you must also
        handle dynamic tasks or you will no longer be able to use the task as a dynamic task generator.

        Returns:
            The result of rank zero.
        """
        min_nodes, max_nodes = run.parse_min_max_nnodes(str(self.task_config.nnodes))

        if isinstance(self.task_config.nproc_per_node, str):
            nproc = run.determine_local_world_size(self.task_config.nproc_per_node)
        else:
            nproc = self.task_config.nproc_per_node
        config = LaunchConfig(
            run_id=flytekit.current_context().execution_id.name,
            min_nodes=min_nodes,
            max_nodes=max_nodes,
            nproc_per_node=nproc,
            rdzv_backend="c10d", # rdzv settings
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

        # put the try catch here
        # https://github.com/flyteorg/flytekit/blob/47ac6ac2a547fdd2b46654db5163493b4f33dbb2/flytekit/core/python_function_task.py#L164

        out = elastic_launch(
            config=config,
            entrypoint=launcher_target_func,
        )(*launcher_args)

        return out[0]


TaskPlugins.register_pythontask_plugin(Elastic, PytorchElasticFunctionTask)

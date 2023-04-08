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
    min_replicas: int = 1
    max_replicas: int = 1
    nproc_per_node: typing.Union[int, str] = "auto"
    start_method: str = "spawn"
    monitor_interval: int = 5  # Interval, in seconds, to monitor the state of workers.
    max_restarts: int = 10  # Maximum number of worker group restarts before failing.


def mp_helper(fn, kwargs):
    print("Using start method spawn")
    fn = cloudpickle.loads(fn)
    return_val = fn(**kwargs)
    return return_val


class PytorchElasticFunctionTask(PythonFunctionTask[Elastic]):
    """
    Actual Plugin that transforms the local python code for execution within a spark context
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
        """
        min_nodes, max_nodes = 1, 1
        
        """
        All this is mocked.
        """
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
            # rdzv_endpoint = "foo"
            start_method=self.task_config.start_method,
        )

        if self.task_config.start_method == "spawn":
            """
            If the user wants to use spawn, we use cloudpickle to serialize the task function.
            We then tell the torch elastic launcher to launch the mp_helper function (which is pickleable)
            instead of the task function. This helper function, in the child-process, then deserializes
            the task function, again with cloudpickle, and executes it.
            
            Note from a few weeks later:
            We might be able to pass the string representation of the task which is used by the task
            resolver to the helper function. In the child process, the task resolver could retrieve the task.
            But we would need a way to not start further child processes from the child process but just execute
            the task function. But the idea basically is: pass task name to child process and use task resolver
            instead of cloudpickle serialisation.
            """
            launcher_target_func = mp_helper

            dumped_target_function = cloudpickle.dumps(self._task_function)
            launcher_args = (dumped_target_function, kwargs)
        elif self.task_config.start_method == "fork":
            """
            If the user wants to do fork, we don't have to serialize the task function with cloudpickle.
            However, the torch elastic launcher doesn't support passing kwargs to the target function,
            only args. Flyte only works with kwargs.
            Thus, we create a closure which already has the task kwargs bound. We tell the torch elastic
            launcher to start this function in the child processes.
            """

            def fn_partial():
                print("Using start method fork")
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

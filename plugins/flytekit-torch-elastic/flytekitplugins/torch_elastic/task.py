from dataclasses import dataclass
from typing import Any, Callable

from flytekit import FlyteContextManager, PythonFunctionTask
from flytekit.core.context_manager import ExecutionParameters
from flytekit.extend import TaskPlugins
from torch.distributed.launcher.api import LaunchConfig, elastic_launch
from torch.distributed.run import parse_min_max_nnodes


@dataclass
class Elastic(object):
    min_replicas: int
    max_replicas: int
    start_method: str = "spawn"


def mp_helper(fn, kwargs):
    print("Using start method spawn")
    import dill

    fn = dill.loads(fn)

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
        config = LaunchConfig(
            run_id="none",
            min_nodes=min_nodes,
            max_nodes=max_nodes,
            nproc_per_node=4,
            rdzv_backend="c10d",
            max_restarts=1,
            # rdzv_endpoint = "foo"
            start_method=self.task_config.start_method,
        )

        if self.task_config.start_method == "spawn":
            """
            If the user wants to use spawn, we use dill to serialize the task function.
            We then tell the torch elastic launcher to launch the mp_helper function (which is pickleable)
            instead of the task function. This helper function, in the child-process, then deserializes
            the task function, again with dill, and executes it.
            
            Note from a few weeks later:
            We might be able to pass the string representation of the task which is used by the task
            resolver to the helper function. In the child process, the task resolver could retrieve the task.
            But we would need a way to not start further child processes from the child process but just execute
            the task function. But the idea basically is: pass task name to child process and use task resolver
            instead of dill serialisation.
            """
            launcher_target_func = mp_helper

            import dill

            dumped_target_function = dill.dumps(self._task_function)
            launcher_args = (dumped_target_function, kwargs)
        elif self.task_config.start_method == "fork":
            """
            If the user wants to do fork, we don't have to serialize the task function with dill.
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

        # return super().execute(**kwargs)


TaskPlugins.register_pythontask_plugin(Elastic, PytorchElasticFunctionTask)

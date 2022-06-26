import typing
from dataclasses import dataclass

from flytekit import PythonFunctionTask
from flytekit.core.task import TaskPlugins


@dataclass
class Dask:
    """
    # FIXME: TODO
    """
    pass


class DaskTask(PythonFunctionTask[Dask]):
    """
    Actual Plugin that transforms the local python code for execution within a spark context
    """

    _DASK_TASK_TYPE = "dask"

    def __init__(self, task_config: Dask, task_function: typing.Callable, **kwargs):
        super(DaskTask, self).__init__(
            task_config=task_config,
            task_type=self._DASK_TASK_TYPE,
            task_function=task_function,
            **kwargs,
        )


# Inject the `dask` plugin into flytekits dynamic plugin loading system
TaskPlugins.register_pythontask_plugin(Dask, DaskTask)

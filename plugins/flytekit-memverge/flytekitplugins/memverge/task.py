from dataclasses import dataclass

from flytekit import PythonFunctionTask
from flytekit.core.task import TaskPlugins


@dataclass
class MemvergeConfig(object):
    ...


class MemvergeTask(PythonFunctionTask[MemvergeConfig]):
    _TASK_TYPE = "memverge_task"

    def __init__(self, *args, **kwargs):
        super().__init__(task_type=self._TASK_TYPE, *args, **kwargs)


TaskPlugins.register_pythontask_plugin(MemvergeConfig, MemvergeTask)

import os
import typing
from dataclasses import dataclass

from flytekit import PythonFunctionTask
from flytekit.core.task import TaskPlugins
from flytekit.extend.backend.base_agent import AsyncAgentExecutorMixin


@dataclass
class MemvergeConfig(object):
    ...


class MemvergeTask(AsyncAgentExecutorMixin, PythonFunctionTask[MemvergeConfig]):
    _TASK_TYPE = "memverge_task"

    def __init__(self, *args, **kwargs):
        super().__init__(task_type=self._TASK_TYPE, *args, **kwargs)

    def execute(self, **kwargs) -> typing.Any:
        if os.getenv("FLYTE_ON_MMCLOUD"):
            print("Running on Memverge Cloud")
            return super(MemvergeTask, self).execute(**kwargs)
        else:
            return super(AsyncAgentExecutorMixin, self).execute(**kwargs)


TaskPlugins.register_pythontask_plugin(MemvergeConfig, MemvergeTask)

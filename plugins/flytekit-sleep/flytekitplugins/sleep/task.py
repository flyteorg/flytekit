from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

from flytekit.configuration import SerializationSettings
from flytekit.core.python_function_task import PythonFunctionTask
from flytekit.core.task import TaskPlugins


@dataclass
class Sleep:
    """
    Route a task to the backend ``core-sleep`` plugin.

    The sleep duration is provided as a normal task input, not plugin config.
    No container is launched; the backend handles the sleep directly.

    Usage::

        from flytekitplugins.sleep import Sleep
        from flytekit import task
        from datetime import timedelta

        @task(task_config=Sleep())
        def sleep_for(duration: timedelta) -> None:
            pass  # only runs locally; backend executes the sleep
    """


class SleepFunctionTask(PythonFunctionTask[Sleep]):
    _TASK_TYPE = "core-sleep"

    def __init__(self, task_config: Optional[Sleep], task_function: Callable, **kwargs):
        super().__init__(
            task_config=task_config or Sleep(),
            task_function=task_function,
            task_type=self._TASK_TYPE,
            **kwargs,
        )

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        return {}


TaskPlugins.register_pythontask_plugin(Sleep, SleepFunctionTask)

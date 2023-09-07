import os
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Optional, Union

from google.protobuf import json_format
from google.protobuf.struct_pb2 import Struct

from flytekit.configuration import DefaultImages, SerializationSettings
from flytekit.core.python_function_task import PythonFunctionTask
from flytekit.extend import TaskPlugins
from flytekit.extend.backend.base_agent import AsyncAgentExecutorMixin
from flytekit.image_spec.image_spec import ImageSpec


@dataclass
class FloatConfig(object):
    """
    Configures FloatTask. Tasks specified with FloatConfig will be executed using Memory Machine Cloud.
    """

    # This allows the user to specify additional arguments for the float submit command
    submit_extra: str = ""


class FloatTask(AsyncAgentExecutorMixin, PythonFunctionTask):
    _TASK_TYPE = "float_task"

    def __init__(
        self,
        task_config: Optional[FloatConfig],
        task_function: Callable,
        container_image: Optional[Union[str, ImageSpec]],
        **kwargs,
    ):
        super().__init__(
            task_config=task_config or FloatConfig(),
            task_type=self._TASK_TYPE,
            task_function=task_function,
            container_image=container_image or DefaultImages.default_image(),
            **kwargs,
        )

    def execute(self, **kwargs) -> Any:
        # FLOAT_JOB_ID should always and only be defined on a Memory Machine Cloud worker node
        if os.getenv("FLOAT_JOB_ID"):
            # Task should be run as a normal PythonFunctionTask
            return PythonFunctionTask.execute(self, **kwargs)
        else:
            # Assume local execution without a FlytePropeller deployment
            return AsyncAgentExecutorMixin.execute(self, **kwargs)

    def get_custom(self, settings: SerializationSettings) -> dict[str, Any]:
        """
        Return plugin-specific data as a serializable dictionary.
        """
        config = {"submit_extra": self.task_config.submit_extra}
        s = Struct()
        s.update(config)
        return json_format.MessageToDict(s)


TaskPlugins.register_pythontask_plugin(FloatConfig, FloatTask)

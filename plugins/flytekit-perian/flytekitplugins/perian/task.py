from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Union

from google.protobuf import json_format
from google.protobuf.struct_pb2 import Struct

from flytekit import PythonFunctionTask
from flytekit.configuration import SerializationSettings
from flytekit.exceptions.user import FlyteUserException
from flytekit.extend import TaskPlugins
from flytekit.image_spec import ImageSpec


@dataclass
class PerianConfig:
    """Used to configure a Perian Task"""
    # Number of CPU cores
    cores: Optional[int] = None
    # Amount of memory in GB
    memory: Optional[int] = None
    # Number of accelerators
    accelerators: Optional[int] = None
    # Type of accelerator (e.g. 'A100')
    # For a full list of supported accelerators, use the perian CLI list-accelerators command
    accelerator_type: Optional[str] = None
    # Country code to run the job in (e.g. 'DE')
    country_code: Optional[str] = None


class PerianTask(PythonFunctionTask):
    """A special task type for running tasks on Perian Job Platform (perian.io)"""

    _TASK_TYPE = "perian_task"

    def __init__(
        self,
        task_config: Optional[PerianConfig],
        task_function: Callable,
        container_image: Optional[Union[str, ImageSpec]] = None,
        **kwargs,
    ):
        super().__init__(
            task_config=task_config,
            task_function=task_function,
            container_image=container_image,
            task_type=self._TASK_TYPE,
            **kwargs,
        )

    def execute(self, **kwargs) -> Any:
        return PythonFunctionTask.execute(self, **kwargs)

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        """
        Return plugin-specific data as a serializable dictionary.
        """
        config = {
            "cores": self.task_config.cores,
            "memory": self.task_config.memory,
            "accelerators": self.task_config.accelerators,
            "accelerator_type": self.task_config.accelerator_type,
            "country_code": _validate_and_format_country_code(self.task_config.country_code),
        }
        s = Struct()
        s.update(config)
        return json_format.MessageToDict(s)


def _validate_and_format_country_code(country_code: Optional[str]) -> Optional[str]:
    if not country_code:
        return None
    if len(country_code) != 2:
        raise FlyteUserException(
            "Invalid country code. Please provide a valid two-letter country code. (e.g. DE)"
        )
    return country_code.upper()


# Inject the Perian plugin into flytekits dynamic plugin loading system
TaskPlugins.register_pythontask_plugin(PerianConfig, PerianTask)

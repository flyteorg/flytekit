from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Union

from google.protobuf import json_format
from google.protobuf.struct_pb2 import Struct

from flytekit import FlyteContextManager, PythonFunctionTask, logger
from flytekit.configuration import SerializationSettings
from flytekit.exceptions.user import FlyteUserException
from flytekit.extend import TaskPlugins
from flytekit.extend.backend.base_agent import AsyncAgentExecutorMixin
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
    # Cloud provider to run the job on
    provider: Optional[str] = None


class PerianTask(AsyncAgentExecutorMixin, PythonFunctionTask):
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
        if isinstance(self.task_config, PerianConfig):
            # Use the Perian agent to run it by default.
            try:
                ctx = FlyteContextManager.current_context()
                if not ctx.file_access.is_remote(ctx.file_access.raw_output_prefix):
                    raise ValueError(
                        "To submit a Perian job locally,"
                        " please set --raw-output-data-prefix to a remote path. e.g. s3://, gcs//, etc."
                    )
                if ctx.execution_state and ctx.execution_state.is_local_execution():
                    return AsyncAgentExecutorMixin.execute(self, **kwargs)
            except Exception as e:
                logger.error("Agent failed to run the task with error: %s", e)
                raise
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
            "provider": self.task_config.provider,
        }
        config = {k: v for k, v in config.items() if v is not None}
        s = Struct()
        s.update(config)
        return json_format.MessageToDict(s)


def _validate_and_format_country_code(country_code: Optional[str]) -> Optional[str]:
    if not country_code:
        return None
    if len(country_code) != 2:
        raise FlyteUserException("Invalid country code. Please provide a valid two-letter country code. (e.g. DE)")
    return country_code.upper()


# Inject the Perian plugin into flytekits dynamic plugin loading system
TaskPlugins.register_pythontask_plugin(PerianConfig, PerianTask)

from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, OrderedDict, Type, Union

from google.protobuf import json_format
from google.protobuf.struct_pb2 import Struct

from flytekit import FlyteContextManager, PythonFunctionTask, logger
from flytekit.configuration import SerializationSettings
from flytekit.core.base_task import PythonTask
from flytekit.core.interface import Interface
from flytekit.exceptions.user import FlyteUserException
from flytekit.extend import TaskPlugins
from flytekit.extend.backend.base_connector import AsyncConnectorExecutorMixin
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
    # OS storage size in GB
    os_storage_size: Optional[int] = None
    # Country code to run the job in (e.g. 'DE')
    country_code: Optional[str] = None
    # Cloud provider to run the job on
    provider: Optional[str] = None


class PerianTask(AsyncConnectorExecutorMixin, PythonFunctionTask):
    """A special task type for running Python function tasks on PERIAN Job Platform (perian.io)"""

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
            # Use the Perian connector to run it by default.
            try:
                ctx = FlyteContextManager.current_context()
                if not ctx.file_access.is_remote(ctx.file_access.raw_output_prefix):
                    raise ValueError(
                        "To submit a Perian job locally,"
                        " please set --raw-output-data-prefix to a remote path. e.g. s3://, gcs//, etc."
                    )
                if ctx.execution_state and ctx.execution_state.is_local_execution():
                    return AsyncConnectorExecutorMixin.execute(self, **kwargs)
            except Exception as e:
                logger.error("Connector failed to run the task with error: %s", e)
                raise
        return PythonFunctionTask.execute(self, **kwargs)

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        """
        Return plugin-specific data as a serializable dictionary.
        """
        config = _get_custom_task_config(self.task_config)
        if self.environment:
            config["environment"] = self.environment
        s = Struct()
        s.update(config)
        return json_format.MessageToDict(s)


class PerianContainerTask(AsyncConnectorExecutorMixin, PythonTask[PerianConfig]):
    """A special task type for running Python container (not function) tasks on PERIAN Job Platform (perian.io)"""

    _TASK_TYPE = "perian_task"

    def __init__(
        self,
        name: str,
        task_config: PerianConfig,
        image: str,
        command: List[str],
        inputs: Optional[OrderedDict[str, Type]] = None,
        **kwargs,
    ):
        if "outputs" in kwargs or "output_data_dir" in kwargs:
            raise ValueError("PerianContainerTask does not support 'outputs' or 'output_data_dir' arguments")
        super().__init__(
            name=name,
            task_type=self._TASK_TYPE,
            task_config=task_config,
            interface=Interface(inputs=inputs or {}),
            **kwargs,
        )
        self._image = image
        self._command = command

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        """
        Return plugin-specific data as a serializable dictionary.
        """
        config = _get_custom_task_config(self.task_config)
        config["image"] = self._image
        config["command"] = self._command
        if self.environment:
            config["environment"] = self.environment
        s = Struct()
        s.update(config)
        return json_format.MessageToDict(s)


def _get_custom_task_config(task_config: PerianConfig) -> Dict[str, Any]:
    config = {
        "cores": task_config.cores,
        "memory": task_config.memory,
        "accelerators": task_config.accelerators,
        "accelerator_type": task_config.accelerator_type,
        "os_storage_size": task_config.os_storage_size,
        "country_code": _validate_and_format_country_code(task_config.country_code),
        "provider": task_config.provider,
    }
    config = {k: v for k, v in config.items() if v is not None}
    return config


def _validate_and_format_country_code(country_code: Optional[str]) -> Optional[str]:
    if not country_code:
        return None
    if len(country_code) != 2:
        raise FlyteUserException("Invalid country code. Please provide a valid two-letter country code. (e.g. DE)")
    return country_code.upper()


# Inject the Perian plugin into flytekits dynamic plugin loading system
TaskPlugins.register_pythontask_plugin(PerianConfig, PerianTask)

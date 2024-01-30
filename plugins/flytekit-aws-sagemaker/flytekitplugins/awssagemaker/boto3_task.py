from dataclasses import dataclass
from typing import Any, Optional, Type, Union

from google.protobuf import json_format
from google.protobuf.struct_pb2 import Struct

from flytekit import ImageSpec
from flytekit.configuration import SerializationSettings
from flytekit.core.python_function_task import PythonInstanceTask
from flytekit.core.interface import Interface
from flytekit.extend.backend.base_agent import AsyncAgentExecutorMixin


@dataclass
class SyncBotoConfig(object):
    service: str
    method: str
    config: dict[str, Any]
    region: str


class SyncBotoTask(AsyncAgentExecutorMixin, PythonInstanceTask[SyncBotoConfig]):
    _TASK_TYPE = "sync-boto"

    def __init__(
        self,
        name: str,
        task_config: SyncBotoConfig,
        inputs: Optional[dict[str, Type]] = None,
        output_type: Optional[Type] = None,
        container_image: Optional[Union[str, ImageSpec]] = None,
        **kwargs,
    ):
        self._output_type = output_type
        super().__init__(
            name=name,
            task_config=task_config,
            task_type=self._TASK_TYPE,
            interface=Interface(inputs=inputs, outputs={"result": output_type}),
            container_image=container_image,
            **kwargs,
        )

    def get_custom(self, settings: SerializationSettings) -> dict[str, Any]:
        config = {
            "service": self.task_config.service,
            "config": self.task_config.config,
            "region": self.task_config.region,
            "method": self.task_config.method,
        }
        s = Struct()
        s.update(config)
        return json_format.MessageToDict(s)

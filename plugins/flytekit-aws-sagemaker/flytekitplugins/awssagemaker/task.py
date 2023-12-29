from dataclasses import dataclass
from typing import Any, Optional, Type

from google.protobuf import json_format
from google.protobuf.struct_pb2 import Struct

from flytekit.configuration import SerializationSettings
from flytekit.core.base_task import PythonTask
from flytekit.core.interface import Interface
from flytekit.extend.backend.base_agent import AsyncAgentExecutorMixin


@dataclass
class SagemakerEndpointConfig(object):
    config: dict[str, Any]
    region: str


class SagemakerEndpointTask(AsyncAgentExecutorMixin, PythonTask[SagemakerEndpointConfig]):
    _TASK_TYPE = "sagemaker-endpoint"

    def __init__(
        self,
        name: str,
        task_config: SagemakerEndpointConfig,
        inputs: Optional[dict[str, Type]] = None,
        **kwargs,
    ):
        super().__init__(
            name=name,
            task_config=task_config,
            interface=Interface(inputs=inputs or {}),
            task_type=self._TASK_TYPE,
            **kwargs,
        )

    def get_custom(self, settings: SerializationSettings) -> dict[str, Any]:
        config = {"config": self.task_config.config, "region": self.task_config.region}
        s = Struct()
        s.update(config)
        return json_format.MessageToDict(s)

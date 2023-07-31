import typing
from dataclasses import asdict, dataclass
from typing import Any, Dict

from google.protobuf import json_format
from google.protobuf.struct_pb2 import Struct

from flytekit.configuration import SerializationSettings
from flytekit.core.base_task import PythonTask
from flytekit.core.interface import Interface
from flytekit.extend.backend.base_agent import AsyncAgentExecutorMixin


@dataclass
class FileSensorConfig(object):
    path: str


class FileSensor(AsyncAgentExecutorMixin, PythonTask[FileSensorConfig]):
    _TASK_TYPE = "file_sensor"

    def __init__(
        self,
        path: str,
        name: str = "file_sensor",
        **kwargs,
    ):
        super().__init__(
            name=name,
            task_config=FileSensorConfig(path=path),
            interface=Interface(inputs=None),
            task_type=self._TASK_TYPE,
            **kwargs,
        )

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        s = Struct()
        s.update(asdict(typing.cast(FileSensorConfig, self.task_config)))
        return json_format.MessageToDict(s)

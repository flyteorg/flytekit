from dataclasses import dataclass
from typing import Any, Dict, Optional

from flytekitplugins.sensor.base_sensor import BaseSensor
from google.protobuf import json_format
from google.protobuf.struct_pb2 import Struct

from flytekit.configuration import SerializationSettings


@dataclass
class SensorConfig(object):
    """
    BigQueryConfig should be used to configure a BigQuery Task.
    """

    poke_interval: float = 60
    timeout: float = 60 * 60


class FileSensor(BaseSensor[SensorConfig]):
    def __init__(
        self,
        name: str,
        path: str,
        task_config: Optional[SensorConfig] = None,
        **kwargs,
    ):
        super().__init__(
            name=name,
            task_config=task_config or SensorConfig(),
            **kwargs,
        )
        self._path = path

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        config = {
            "poke_interval": self.task_config.poke_interval,
            "timeout": self.task_config.timeout,
            "path": self._path,
        }
        s = Struct()
        s.update(config)
        return json_format.MessageToDict(s)

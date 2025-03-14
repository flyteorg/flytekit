from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional, Type

from google.protobuf import json_format
from google.protobuf.struct_pb2 import Struct

from flytekit import Resources, kwtypes, logger
from flytekit.configuration import SerializationSettings
from flytekit.core.base_task import PythonTask
from flytekit.core.interface import Interface
from flytekit.extend.backend.base_connector import AsyncConnectorExecutorMixin


@dataclass
class DataServiceConfig(object):
    """DataServiceConfig should be used to configure a DataServiceTask."""

    Name: Optional[str] = None
    Requests: Optional[Resources] = None
    Limits: Optional[Resources] = None
    Port: Optional[int] = None
    Image: Optional[str] = None
    Command: Optional[List[str]] = None
    Replicas: Optional[int] = None
    ExistingReleaseName: Optional[str] = None
    Cluster: Optional[str] = None


class DataServiceTask(AsyncConnectorExecutorMixin, PythonTask[DataServiceConfig]):
    _TASK_TYPE = "dataservicetask"

    def __init__(
        self,
        name: str,
        task_config: Optional[DataServiceConfig],
        inputs: Optional[Dict[str, Type]] = None,
        **kwargs,
    ):
        super().__init__(
            name=name,
            task_config=task_config,
            interface=Interface(inputs=inputs, outputs=kwtypes(name=str)),
            task_type=self._TASK_TYPE,
            **kwargs,
        )

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        logger.info("get_custom is invoked")
        config = {}
        limits = None
        requests = None
        if self.task_config is not None:
            limits = asdict(self.task_config.Limits) if self.task_config.Limits is not None else None
            requests = asdict(self.task_config.Requests) if self.task_config.Requests is not None else None
            ge = {
                "Name": self.task_config.Name,
                "Image": self.task_config.Image,
                "Command": self.task_config.Command,
                "Port": self.task_config.Port,
                "Replicas": self.task_config.Replicas,
                "ExistingReleaseName": self.task_config.ExistingReleaseName,
                "Cluster": self.task_config.Cluster,
            }
            if limits is not None:
                ge["Limits"] = limits
            if requests is not None:
                ge["Requests"] = requests
            config = ge
        s = Struct()
        s.update(config)
        return json_format.MessageToDict(s)

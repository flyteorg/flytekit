import typing
from dataclasses import asdict, dataclass
from typing import Any, Dict, Optional, Type

from airflow.sensors.base import BaseSensorOperator
from google.protobuf import json_format
from google.protobuf.struct_pb2 import Struct

from flytekit import FlyteContextManager
from flytekit.configuration import SerializationSettings
from flytekit.core.base_task import PythonTask
from flytekit.core.interface import Interface
from flytekit.extend.backend.base_agent import AsyncAgentExecutorMixin


@dataclass
class AirflowConfig(object):
    task_module: str
    task_name: str
    task_config: typing.Dict[str, Any]


class AirflowTask(AsyncAgentExecutorMixin, PythonTask[AirflowConfig]):
    _TASK_TYPE = "airflow"

    def __init__(
        self,
        name: str,
        query_template: str,
        task_config: Optional[AirflowConfig],
        inputs: Optional[Dict[str, Type]] = None,
        original_class: Optional[Type] = None,
        **kwargs,
    ):
        super().__init__(
            name=name,
            task_config=task_config,
            query_template=query_template,
            interface=Interface(inputs=inputs or {}),
            task_type=self._TASK_TYPE,
            **kwargs,
        )
        self._original_class = original_class

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        s = Struct()
        s.update(asdict(self.task_config))
        return json_format.MessageToDict(s)


def _to_flyte_task(*args, **kwargs):
    cls = args[0]
    ctx = FlyteContextManager.current_context()
    if ctx.user_space_params._attrs.get("GET_ORIGINAL_TASK"):
        return object.__new__(cls)
    config = AirflowConfig(task_module=cls.__module__, task_name=cls.__name__, task_config=kwargs)
    t = AirflowTask(name=cls.__name__, query_template="", task_config=config, original_new=cls.__new__)
    return t()


BaseSensorOperator.__new__ = _to_flyte_task

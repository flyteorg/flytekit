import typing
from dataclasses import asdict, dataclass
from typing import Any, Dict, Optional, Type

from airflow.sensors.base import BaseSensorOperator
from google.protobuf import json_format
from google.protobuf.struct_pb2 import Struct

from flytekit import FlyteContextManager
from flytekit.configuration import SerializationSettings
from flytekit.extend import SQLTask
from flytekit.extend.backend.base_agent import AsyncAgentExecutorMixin
from flytekit.models import task as _task_model


@dataclass
class AirflowConfig(object):
    task_module: str
    task_name: str
    task_config: typing.Dict[str, Any]


class AirflowTask(AsyncAgentExecutorMixin, SQLTask[AirflowConfig]):
    # This task is executed using the BigQuery handler in the backend.
    # https://github.com/flyteorg/flyteplugins/blob/43623826fb189fa64dc4cb53e7025b517d911f22/go/tasks/plugins/webapi/bigquery/plugin.go#L34
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
            inputs=inputs,
            task_type=self._TASK_TYPE,
            **kwargs,
        )
        self._original_class = original_class

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        s = Struct()
        s.update(asdict(self.task_config))
        return json_format.MessageToDict(s)

    def get_sql(self, settings: SerializationSettings) -> Optional[_task_model.Sql]:
        # TODO: Use get_container?
        return _task_model.Sql()


def _to_flyte_task(*args, **kwargs):
    cls = args[0]
    ctx = FlyteContextManager.current_context()
    if ctx.user_space_params._attrs.get("GET_ORIGINAL_TASK"):
        return object.__new__(cls)
    config = AirflowConfig(task_module=cls.__module__, task_name=cls.__name__, task_config=kwargs)
    t = AirflowTask(name=cls.__name__, query_template="", task_config=config, original_new=cls.__new__)
    return t()


BaseSensorOperator.__new__ = _to_flyte_task

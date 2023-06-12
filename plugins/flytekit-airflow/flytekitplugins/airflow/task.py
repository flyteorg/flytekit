import types
import typing
from dataclasses import dataclass, asdict
from typing import Any, Dict, Optional, Type, Tuple

from airflow.sensors.bash import BashSensor
from google.protobuf import json_format
from google.protobuf.struct_pb2 import Struct
from airflow.sensors.base import BaseSensorOperator

from flytekit import FlyteContext
from flytekit.configuration import SerializationSettings
from flytekit.core.promise import Promise, VoidPromise
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

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        s = Struct()
        s.update(asdict(self.task_config))
        return json_format.MessageToDict(s)

    def get_sql(self, settings: SerializationSettings) -> Optional[_task_model.Sql]:
        # TODO: Use get_container?
        return _task_model.Sql()


def _to_flyte_task(*args, **kwargs) -> tuple[Promise] | Promise | VoidPromise | tuple | None:
    cls = args[0]
    config = AirflowConfig(task_module=cls.__module__, task_name=cls.__name__, task_config=kwargs)
    t = AirflowTask(name=cls.__name__, query_template="", task_config=config)
    print(f"Convert {cls.__name__} to flyte sensor...")
    return t


def translate_airflow_to_flyte(cls):
    # origin_new = BaseSensorOperator.__new__
    ctx = FlyteContext.current_context()
    # ctx.user_space_params.
    BaseSensorOperator.__new__ = _to_flyte_task


BaseSensorOperator.__new__ = _to_flyte_task


def reset_airflow_sensor():
    print("reset BaseSensorOperator...")
    del BashSensor.__new__

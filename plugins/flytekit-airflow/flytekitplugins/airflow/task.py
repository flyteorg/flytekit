import typing
from dataclasses import dataclass
from typing import Any, Dict, Optional, Type

import cloudpickle
from airflow import DAG
from airflow.models import BaseOperator
from airflow.sensors.base import BaseSensorOperator

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

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        return {"task_config_pkl": cloudpickle.dumps(self.task_config)}


def _flyte_task(*args, **kwargs):
    cls = args[0]
    ctx = FlyteContextManager.current_context()
    if ctx.user_space_params._attrs.get("GET_ORIGINAL_TASK"):
        return object.__new__(cls)
    config = AirflowConfig(task_module=cls.__module__, task_name=cls.__name__, task_config=kwargs)
    t = AirflowTask(name=cls.__name__, query_template="", task_config=config, original_new=cls.__new__)
    return t()


BaseOperator.__new__ = _flyte_task
# BaseHook.__new__ = Connection()
# BaseOperator.xcom_push = None
BaseOperator.xcom_push = lambda *args, **kwargs: None
BaseSensorOperator.dag = DAG(dag_id="dummy_dag_id")

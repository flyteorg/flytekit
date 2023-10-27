import logging
import typing
from dataclasses import dataclass
from typing import Any, Dict, Optional, Type

import jsonpickle
from airflow import DAG
from airflow.models import BaseOperator
from airflow.sensors.base import BaseSensorOperator

from flytekit import FlyteContextManager
from flytekit.configuration import SerializationSettings
from flytekit.core.base_task import PythonTask
from flytekit.core.interface import Interface
from flytekit.extend.backend.base_agent import AsyncAgentExecutorMixin


@dataclass
class AirflowObj(object):
    """
    This class is used to store the Airflow task configuration. It is serialized and stored in the Flyte task config.
    It can be trigger, hook, operator or sensor. For example:

    from airflow.sensors.filesystem import FileSensor
    sensor = FileSensor(task_id="id", filepath="/tmp/1234")

    In this case, the attributes of AirflowObj will be:
    module: airflow.sensors.filesystem
    name: FileSensor
    parameters: {"task_id": "id", "filepath": "/tmp/1234"}
    """

    module: str
    name: str
    parameters: typing.Dict[str, Any]


class AirflowTask(AsyncAgentExecutorMixin, PythonTask[AirflowObj]):
    """
    This python task is used to wrap an Airflow task. It is used to run an Airflow task in Flyte.
    The airflow task module, name and parameters are stored in the task config.
    """

    _TASK_TYPE = "airflow"

    def __init__(
        self,
        name: str,
        task_config: Optional[AirflowObj],
        inputs: Optional[Dict[str, Type]] = None,
        **kwargs,
    ):
        super().__init__(
            name=name,
            task_config=task_config,
            interface=Interface(inputs=inputs or {}),
            task_type=self._TASK_TYPE,
            **kwargs,
        )

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        # Use jsonpickle to serialize the Airflow task config since the return value should be json serializable.
        return jsonpickle.encode(self.task_config)


def _flyte_operator(*args, **kwargs):
    """
    This function is called by the Airflow operator to create a new task. We intercept this call and return a Flyte
    task instead.
    """
    cls = args[0]
    try:
        if FlyteContextManager.current_context().user_space_params.get_original_task:
            # Return original task when running in the agent.
            return object.__new__(cls)
    except AssertionError:
        logging.debug("failed to get the attribute GET_ORIGINAL_TASK from user space params")
    config = AirflowObj(module=cls.__module__, name=cls.__name__, parameters=kwargs)
    t = AirflowTask(name=kwargs["task_id"] or cls.__name__, task_config=config)
    return t()


def _flyte_xcom_push(*args, **kwargs):
    """
    This function is called by the Airflow operator to push data to XCom. We intercept this call and store the data
    in the Flyte context.
    """
    FlyteContextManager.current_context().user_space_params.xcom_data = kwargs


params = FlyteContextManager.current_context().user_space_params
params.builder().add_attr("GET_ORIGINAL_TASK", False).add_attr("XCOM_DATA", {}).build()

# Monkey patch the Airflow operator does not create a new task. Instead, it returns a Flyte task.
BaseOperator.__new__ = _flyte_operator
BaseOperator.xcom_push = _flyte_xcom_push
# Monkey patch the xcom_push method to store the data in the Flyte context.
# Create a dummy DAG to avoid Airflow errors. This DAG is not used.
# TODO: Add support using Airflow DAG in Flyte workflow. We can probably convert the Airflow DAG to a Flyte subworkflow.
BaseSensorOperator.dag = DAG(dag_id="flyte_dag")

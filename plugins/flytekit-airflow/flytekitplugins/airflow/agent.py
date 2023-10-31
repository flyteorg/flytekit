import asyncio
import typing
from dataclasses import dataclass, field
from typing import Optional

import cloudpickle
import grpc
import jsonpickle
from airflow.exceptions import AirflowException, TaskDeferred
from airflow.models import BaseOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.triggers.base import TriggerEvent
from airflow.utils.context import Context
from flyteidl.admin.agent_pb2 import (
    RETRYABLE_FAILURE,
    RUNNING,
    SUCCEEDED,
    CreateTaskResponse,
    DeleteTaskResponse,
    GetTaskResponse,
    Resource,
)
from flytekitplugins.airflow.task import AirflowObj, _get_airflow_instance

from flytekit import logger
from flytekit.exceptions.user import FlyteUserException
from flytekit.extend.backend.base_agent import AgentBase, AgentRegistry
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate


@dataclass
class ResourceMetadata:
    """
    This class is used to store the Airflow task configuration. It is serialized and returned to FlytePropeller.
    """

    airflow_operator: AirflowObj
    airflow_trigger: AirflowObj = field(default=None)
    airflow_trigger_callback: str = field(default=None)
    job_id: typing.Optional[str] = field(default=None)


class AirflowAgent(AgentBase):
    """
    It is used to run Airflow tasks. It is registered as an agent in the AgentRegistry.
    """

    def __init__(self):
        super().__init__(task_type="airflow", asynchronous=True)

    async def async_create(
        self,
        context: grpc.ServicerContext,
        output_prefix: str,
        task_template: TaskTemplate,
        inputs: Optional[LiteralMap] = None,
    ) -> CreateTaskResponse:
        airflow_obj = jsonpickle.decode(task_template.custom["task_config_pkl"])
        airflow_instance = _get_airflow_instance(airflow_obj)
        resource_meta = ResourceMetadata(airflow_operator=airflow_obj)

        if isinstance(airflow_instance, BaseOperator) and not isinstance(airflow_instance, BaseSensorOperator):
            try:
                resource_meta = ResourceMetadata(airflow_operator=airflow_obj)
                airflow_instance.execute(context=Context())
            except TaskDeferred as td:
                parameters = td.trigger.__dict__.copy()
                # Remove parameters that are in the base class
                parameters.pop("task_instance", None)
                parameters.pop("trigger_id", None)

                resource_meta.airflow_trigger = AirflowObj(
                    module=td.trigger.__module__, name=td.trigger.__class__.__name__, parameters=parameters
                )
                resource_meta.airflow_trigger_callback = td.method_name

        return CreateTaskResponse(resource_meta=cloudpickle.dumps(resource_meta))

    async def async_get(self, context: grpc.ServicerContext, resource_meta: bytes) -> GetTaskResponse:
        meta = cloudpickle.loads(resource_meta)
        airflow_operator_instance = _get_airflow_instance(meta.airflow_operator)
        airflow_trigger_instance = _get_airflow_instance(meta.airflow_trigger) if meta.airflow_trigger else None
        airflow_ctx = Context()
        message = None
        cur_state = RUNNING

        if isinstance(airflow_operator_instance, BaseSensorOperator):
            ok = airflow_operator_instance.poke(context=airflow_ctx)
            cur_state = SUCCEEDED if ok else RUNNING
        elif isinstance(airflow_operator_instance, BaseOperator):
            if airflow_trigger_instance:
                try:
                    # Airflow trigger returns immediately once the task is succeeded or failed
                    # succeeded: returns a TriggerEvent with payload
                    # failed: raises AirflowException
                    # running: runs forever, so set a default timeout (2 seconds) here.
                    event = await asyncio.wait_for(airflow_trigger_instance.run().__anext__(), 2)
                    try:
                        trigger_callback = getattr(airflow_operator_instance, meta.airflow_trigger_callback)
                        trigger_callback(context=airflow_ctx, event=typing.cast(TriggerEvent, event).payload)
                        cur_state = SUCCEEDED
                    except AirflowException as e:
                        cur_state = RETRYABLE_FAILURE
                        message = e.__str__()
                except asyncio.TimeoutError:
                    logger.debug("No event received from airflow trigger")
            else:
                airflow_operator_instance.execute(context=Context())
                cur_state = SUCCEEDED

        else:
            raise FlyteUserException("Only sensor and operator are supported.")

        return GetTaskResponse(resource=Resource(state=cur_state, message=message))

    async def async_delete(self, context: grpc.ServicerContext, resource_meta: bytes) -> DeleteTaskResponse:
        return DeleteTaskResponse()


AgentRegistry.register(AirflowAgent())

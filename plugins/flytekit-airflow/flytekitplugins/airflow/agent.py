import importlib
import typing
from dataclasses import dataclass, field
from typing import Optional

import cloudpickle
import grpc
import jsonpickle
from airflow.exceptions import AirflowException, TaskDeferred
from airflow.models import BaseOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.triggers.base import BaseTrigger
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
from flytekitplugins.airflow.task import AirflowObj

from flytekit import FlyteContextManager, logger
from flytekit.exceptions.user import FlyteUserException
from flytekit.extend.backend.base_agent import AgentBase, AgentRegistry
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate


@dataclass
class ResourceMetadata:
    airflow_operator: AirflowObj
    airflow_trigger: AirflowObj = field(default=None)
    airflow_trigger_callback: str = field(default=None)
    job_id: typing.Optional[str] = field(default=None)


def _get_airflow_instance(airflow_obj: AirflowObj) -> typing.Union[BaseOperator, BaseSensorOperator, BaseTrigger]:
    obj_module = importlib.import_module(name=airflow_obj.module)
    obj_def = getattr(obj_module, airflow_obj.name)
    # Set the GET_ORIGINAL_TASK attribute to True so that obj_def will return the original
    # airflow task instead of the Flyte task.
    ctx = FlyteContextManager.current_context()
    ctx.user_space_params.builder().add_attr("GET_ORIGINAL_TASK", True).build()
    if issubclass(obj_def, BaseOperator) and not issubclass(obj_def, BaseSensorOperator):
        try:
            return obj_def(**airflow_obj.parameters, deferrable=True)
        except AirflowException:
            logger.debug(f"Airflow operator {airflow_obj.name} does not support deferring")

    return obj_def(**airflow_obj.parameters, get_original_task=True)


class AirflowAgent(AgentBase):
    def __init__(self):
        super().__init__(task_type="airflow", asynchronous=True)

    async def async_create(
        self,
        context: grpc.ServicerContext,
        output_prefix: str,
        task_template: TaskTemplate,
        inputs: Optional[LiteralMap] = None,
    ) -> CreateTaskResponse:
        airflow_obj = jsonpickle.decode(task_template.custom)
        airflow_instance = _get_airflow_instance(airflow_obj)
        resource_meta = ResourceMetadata(airflow_operator=airflow_obj)

        if isinstance(airflow_instance, BaseOperator) and not isinstance(airflow_instance, BaseSensorOperator):
            try:
                resource_meta = ResourceMetadata(airflow_operator=airflow_obj)
                airflow_instance.execute(context=Context())
            except TaskDeferred as td:
                resource_meta.airflow_trigger = AirflowObj(
                    module=td.trigger.__module__, name=td.trigger.__name__, parameters=td.trigger.__dict__
                )
                resource_meta.airflow_trigger_callback = td.method_name

        return CreateTaskResponse(resource_meta=cloudpickle.dumps(resource_meta))

    def async_get(self, context: grpc.ServicerContext, resource_meta: bytes) -> GetTaskResponse:
        meta = cloudpickle.loads(resource_meta)
        airflow_operator_instance = _get_airflow_instance(meta.airflow_operator)
        airflow_trigger_instance = _get_airflow_instance(meta.airflow_trigger) if meta.airflow_trigger else None
        airflow_ctx = Context()
        cur_state = RUNNING
        message = None

        print("get")
        if isinstance(airflow_operator_instance, BaseSensorOperator):
            ok = airflow_operator_instance.poke(context=airflow_ctx)
            cur_state = SUCCEEDED if ok else RUNNING
        elif isinstance(airflow_operator_instance, BaseOperator):
            if airflow_trigger_instance:
                event = airflow_trigger_instance.run()
                handle_event = getattr(airflow_trigger_instance, meta.airflow_trigger_callback)
                try:
                    # TODO: Add timeout
                    handle_event(context=airflow_ctx, event=event)
                    cur_state = SUCCEEDED
                except AirflowException as e:
                    cur_state = RETRYABLE_FAILURE
                    message = e.__str__()
            else:
                airflow_operator_instance.execute(context=Context())

        else:
            raise FlyteUserException("Only sensor and operator are supported.")

        return GetTaskResponse(resource=Resource(state=cur_state, message=message))

    def async_delete(self, context: grpc.ServicerContext, resource_meta: bytes) -> DeleteTaskResponse:
        return DeleteTaskResponse()


AgentRegistry.register(AirflowAgent())

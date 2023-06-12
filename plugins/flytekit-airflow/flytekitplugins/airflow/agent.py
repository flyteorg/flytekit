import importlib
from typing import Optional

import grpc
import msgpack
from airflow.sensors.base import BaseSensorOperator
from flyteidl.admin.agent_pb2 import SUCCEEDED, CreateTaskResponse, DeleteTaskResponse, GetTaskResponse, Resource, \
    RUNNING

from flytekit.extend.backend.base_agent import AgentBase, AgentRegistry
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate
from flytekitplugins.airflow.task import AirflowConfig, reset_airflow_sensor
from airflow.utils.context import Context


class AirflowAgent(AgentBase):
    def __init__(self):
        super().__init__(task_type="airflow")

    def create(
        self,
        context: grpc.ServicerContext,
        output_prefix: str,
        task_template: TaskTemplate,
        inputs: Optional[LiteralMap] = None,
    ) -> CreateTaskResponse:
        return CreateTaskResponse(resource_meta=msgpack.packb(task_template.custom))

    def get(self, context: grpc.ServicerContext, resource_meta: bytes) -> GetTaskResponse:
        print("get status from airflow agent")
        cfg = AirflowConfig(**msgpack.unpackb(resource_meta))
        # reset_airflow_sensor()
        task_module = importlib.import_module(name=cfg.task_module)
        task_def = getattr(task_module, cfg.task_name)
        sensor = task_def(**cfg.task_config)
        res = sensor.poke(Context())
        print("res: ", res)
        cur_state = RUNNING
        if res:
            cur_state = SUCCEEDED
        return GetTaskResponse(resource=Resource(state=cur_state, outputs=None))

    def delete(self, context: grpc.ServicerContext, resource_meta: bytes) -> DeleteTaskResponse:
        # Do Nothing
        return DeleteTaskResponse()


AgentRegistry.register(AirflowAgent())

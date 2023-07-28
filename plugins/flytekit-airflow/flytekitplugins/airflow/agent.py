import codecs
import importlib
import pickle
from typing import Optional

import cloudpickle
import grpc
import msgpack
from airflow.models import TaskInstance
from airflow.sensors.base import BaseSensorOperator
from airflow.serialization.pydantic.taskinstance import TaskInstancePydantic
from airflow.utils.context import Context
from flyteidl.admin.agent_pb2 import (
    PERMANENT_FAILURE,
    RUNNING,
    SUCCEEDED,
    CreateTaskResponse,
    DeleteTaskResponse,
    GetTaskResponse,
    Resource,
)
from flytekitplugins.airflow.task import AirflowConfig

from flytekit import FlyteContextManager, logger
from flytekit.extend.backend.base_agent import AgentBase, AgentRegistry
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate


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
        meta = msgpack.unpackb(resource_meta)
        if meta.get("task_config_pkl"):
            meta = cloudpickle.loads(codecs.decode(meta.get("task_config_pkl").encode(), "base64"))

        cfg = AirflowConfig(**meta)
        task_module = importlib.import_module(name=cfg.task_module)
        task_def = getattr(task_module, cfg.task_name)
        ctx = FlyteContextManager.current_context()
        ctx.user_space_params._attrs["GET_ORIGINAL_TASK"] = True
        config = pickle.loads(cfg.task_config)
        task = task_def(**config)
        try:
            if issubclass(type(task), BaseSensorOperator):
                res = task.poke(context=Context())
            else:
                res = task.execute(context=Context())
            if res:
                cur_state = SUCCEEDED
            else:
                cur_state = RUNNING
        except Exception as e:
            print(e)
            logger.error(e.__str__())
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(e.__str__())
            return GetTaskResponse(resource=Resource(state=PERMANENT_FAILURE))
        return GetTaskResponse(resource=Resource(state=cur_state, outputs=None))

    def delete(self, context: grpc.ServicerContext, resource_meta: bytes) -> DeleteTaskResponse:
        # Do Nothing
        return DeleteTaskResponse()


AgentRegistry.register(AirflowAgent())

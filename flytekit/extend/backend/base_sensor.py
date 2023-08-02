import typing
from typing import Optional

import cloudpickle
import grpc
from flyteidl.admin.agent_pb2 import (
    RUNNING,
    SUCCEEDED,
    CreateTaskResponse,
    DeleteTaskResponse,
    GetTaskResponse,
    Resource,
)

from flytekit.extend.backend.base_agent import AgentBase
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate

T = typing.TypeVar("T")


class SensorBase(AgentBase):
    def __init__(self, task_type: str):
        super().__init__(task_type=task_type, asynchronous=True)

    async def poke(self, metadata: T) -> bool:
        # Sensor will use this method to check if the condition is met
        # For example:
        # - Check if the file exists
        # - Check if the file is modified
        # - Check if the workflow is completed or failed
        raise NotImplementedError

    async def extract(
        self, context: grpc.ServicerContext, task_template: TaskTemplate, inputs: Optional[LiteralMap] = None
    ) -> T:
        # Sensor will use this method to extract the metadata from the task template
        raise NotImplementedError

    async def async_create(
        self,
        context: grpc.ServicerContext,
        output_prefix: str,
        task_template: TaskTemplate,
        inputs: Optional[LiteralMap] = None,
    ) -> CreateTaskResponse:
        metadata = await self.extract(context, task_template, inputs)
        return CreateTaskResponse(resource_meta=cloudpickle.dumps(metadata))

    async def async_get(self, context: grpc.ServicerContext, resource_meta: bytes) -> GetTaskResponse:
        meta = cloudpickle.loads(resource_meta)
        cur_state = SUCCEEDED if await self.poke(meta) else RUNNING
        return GetTaskResponse(resource=Resource(state=cur_state, outputs=None))

    async def async_delete(self, context: grpc.ServicerContext, resource_meta: bytes) -> DeleteTaskResponse:
        return DeleteTaskResponse()

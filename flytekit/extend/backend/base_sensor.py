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

from flytekit.core.sensor_task import FileSensorConfig
from flytekit.extend.backend.base_agent import AgentBase
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate


class SensorBase(AgentBase):
    def __init__(self, task_type: str):
        super().__init__(task_type=task_type, asynchronous=True)

    async def poke(self, path: str) -> bool:
        # Sensor will use this method to check if the condition is met
        # - Check if the file exists
        # - Check if the file is modified
        # - Check if the workflow is completed or failed
        raise NotImplementedError

    async def async_create(
        self,
        context: grpc.ServicerContext,
        output_prefix: str,
        task_template: TaskTemplate,
        inputs: Optional[LiteralMap] = None,
    ) -> CreateTaskResponse:
        return CreateTaskResponse(resource_meta=cloudpickle.dumps(task_template.custom))

    async def async_get(self, context: grpc.ServicerContext, resource_meta: bytes) -> GetTaskResponse:
        meta = cloudpickle.loads(resource_meta)
        path = FileSensorConfig(**meta).path
        cur_state = SUCCEEDED if await self.poke(path) else RUNNING
        return GetTaskResponse(resource=Resource(state=cur_state, outputs=None))

    async def async_delete(self, context: grpc.ServicerContext, resource_meta: bytes) -> DeleteTaskResponse:
        return DeleteTaskResponse()

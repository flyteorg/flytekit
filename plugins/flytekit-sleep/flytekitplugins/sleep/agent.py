import json
from asyncio import sleep
from dataclasses import asdict, dataclass
from typing import Optional

import grpc
from flyteidl.admin.agent_pb2 import SUCCEEDED, CreateTaskResponse, DeleteTaskResponse, GetTaskResponse, Resource
from flytekit.extend.backend.base_agent import AgentBase, AgentRegistry
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate


@dataclass
class Metadata:
    job_id: str


class SleepAgent(AgentBase):
    def __init__(self):
        super().__init__(task_type="sleep", asynchronous=True)

    async def async_create(
        self,
        context: grpc.ServicerContext,
        output_prefix: str,
        task_template: TaskTemplate,
        inputs: Optional[LiteralMap] = None,
    ) -> CreateTaskResponse:
        print("creating...")
        await sleep(2)
        print("creating done")
        return CreateTaskResponse(
            resource_meta=json.dumps(asdict(Metadata(job_id="job_id"))).encode("utf-8")
        )

    async def async_get(self, context: grpc.ServicerContext, resource_meta: bytes) -> GetTaskResponse:
        print("getting...")
        await sleep(2)
        print("getting done...")
        return GetTaskResponse(resource=Resource(state=SUCCEEDED))

    async def async_delete(self, context: grpc.ServicerContext, resource_meta: bytes) -> DeleteTaskResponse:
        print("deleting...")
        await sleep(2)
        print("deleting done...")
        return DeleteTaskResponse()


AgentRegistry.register(SleepAgent())

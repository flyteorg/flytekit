from dataclasses import dataclass
from typing import Optional

import cloudpickle
import grpc
from flyteidl.admin.agent_pb2 import SUCCEEDED, CreateTaskResponse, DeleteTaskResponse, GetTaskResponse, Resource

from flytekit.extend.backend.base_agent import AgentBase, AgentRegistry
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate


@dataclass
class Metadata:
    job_id: str


class MemvergeAgent(AgentBase):
    def __init__(self):
        super().__init__(task_type="memverge_task")

    def create(
        self,
        context: grpc.ServicerContext,
        output_prefix: str,
        task_template: TaskTemplate,
        inputs: Optional[LiteralMap] = None,
    ) -> CreateTaskResponse:
        print("task template", task_template)
        print("container args", task_template.container.args)
        print("container image", task_template.container.image)

        job_id = "memverge_job_id"
        return CreateTaskResponse(resource_meta=cloudpickle.dumps(job_id))

    def get(self, context: grpc.ServicerContext, resource_meta: bytes) -> GetTaskResponse:
        # Get job status
        job_id = cloudpickle.loads(resource_meta)
        # float get status --id job_id
        return GetTaskResponse(resource=Resource(state=SUCCEEDED))

    def delete(self, context: grpc.ServicerContext, resource_meta: bytes) -> DeleteTaskResponse:
        return DeleteTaskResponse()


AgentRegistry.register(MemvergeAgent())

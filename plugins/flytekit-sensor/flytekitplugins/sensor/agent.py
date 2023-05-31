import typing

import grpc
from flyteidl.admin.task_pb2 import TaskCreateResponse
from flyteidl.service.agent_service_pb2 import RUNNING, SUCCEEDED, TaskDeleteResponse, TaskGetResponse

from flytekit import FlyteContextManager
from flytekit.extend.backend.base_agent import AgentBase
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate


class FileAgent(AgentBase):
    def __init__(self, path: str, **kwargs):
        super().__init__(**kwargs)
        self._path = path

    def create(
        self,
        context: grpc.ServicerContext,
        output_prefix: str,
        task_template: TaskTemplate,
        inputs: typing.Optional[LiteralMap] = None,
    ) -> TaskCreateResponse:
        pass

    def get(self, context: grpc.ServicerContext, job_id: str) -> TaskGetResponse:
        ctx = FlyteContextManager.current_context()
        is_exist = ctx.file_access.exists(self._path)
        if not is_exist:
            return TaskGetResponse(state=RUNNING)
        return TaskGetResponse(state=SUCCEEDED)

    def delete(self, context: grpc.ServicerContext, job_id: str) -> TaskDeleteResponse:
        # Do nothing
        pass

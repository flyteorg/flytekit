import typing

import grpc
from flyteidl.admin.agent_pb2 import DoTaskResponse
from flytekit.core.external_api_task import TASK_TYPE

from flytekit.extend.backend.base_agent import AgentBase, AgentRegistry
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate

T = typing.TypeVar("T")


class SyncAgentBase(AgentBase):
    """
    TaskExecutor is an agent responsible for executing external API tasks.

    This class is meant to be subclassed when implementing plugins that require
    an external API to perform the task execution. It provides a routing mechanism
    to direct the task to the appropriate handler based on the task's specifications.
    """

    def __init__(self):
        super().__init__(task_type=TASK_TYPE, asynchronous=True)

    async def async_do(
        self,
        context: grpc.ServicerContext,
        output_prefix: str,
        task_template: TaskTemplate,
        inputs: typing.Optional[LiteralMap] = None,
    ) -> DoTaskResponse:
       pass


AgentRegistry.register(TaskExecutor())
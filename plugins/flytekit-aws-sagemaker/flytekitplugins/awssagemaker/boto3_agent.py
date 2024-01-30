from typing import Optional

import grpc
from flyteidl.admin.agent_pb2 import SUCCEEDED, CreateTaskResponse, Resource
from flyteidl.core.tasks_pb2 import TaskTemplate
from flytekit import FlyteContextManager
from flytekit.core.type_engine import TypeEngine
from flytekit.extend.backend.base_agent import (
    AgentBase,
    AgentRegistry,
    get_agent_secret,
)
from flytekit.models.literals import LiteralMap
import asyncio

from .boto3_mixin import Boto3AgentMixin

TIMEOUT_SECONDS = 20


class SyncBotoAgent(AgentBase):
    """A general purpose boto3 agent that can be used to call any boto3 method synchronously."""

    def __init__(self):
        super().__init__(
            task_type="sync-boto",
            asynchronous=False,
        )

    async def create(
        self,
        context: grpc.ServicerContext,
        output_prefix: str,
        task_template: TaskTemplate,
        inputs: Optional[LiteralMap] = None,
    ) -> CreateTaskResponse:
        custom = task_template.custom
        service = custom["service"]
        config = custom["config"]
        region = custom["region"]
        method = custom["method"]

        boto3_object = Boto3AgentMixin(service=service, region=region)
        result = await asyncio.wait_for(
            boto3_object._call(
                method=method,
                config=config,
                container=task_template.container,
                inputs=inputs,
                aws_access_key_id=get_agent_secret(secret_key="AWS_ACCESS_KEY"),
                aws_secret_access_key=get_agent_secret(secret_key="AWS_SECRET_ACCESS_KEY"),
                aws_session_token=get_agent_secret(secret_key="AWS_SESSION_TOKEN"),
            ),
            timeout=TIMEOUT_SECONDS,
        )

        outputs = None
        if result:
            ctx = FlyteContextManager.current_context()
            outputs = LiteralMap(
                {
                    "o0": TypeEngine.to_literal(
                        ctx,
                        result,
                        type(result),
                        TypeEngine.to_literal_type(type(result)),
                    )
                }
            ).to_flyte_idl()

        return CreateTaskResponse(resource=Resource(state=SUCCEEDED, outputs=outputs))


AgentRegistry.register(SyncBotoAgent())

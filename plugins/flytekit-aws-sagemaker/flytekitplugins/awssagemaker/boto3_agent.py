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

from .boto3_mixin import Boto3AgentMixin


class BotoAgent(AgentBase):
    """A general purpose boto3 agent that can be used to call any boto3 method."""

    def __init__(self):
        super().__init__(task_type="sync-boto")

    async def async_create(
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
        output_type = custom["output_type"]

        boto3_object = Boto3AgentMixin(service=service, region=region)
        result = await boto3_object._call(
            method=method,
            config=config,
            container=task_template.container,
            inputs=inputs,
            aws_access_key_id=get_agent_secret(secret_key="AWS_ACCESS_KEY"),
            aws_secret_access_key=get_agent_secret(secret_key="AWS_SECRET_ACCESS_KEY"),
            aws_session_token=get_agent_secret(secret_key="AWS_SESSION_TOKEN"),
        )

        outputs = None
        if result:
            ctx = FlyteContextManager.current_context()
            outputs = LiteralMap(
                {
                    "result": TypeEngine.to_literal(
                        ctx,
                        result,
                        output_type,
                        TypeEngine.to_literal_type(output_type),
                    )
                }
            ).to_flyte_idl()

        return CreateTaskResponse(resource=Resource(state=SUCCEEDED, outputs=outputs))


AgentRegistry.register(BotoAgent())

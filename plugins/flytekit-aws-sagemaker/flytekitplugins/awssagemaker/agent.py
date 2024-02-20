import json
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Optional

import grpc
from flyteidl.admin.agent_pb2 import (
    CreateTaskResponse,
    DeleteTaskResponse,
    GetTaskResponse,
    Resource,
)
from flyteidl.core.tasks_pb2 import TaskTemplate

from flytekit import FlyteContextManager
from flytekit.core.type_engine import TypeEngine
from flytekit.extend.backend.base_agent import (
    AgentBase,
    AgentRegistry,
    convert_to_flyte_phase,
    get_agent_secret,
)
from flytekit.models.literals import LiteralMap

from .boto3_mixin import Boto3AgentMixin

states = {
    "Creating": "Running",
    "InService": "Success",
    "Failed": "Failed",
}


class DateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()

        return json.JSONEncoder.default(self, o)


@dataclass
class Metadata:
    endpoint_name: str
    region: str


class SagemakerEndpointAgent(Boto3AgentMixin, AgentBase):
    """This agent creates an endpoint."""

    def __init__(self):
        super().__init__(service="sagemaker", task_type="sagemaker-endpoint")

    async def async_create(
        self,
        context: grpc.ServicerContext,
        output_prefix: str,
        task_template: TaskTemplate,
        inputs: Optional[LiteralMap] = None,
    ) -> CreateTaskResponse:
        custom = task_template.custom
        config = custom["config"]
        region = custom["region"]

        await self._call(
            method="create_endpoint",
            config=config,
            inputs=inputs,
            region=region,
            aws_access_key_id=get_agent_secret(secret_key="aws-access-key"),
            aws_secret_access_key=get_agent_secret(secret_key="aws-secret-access-key"),
            aws_session_token=get_agent_secret(secret_key="aws-session-token"),
        )

        metadata = Metadata(endpoint_name=config["EndpointName"], region=region)
        return CreateTaskResponse(resource_meta=json.dumps(asdict(metadata)).encode("utf-8"))

    async def async_get(self, context: grpc.ServicerContext, resource_meta: bytes) -> GetTaskResponse:
        metadata = Metadata(**json.loads(resource_meta.decode("utf-8")))

        endpoint_status = await self._call(
            method="describe_endpoint",
            config={"EndpointName": metadata.endpoint_name},
            region=metadata.region,
            aws_access_key_id=get_agent_secret(secret_key="aws-access-key"),
            aws_secret_access_key=get_agent_secret(secret_key="aws-secret-access-key"),
            aws_session_token=get_agent_secret(secret_key="aws-session-token"),
        )

        current_state = endpoint_status.get("EndpointStatus")
        flyte_state = convert_to_flyte_phase(states[current_state])

        message = None
        if current_state == "Failed":
            message = endpoint_status.get("FailureReason")

        res = None
        if current_state == "InService":
            ctx = FlyteContextManager.current_context()
            res = LiteralMap(
                {
                    "result": TypeEngine.to_literal(
                        ctx,
                        json.dumps(endpoint_status, cls=DateTimeEncoder),
                        str,
                        TypeEngine.to_literal_type(str),
                    )
                }
            ).to_flyte_idl()

        return GetTaskResponse(resource=Resource(state=flyte_state, outputs=res, message=message))

    async def async_delete(self, context: grpc.ServicerContext, resource_meta: bytes) -> DeleteTaskResponse:
        metadata = Metadata(**json.loads(resource_meta.decode("utf-8")))

        await self._call(
            "delete_endpoint",
            config={"EndpointName": metadata.endpoint_name},
            region=metadata.region,
            aws_access_key_id=get_agent_secret(secret_key="aws-access-key"),
            aws_secret_access_key=get_agent_secret(secret_key="aws-secret-access-key"),
            aws_session_token=get_agent_secret(secret_key="aws-session-token"),
        )

        return DeleteTaskResponse()


AgentRegistry.register(SagemakerEndpointAgent())

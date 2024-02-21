import json
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from flytekit import FlyteContextManager
from flytekit.core.type_engine import TypeEngine
from flytekit.extend.backend.base_agent import (
    AgentRegistry,
    AsyncAgentBase,
    Resource,
    ResourceMeta,
)
from flytekit.extend.backend.utils import convert_to_flyte_phase, get_agent_secret
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate

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
class SageMakerEndpointMetadata(ResourceMeta):
    endpoint_name: str
    region: str


class SageMakerEndpointAgent(Boto3AgentMixin, AsyncAgentBase):
    """This agent creates an endpoint."""

    def __init__(self):
        super().__init__(
            service="sagemaker",
            task_type_name="sagemaker-endpoint",
            metadata_type=SageMakerEndpointMetadata,
        )

    async def create(
        self, task_template: TaskTemplate, inputs: Optional[LiteralMap] = None, **kwargs
    ) -> SageMakerEndpointMetadata:
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

        return SageMakerEndpointMetadata(endpoint_name=config["EndpointName"], region=region)

    async def get(self, metadata: SageMakerEndpointMetadata, **kwargs) -> Resource:
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

        return Resource(phase=flyte_state, outputs=res, message=message)

    async def delete(self, metadata: SageMakerEndpointMetadata, **kwargs):
        await self._call(
            "delete_endpoint",
            config={"EndpointName": metadata.endpoint_name},
            region=metadata.region,
            aws_access_key_id=get_agent_secret(secret_key="aws-access-key"),
            aws_secret_access_key=get_agent_secret(secret_key="aws-secret-access-key"),
            aws_session_token=get_agent_secret(secret_key="aws-session-token"),
        )


AgentRegistry.register(SageMakerEndpointAgent())

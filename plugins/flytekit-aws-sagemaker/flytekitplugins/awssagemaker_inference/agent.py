import json
from datetime import datetime
from typing import Optional

from flytekit.extend.backend.base_agent import (
    AgentRegistry,
    AsyncAgentBase,
    Resource,
)
from flytekit.extend.backend.utils import convert_to_flyte_phase, get_agent_secret
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate

from .boto3_mixin import Boto3AgentMixin
from .task import SageMakerEndpointMetadata

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


class SageMakerEndpointAgent(Boto3AgentMixin, AsyncAgentBase):
    """This agent creates an endpoint."""

    name = "SageMaker Endpoint Agent"

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
        config = custom.get("config")
        region = custom.get("region")

        await self._call(
            method="create_endpoint",
            config=config,
            inputs=inputs,
            region=region,
            aws_access_key_id=get_agent_secret(secret_key="aws-access-key"),
            aws_secret_access_key=get_agent_secret(secret_key="aws-secret-access-key"),
            aws_session_token=get_agent_secret(secret_key="aws-session-token"),
        )

        return SageMakerEndpointMetadata(config=config, region=region, inputs=inputs)

    async def get(self, resource_meta: SageMakerEndpointMetadata, **kwargs) -> Resource:
        endpoint_status = await self._call(
            method="describe_endpoint",
            config={"EndpointName": resource_meta.config.get("EndpointName")},
            inputs=resource_meta.inputs,
            region=resource_meta.region,
            aws_access_key_id=get_agent_secret(secret_key="aws-access-key"),
            aws_secret_access_key=get_agent_secret(secret_key="aws-secret-access-key"),
            aws_session_token=get_agent_secret(secret_key="aws-session-token"),
        )

        current_state = endpoint_status.get("EndpointStatus")
        flyte_phase = convert_to_flyte_phase(states[current_state])

        message = None
        if current_state == "Failed":
            message = endpoint_status.get("FailureReason")

        res = None
        if current_state == "InService":
            res = {"result": json.dumps(endpoint_status, cls=DateTimeEncoder)}

        return Resource(phase=flyte_phase, outputs=res, message=message)

    async def delete(self, resource_meta: SageMakerEndpointMetadata, **kwargs):
        await self._call(
            "delete_endpoint",
            config={"EndpointName": resource_meta.config.get("EndpointName")},
            region=resource_meta.region,
            inputs=resource_meta.inputs,
            aws_access_key_id=get_agent_secret(secret_key="aws-access-key"),
            aws_secret_access_key=get_agent_secret(secret_key="aws-secret-access-key"),
            aws_session_token=get_agent_secret(secret_key="aws-session-token"),
        )


AgentRegistry.register(SageMakerEndpointAgent())

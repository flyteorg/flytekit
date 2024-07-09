import re
from dataclasses import dataclass
from typing import Any, Dict, Optional

import cloudpickle
from flyteidl.core.execution_pb2 import TaskExecution

from flytekit.extend.backend.base_agent import (
    AgentRegistry,
    AsyncAgentBase,
    Resource,
    ResourceMeta,
)
from flytekit.extend.backend.utils import convert_to_flyte_phase
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate

from .boto3_mixin import Boto3AgentMixin, IdempotenceTokenException


@dataclass
class SageMakerEndpointMetadata(ResourceMeta):
    config: Dict[str, Any]
    region: Optional[str] = None
    inputs: Optional[LiteralMap] = None

    def encode(self) -> bytes:
        return cloudpickle.dumps(self)

    @classmethod
    def decode(cls, data: bytes) -> "SageMakerEndpointMetadata":
        return cloudpickle.loads(data)


states = {
    "Creating": "Running",
    "InService": "Success",
    "Failed": "Failed",
}


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
        )

        return SageMakerEndpointMetadata(config=config, region=region, inputs=inputs)

    async def get(self, resource_meta: SageMakerEndpointMetadata, **kwargs) -> Resource:
        try:
            endpoint_status, idempotence_token = await self._call(
                method="describe_endpoint",
                config={"EndpointName": resource_meta.config.get("EndpointName")},
                inputs=resource_meta.inputs,
                region=resource_meta.region,
            )
        except IdempotenceTokenException as e:
            error_code = e.response["Error"]["Code"]
            error_message = e.response["Error"]["Message"]

            if error_code == "ValidationException" and "Cannot create already existing" in error_message:
                arn = re.search(r"arn:aws:sagemaker:[^ ]+", error_message).group(0)
                if arn:
                    return Resource(
                        phase=TaskExecution.SUCCEEDED,
                        outputs={
                            "result": f"Entity already exists: {arn}",
                            "idempotence_token": "",
                        },
                    )
                else:
                    return Resource(
                        phase=TaskExecution.SUCCEEDED,
                        outputs={
                            "result": "Entity already exists.",
                            "idempotence_token": "",
                        },
                    )
            else:
                # Re-raise the exception if it's not the specific error we're handling
                print(f"An unexpected error occurred: {e}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")

        current_state = endpoint_status.get("EndpointStatus")
        flyte_phase = convert_to_flyte_phase(states[current_state])

        message = None
        if current_state == "Failed":
            message = endpoint_status.get("FailureReason")

        res = None
        if current_state == "InService":
            res = {
                "result": {"EndpointArn": endpoint_status.get("EndpointArn")},
                "idempotence_token": idempotence_token,
            }

        return Resource(phase=flyte_phase, outputs=res, message=message)

    async def delete(self, resource_meta: SageMakerEndpointMetadata, **kwargs):
        await self._call(
            "delete_endpoint",
            config={"EndpointName": resource_meta.config.get("EndpointName")},
            region=resource_meta.region,
            inputs=resource_meta.inputs,
        )


AgentRegistry.register(SageMakerEndpointAgent())

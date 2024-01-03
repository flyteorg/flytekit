import json
from dataclasses import asdict, dataclass
from typing import Optional, Any, Union, Type

import grpc
from flyteidl.admin.agent_pb2 import (
    CreateTaskResponse,
    DeleteTaskResponse,
    GetTaskResponse,
    Resource,
)
from flyteidl.core.tasks_pb2 import TaskTemplate

from flytekit.extend.backend.base_agent import (
    AgentBase,
    convert_to_flyte_state,
    get_agent_secret,
)
from flytekit.models.literals import LiteralMap
from flytekit import ImageSpec

from .boto3.mixin import Boto3AgentMixin
from .boto3.agent import SyncBotoAgentTask


@dataclass
class Metadata:
    endpoint_name: str
    region: str


class SagemakerEndpointAgent(Boto3AgentMixin, AgentBase):
    """This agent creates an endpoint."""

    def __init__(self):
        super().__init__(
            service="sagemaker",
            task_type="sagemaker-endpoint",
        )

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
            "create_endpoint",
            config=config,
            task_template=task_template,
            inputs=inputs,
            region=region,
            aws_access_key_id=get_agent_secret(secret_key="AWS_ACCESS_KEY"),
            aws_secret_access_key=get_agent_secret(secret_key="AWS_SECRET_ACCESS_KEY"),
            aws_session_token=get_agent_secret(secret_key="AWS_SESSION_TOKEN"),
        )

        metadata = Metadata(endpoint_name=config["EndpointName"], region=region)
        return CreateTaskResponse(resource_meta=json.dumps(asdict(metadata)).encode("utf-8"))

    async def async_get(self, context: grpc.ServicerContext, resource_meta: bytes) -> GetTaskResponse:
        metadata = Metadata(**json.loads(resource_meta.decode("utf-8")))

        endpoint_status = await self._call(
            "describe_endpoint",
            config={"EndpointName": metadata.endpoint_name},
        )

        current_state = endpoint_status.get("EndpointStatus")
        message = ""
        if current_state in ("Failed", "UpdateRollbackFailed"):
            message = endpoint_status.get("FailureReason")

        # THIS WON'T WORK. NEED TO FIX THIS.
        flyte_state = convert_to_flyte_state(current_state)

        return GetTaskResponse(resource=Resource(state=flyte_state, message=message))

    async def async_delete(self, context: grpc.ServicerContext, resource_meta: bytes) -> DeleteTaskResponse:
        metadata = Metadata(**json.loads(resource_meta.decode("utf-8")))

        await self._call(
            "delete_endpoint",
            config={"EndpointName": metadata.endpoint_name},
        )

        return DeleteTaskResponse()


class SagemakerModelTask(SyncBotoAgentTask):
    def __init__(
        self,
        name: str,
        config: dict[str, Any],
        region: Optional[str] = None,
        container_image: Optional[Union[str, ImageSpec]] = None,
        **kwargs,
    ):
        super(SagemakerModelTask, self).__init__(
            service="sagemaker",
            method="create_model",
            region=region,
            name=name,
            config=config,
            container_image=container_image,
            **kwargs,
        )


class SagemakerEndpointConfigTask(SyncBotoAgentTask):
    def __init__(self, name: str, config: dict[str, Any], region: Optional[str] = None, **kwargs):
        super(SagemakerEndpointConfigTask, self).__init__(
            service="sagemaker",
            method="create_endpoint_config",
            region=region,
            name=name,
            config=config,
            **kwargs,
        )


class SagemakerDeleteEndpointTask(SyncBotoAgentTask):
    def __init__(self, name: str, config: dict[str, Any], region: Optional[str] = None, **kwargs):
        super(SagemakerDeleteEndpointTask, self).__init__(
            service="sagemaker",
            method="delete_endpoint",
            region=region,
            name=name,
            config=config,
            **kwargs,
        )


class SagemakerDeleteEndpointConfigTask(SyncBotoAgentTask):
    def __init__(self, name: str, config: dict[str, Any], region: Optional[str] = None, **kwargs):
        super(SagemakerDeleteEndpointConfigTask, self).__init__(
            service="sagemaker",
            method="delete_endpoint_config",
            region=region,
            name=name,
            config=config,
            **kwargs,
        )


class SagemakerDeleteModelTask(SyncBotoAgentTask):
    def __init__(self, name: str, config: dict[str, Any], region: Optional[str] = None, **kwargs):
        super(SagemakerDeleteModelTask, self).__init__(
            service="sagemaker",
            method="delete_model",
            region=region,
            name=name,
            config=config,
            **kwargs,
        )


class SagemakerInvokeEndpointTask(SyncBotoAgentTask):
    def __init__(self, name: str, config: dict[str, Any], region: Optional[str] = None, **kwargs):
        super(SagemakerInvokeEndpointTask, self).__init__(
            service="sagemaker-runtime",
            method="invoke_endpoint_async",
            region=region,
            name=name,
            config=config,
            **kwargs,
        )

    def do(
        self,
        output_result_type: Type,
        **kwargs,
    ):
        super(SagemakerInvokeEndpointTask, self).do(
            output_result_type=dict[str, Union[str, output_result_type]],
            **kwargs,
        )

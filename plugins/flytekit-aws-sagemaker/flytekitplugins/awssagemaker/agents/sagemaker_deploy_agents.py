import json
from dataclasses import asdict, dataclass
from typing import Any, Optional, Type

import grpc
from flyteidl.admin.agent_pb2 import (
    SUCCEEDED,
    CreateTaskResponse,
    DeleteTaskResponse,
    GetTaskResponse,
    Resource,
)
from flyteidl.core.tasks_pb2 import TaskTemplate

from flytekit import FlyteContextManager
from flytekit.core.external_api_task import ExternalApiTask
from flytekit.core.type_engine import TypeEngine
from flytekit.extend.backend.base_agent import (
    AgentBase,
    convert_to_flyte_state,
    get_agent_secret,
    AgentRegistry,
)
from flytekit.models.literals import LiteralMap

from .boto3_mixin import Boto3AgentMixin


@dataclass
class Metadata:
    endpoint_name: str
    region: str


class SagemakerModelTask(Boto3AgentMixin, ExternalApiTask):
    """This agent creates a Sagemaker model."""

    def __init__(self, name: str, config: dict[str, Any], region: Optional[str] = None, **kwargs):
        super().__init__(service="sagemaker-runtime", region=region, name=name, config=config, **kwargs)

    def do(
        self,
        task_template: TaskTemplate,
        inputs: Optional[LiteralMap] = None,
        additional_args: Optional[dict[str, Any]] = None,
    ) -> CreateTaskResponse:
        inputs = inputs or LiteralMap(literals={})

        result = self._call(
            method="create_model",
            config=task_template.custom["task_config"],
            inputs=inputs,
            task_template=task_template,
            additional_args=additional_args,
            aws_access_key_id=get_agent_secret(secret_key="AWS_ACCESS_KEY"),
            aws_secret_access_key=get_agent_secret(secret_key="AWS_SECRET_ACCESS_KEY"),
            aws_session_token=get_agent_secret(secret_key="AWS_SESSION_TOKEN"),
        )

        ctx = FlyteContextManager.current_context()
        outputs = LiteralMap(
            {
                "o0": TypeEngine.to_literal(
                    ctx,
                    result,
                    dict[str, str],
                    TypeEngine.to_literal_type(dict[str, str]),
                )
            }
        ).to_flyte_idl()
        return CreateTaskResponse(resource=Resource(state=SUCCEEDED, outputs=outputs))


class SagemakerEndpointConfigTask(Boto3AgentMixin, ExternalApiTask):
    """This agent creates an endpoint config."""

    def __init__(self, name: str, config: dict[str, Any], region: Optional[str] = None, **kwargs):
        super().__init__(service="sagemaker-runtime", region=region, name=name, config=config, **kwargs)

    def do(
        self,
        task_template: TaskTemplate,
        inputs: Optional[LiteralMap] = None,
        additional_args: Optional[dict[str, Any]] = None,
    ) -> CreateTaskResponse:
        inputs = inputs or LiteralMap(literals={})

        result = self._call(
            method="create_endpoint_config",
            inputs=inputs,
            config=task_template.custom["task_config"],
            additional_args=additional_args,
            aws_access_key_id=get_agent_secret(secret_key="AWS_ACCESS_KEY"),
            aws_secret_access_key=get_agent_secret(secret_key="AWS_SECRET_ACCESS_KEY"),
            aws_session_token=get_agent_secret(secret_key="AWS_SESSION_TOKEN"),
        )

        ctx = FlyteContextManager.current_context()
        outputs = LiteralMap(
            {
                "o0": TypeEngine.to_literal(
                    ctx,
                    result,
                    dict[str, str],
                    TypeEngine.to_literal_type(dict[str, str]),
                )
            }
        ).to_flyte_idl()
        return CreateTaskResponse(resource=Resource(state=SUCCEEDED, outputs=outputs))


class SagemakerEndpointAgent(Boto3AgentMixin, AgentBase):
    """This agent creates an endpoint."""

    def __init__(self, region: str):
        super().__init__(
            service="sagemaker-runtime",
            region=region,
            task_type="sagemaker-endpoint",
            asynchronous=True,
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


class SagemakerInvokeEndpointTask(Boto3AgentMixin, ExternalApiTask):
    """This agent invokes an endpoint."""

    def __init__(self, name: str, config: dict[str, Any], region: Optional[str] = None, **kwargs):
        super().__init__(service="sagemaker-runtime", region=region, name=name, config=config, **kwargs)

    def do(
        self,
        task_template: TaskTemplate,
        output_result_type: Type,
        inputs: Optional[LiteralMap] = None,
        additional_args: Optional[dict[str, Any]] = None,
    ) -> CreateTaskResponse:
        inputs = inputs or LiteralMap(literals={})

        result = self._call(
            method="invoke_endpoint",
            inputs=inputs,
            config=task_template.custom["task_config"],
            additional_args=additional_args,
            aws_access_key_id=get_agent_secret(secret_key="AWS_ACCESS_KEY"),
            aws_secret_access_key=get_agent_secret(secret_key="AWS_SECRET_ACCESS_KEY"),
            aws_session_token=get_agent_secret(secret_key="AWS_SESSION_TOKEN"),
        )

        ctx = FlyteContextManager.current_context()
        outputs = LiteralMap(
            {
                "o0": TypeEngine.to_literal(
                    ctx,
                    result,
                    dict[str, output_result_type],
                    TypeEngine.to_literal_type(dict[str, output_result_type]),
                )
            }
        ).to_flyte_idl()
        return CreateTaskResponse(resource=Resource(state=SUCCEEDED, outputs=outputs))


class SagemakerDeleteEndpointTask(Boto3AgentMixin, ExternalApiTask):
    """This agent deletes the Sagemaker model."""

    def __init__(self, name: str, config: dict[str, Any], region: Optional[str] = None, **kwargs):
        super().__init__(service="sagemaker-runtime", region=region, name=name, config=config, **kwargs)

    def do(
        self,
        task_template: TaskTemplate,
        inputs: Optional[LiteralMap] = None,
    ) -> CreateTaskResponse:
        inputs = inputs or LiteralMap(literals={})

        self._call(
            method="delete_endpoint",
            inputs=inputs,
            config=task_template.custom["task_config"],
            aws_access_key_id=get_agent_secret(secret_key="AWS_ACCESS_KEY"),
            aws_secret_access_key=get_agent_secret(secret_key="AWS_SECRET_ACCESS_KEY"),
            aws_session_token=get_agent_secret(secret_key="AWS_SESSION_TOKEN"),
        )

        return CreateTaskResponse(resource=Resource(state=SUCCEEDED, outputs=None))


class SagemakerDeleteEndpointConfigTask(Boto3AgentMixin, ExternalApiTask):
    """This agent deletes the endpoint config."""

    def __init__(self, name: str, config: dict[str, Any], region: Optional[str] = None, **kwargs):
        super().__init__(service="sagemaker-runtime", region=region, name=name, config=config, **kwargs)

    def do(
        self,
        task_template: TaskTemplate,
        inputs: Optional[LiteralMap] = None,
    ) -> CreateTaskResponse:
        inputs = inputs or LiteralMap(literals={})

        self._call(
            method="delete_endpoint_config",
            inputs=inputs,
            config=task_template.custom["task_config"],
            aws_access_key_id=get_agent_secret(secret_key="AWS_ACCESS_KEY"),
            aws_secret_access_key=get_agent_secret(secret_key="AWS_SECRET_ACCESS_KEY"),
            aws_session_token=get_agent_secret(secret_key="AWS_SESSION_TOKEN"),
        )

        return CreateTaskResponse(resource=Resource(state=SUCCEEDED, outputs=None))


class SagemakerDeleteModelTask(Boto3AgentMixin, ExternalApiTask):
    """This agent deletes an endpoint."""

    def __init__(self, name: str, config: dict[str, Any], region: Optional[str] = None, **kwargs):
        super().__init__(service="sagemaker-runtime", region=region, name=name, config=config, **kwargs)

    def do(
        self,
        task_template: TaskTemplate,
        inputs: Optional[LiteralMap] = None,
    ) -> CreateTaskResponse:
        inputs = inputs or LiteralMap(literals={})

        self._call(
            method="delete_model",
            inputs=inputs,
            config=task_template.custom["task_config"],
            aws_access_key_id=get_agent_secret(secret_key="AWS_ACCESS_KEY"),
            aws_secret_access_key=get_agent_secret(secret_key="AWS_SECRET_ACCESS_KEY"),
            aws_session_token=get_agent_secret(secret_key="AWS_SESSION_TOKEN"),
        )

        return CreateTaskResponse(resource=Resource(state=SUCCEEDED, outputs=None))


AgentRegistry.register(SagemakerEndpointAgent())

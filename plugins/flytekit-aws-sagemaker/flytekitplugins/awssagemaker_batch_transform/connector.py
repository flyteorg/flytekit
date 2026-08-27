"""SageMaker batch-transform connector.

Mirrors the training-job connector. Targets ``CreateTransformJob`` /
``DescribeTransformJob`` / ``StopTransformJob``. Surfaces the predictions
``S3OutputPath`` so downstream Flyte tasks can read scores written by SageMaker
without any extra plumbing.

Note: ``TransformJobStatus`` has no ``Deleting`` state and there is no
``SecondaryStatus`` — running phase has no live progress signal beyond the job
being in flight.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional

import cloudpickle
from flyteidl.core.execution_pb2 import TaskExecution
from flytekitplugins.awssagemaker_inference.boto3_mixin import (
    Boto3ConnectorMixin,
    CustomException,
)

from flytekit.extend.backend.base_connector import (
    AsyncConnectorBase,
    ConnectorRegistry,
    Resource,
    ResourceMeta,
)
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate


@dataclass
class SageMakerTransformJobMetadata(ResourceMeta):
    config: Dict[str, Any]
    region: Optional[str] = None
    inputs: Optional[LiteralMap] = None

    def encode(self) -> bytes:
        return cloudpickle.dumps(self)

    @classmethod
    def decode(cls, data: bytes) -> "SageMakerTransformJobMetadata":
        return cloudpickle.loads(data)


_STATE_MAP = {
    "InProgress": TaskExecution.RUNNING,
    "Stopping": TaskExecution.RUNNING,
    "Completed": TaskExecution.SUCCEEDED,
    "Failed": TaskExecution.FAILED,
    "Stopped": TaskExecution.FAILED,
}


def _isoformat(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _build_outputs(describe_response: Dict[str, Any]) -> Dict[str, Any]:
    """Project describe_transform_job down to a stable, downstream-friendly dict."""
    transform_output = describe_response.get("TransformOutput") or {}
    return {
        "TransformJobArn": describe_response.get("TransformJobArn"),
        "TransformJobName": describe_response.get("TransformJobName"),
        "ModelName": describe_response.get("ModelName"),
        "TransformOutput": {"S3OutputPath": transform_output.get("S3OutputPath")},
        "TransformStartTime": _isoformat(describe_response.get("TransformStartTime")),
        "TransformEndTime": _isoformat(describe_response.get("TransformEndTime")),
    }


class SageMakerTransformJobConnector(Boto3ConnectorMixin, AsyncConnectorBase):
    """Long-running connector for SageMaker batch-transform jobs."""

    name = "SageMaker Transform Job Connector"

    def __init__(self):
        super().__init__(
            service="sagemaker",
            task_type_name="sagemaker-transform-job",
            metadata_type=SageMakerTransformJobMetadata,
        )

    async def create(
        self, task_template: TaskTemplate, inputs: Optional[LiteralMap] = None, **kwargs
    ) -> SageMakerTransformJobMetadata:
        custom = task_template.custom
        config = custom.get("config")
        region = custom.get("region")

        try:
            await self._call(
                method="create_transform_job",
                config=config,
                inputs=inputs,
                region=region,
            )
        except CustomException as e:
            original_exception = e.original_exception
            error_code = original_exception.response["Error"]["Code"]
            error_message = original_exception.response["Error"]["Message"]

            if e.idempotence_token and (
                error_code == "ResourceInUse"
                or (error_code == "ValidationException" and "Cannot create already existing" in error_message)
            ):
                return SageMakerTransformJobMetadata(config=config, region=region, inputs=inputs)
            raise e

        return SageMakerTransformJobMetadata(config=config, region=region, inputs=inputs)

    async def get(self, resource_meta: SageMakerTransformJobMetadata, **kwargs) -> Resource:
        describe_response, _ = await self._call(
            method="describe_transform_job",
            config={"TransformJobName": resource_meta.config.get("TransformJobName")},
            inputs=resource_meta.inputs,
            region=resource_meta.region,
        )

        current_state = describe_response.get("TransformJobStatus")
        flyte_phase = _STATE_MAP.get(current_state, TaskExecution.RUNNING)

        message: Optional[str] = None
        if current_state in ("Failed", "Stopped"):
            message = describe_response.get("FailureReason")

        outputs: Optional[Dict[str, Any]] = None
        if current_state == "Completed":
            outputs = {"result": _build_outputs(describe_response)}

        return Resource(phase=flyte_phase, outputs=outputs, message=message)

    async def delete(self, resource_meta: SageMakerTransformJobMetadata, **kwargs):
        try:
            await self._call(
                method="stop_transform_job",
                config={"TransformJobName": resource_meta.config.get("TransformJobName")},
                region=resource_meta.region,
                inputs=resource_meta.inputs,
            )
        except CustomException as e:
            original_exception = e.original_exception
            error_code = original_exception.response["Error"]["Code"]
            error_message = original_exception.response["Error"]["Message"]

            if error_code == "ResourceNotFound" or (
                error_code == "ValidationException" and "non-running" in error_message
            ):
                return
            raise e


ConnectorRegistry.register(SageMakerTransformJobConnector())

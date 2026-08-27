"""SageMaker Inference Recommender job connector.

Mirrors the training-job / batch-transform connectors. Targets
``CreateInferenceRecommendationsJob`` / ``DescribeInferenceRecommendationsJob`` /
``StopInferenceRecommendationsJob``. Surfaces the ranked
``InferenceRecommendations`` list (Default jobs) and the
``EndpointPerformances`` list (Default jobs targeting existing endpoints) so
downstream Flyte tasks can pick an instance type / endpoint config without
re-querying SageMaker.

Note: ``InferenceRecommendationsJob`` ``Status`` values are ALL_CAPS
(``PENDING`` / ``IN_PROGRESS`` / ``COMPLETED`` / ``FAILED`` / ``STOPPING`` /
``STOPPED`` / ``DELETING`` / ``DELETED``), unlike training/transform jobs which
use PascalCase.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

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
class SageMakerInferenceRecommenderJobMetadata(ResourceMeta):
    config: Dict[str, Any]
    region: Optional[str] = None
    inputs: Optional[LiteralMap] = None

    def encode(self) -> bytes:
        return cloudpickle.dumps(self)

    @classmethod
    def decode(cls, data: bytes) -> "SageMakerInferenceRecommenderJobMetadata":
        return cloudpickle.loads(data)


# Status values per boto3 reference. PENDING and IN_PROGRESS keep the job
# in flight; STOPPING is still a running tear-down. STOPPED covers both
# user-stop and timeout. DELETING/DELETED are admin states - treat as failure
# so we don't silently surface partial recommendations.
_STATE_MAP = {
    "PENDING": TaskExecution.RUNNING,
    "IN_PROGRESS": TaskExecution.RUNNING,
    "STOPPING": TaskExecution.RUNNING,
    "COMPLETED": TaskExecution.SUCCEEDED,
    "FAILED": TaskExecution.FAILED,
    "STOPPED": TaskExecution.FAILED,
    "DELETING": TaskExecution.FAILED,
    "DELETED": TaskExecution.FAILED,
}


def _isoformat(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _project_recommendation(rec: Dict[str, Any]) -> Dict[str, Any]:
    """Trim a single InferenceRecommendations entry to the fields users actually pick on."""
    metrics = rec.get("Metrics") or {}
    endpoint_config = rec.get("EndpointConfiguration") or {}
    model_config = rec.get("ModelConfiguration") or {}
    serverless_config = endpoint_config.get("ServerlessConfig") or {}

    return {
        "RecommendationId": rec.get("RecommendationId"),
        "Metrics": {
            "CostPerHour": metrics.get("CostPerHour"),
            "CostPerInference": metrics.get("CostPerInference"),
            "MaxInvocations": metrics.get("MaxInvocations"),
            "ModelLatency": metrics.get("ModelLatency"),
            "CpuUtilization": metrics.get("CpuUtilization"),
            "MemoryUtilization": metrics.get("MemoryUtilization"),
            "ModelSetupTime": metrics.get("ModelSetupTime"),
        },
        "EndpointConfiguration": {
            "EndpointName": endpoint_config.get("EndpointName"),
            "VariantName": endpoint_config.get("VariantName"),
            "InstanceType": endpoint_config.get("InstanceType"),
            "InitialInstanceCount": endpoint_config.get("InitialInstanceCount"),
            "ServerlessConfig": {
                "MemorySizeInMB": serverless_config.get("MemorySizeInMB"),
                "MaxConcurrency": serverless_config.get("MaxConcurrency"),
                "ProvisionedConcurrency": serverless_config.get("ProvisionedConcurrency"),
            }
            if serverless_config
            else None,
        },
        "ModelConfiguration": {
            "InferenceSpecificationName": model_config.get("InferenceSpecificationName"),
            "CompilationJobName": model_config.get("CompilationJobName"),
        },
        "InvocationStartTime": _isoformat(rec.get("InvocationStartTime")),
        "InvocationEndTime": _isoformat(rec.get("InvocationEndTime")),
    }


def _project_endpoint_performance(perf: Dict[str, Any]) -> Dict[str, Any]:
    metrics = perf.get("Metrics") or {}
    endpoint_info = perf.get("EndpointInfo") or {}
    return {
        "Metrics": {
            "MaxInvocations": metrics.get("MaxInvocations"),
            "ModelLatency": metrics.get("ModelLatency"),
        },
        "EndpointInfo": {"EndpointName": endpoint_info.get("EndpointName")},
    }


def _build_outputs(describe_response: Dict[str, Any]) -> Dict[str, Any]:
    """Project describe_inference_recommendations_job down to a stable, downstream-friendly dict."""
    recommendations: List[Dict[str, Any]] = [
        _project_recommendation(rec) for rec in (describe_response.get("InferenceRecommendations") or [])
    ]
    endpoint_performances: List[Dict[str, Any]] = [
        _project_endpoint_performance(perf) for perf in (describe_response.get("EndpointPerformances") or [])
    ]

    return {
        "JobArn": describe_response.get("JobArn"),
        "JobName": describe_response.get("JobName"),
        "JobType": describe_response.get("JobType"),
        "InferenceRecommendations": recommendations,
        "EndpointPerformances": endpoint_performances,
        "CompletionTime": _isoformat(describe_response.get("CompletionTime")),
    }


class SageMakerInferenceRecommenderJobConnector(Boto3ConnectorMixin, AsyncConnectorBase):
    """Long-running connector for SageMaker Inference Recommender jobs."""

    name = "SageMaker Inference Recommender Job Connector"

    def __init__(self):
        super().__init__(
            service="sagemaker",
            task_type_name="sagemaker-inference-recommender-job",
            metadata_type=SageMakerInferenceRecommenderJobMetadata,
        )

    async def create(
        self, task_template: TaskTemplate, inputs: Optional[LiteralMap] = None, **kwargs
    ) -> SageMakerInferenceRecommenderJobMetadata:
        custom = task_template.custom
        config = custom.get("config")
        region = custom.get("region")

        try:
            await self._call(
                method="create_inference_recommendations_job",
                config=config,
                inputs=inputs,
                region=region,
            )
        except CustomException as e:
            original_exception = e.original_exception
            error_code = original_exception.response["Error"]["Code"]
            error_message = original_exception.response["Error"]["Message"]

            # Idempotent re-runs: SageMaker rejects duplicate job names. Treat as already-running.
            if e.idempotence_token and (
                error_code == "ResourceInUse"
                or (error_code == "ValidationException" and "Cannot create already existing" in error_message)
            ):
                return SageMakerInferenceRecommenderJobMetadata(config=config, region=region, inputs=inputs)
            raise e

        return SageMakerInferenceRecommenderJobMetadata(config=config, region=region, inputs=inputs)

    async def get(self, resource_meta: SageMakerInferenceRecommenderJobMetadata, **kwargs) -> Resource:
        describe_response, _ = await self._call(
            method="describe_inference_recommendations_job",
            config={"JobName": resource_meta.config.get("JobName")},
            inputs=resource_meta.inputs,
            region=resource_meta.region,
        )

        current_state = describe_response.get("Status")
        flyte_phase = _STATE_MAP.get(current_state, TaskExecution.RUNNING)

        # Inference Recommender has no SecondaryStatus, but FailureReason is the
        # most useful single line on terminal failure.
        message: Optional[str] = None
        if current_state in ("FAILED", "STOPPED", "DELETING", "DELETED"):
            message = describe_response.get("FailureReason")

        outputs: Optional[Dict[str, Any]] = None
        if current_state == "COMPLETED":
            outputs = {"result": _build_outputs(describe_response)}

        return Resource(phase=flyte_phase, outputs=outputs, message=message)

    async def delete(self, resource_meta: SageMakerInferenceRecommenderJobMetadata, **kwargs):
        try:
            await self._call(
                method="stop_inference_recommendations_job",
                config={"JobName": resource_meta.config.get("JobName")},
                region=resource_meta.region,
                inputs=resource_meta.inputs,
            )
        except CustomException as e:
            original_exception = e.original_exception
            error_code = original_exception.response["Error"]["Code"]
            error_message = original_exception.response["Error"]["Message"]

            # Flyte may invoke delete() after the job has naturally completed (or already
            # been stopped). SageMaker rejects stop on a non-running job - swallow that
            # specific error since there's nothing to cancel.
            if error_code == "ResourceNotFound" or (
                error_code == "ValidationException" and "non-running" in error_message
            ):
                return
            raise e


ConnectorRegistry.register(SageMakerInferenceRecommenderJobConnector())

"""SageMaker training-job connector.

Mirrors the pattern in ``awssagemaker_inference.connector`` (long-running async
lifecycle: create → describe-poll → stop) but targets ``CreateTrainingJob``
instead of ``CreateEndpoint``. Surfaces the trained ``S3ModelArtifacts`` URI and
final metrics in outputs so downstream Flyte tasks (a ``SageMakerModelTask`` for
deployment, or a custom Flyte gate task for accuracy thresholds) can consume them
without any extra plumbing.
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
from flytekitplugins.awssagemaker_inference.pythonic_base import (
    PythonicSageMakerJobConnector,
    _build_environment,
    _container_entrypoint,
    _tags_to_list,
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
class SageMakerTrainingJobMetadata(ResourceMeta):
    config: Dict[str, Any]
    region: Optional[str] = None
    inputs: Optional[LiteralMap] = None

    def encode(self) -> bytes:
        return cloudpickle.dumps(self)

    @classmethod
    def decode(cls, data: bytes) -> "SageMakerTrainingJobMetadata":
        return cloudpickle.loads(data)


# TrainingJobStatus → Flyte phase (verified against current boto3 reference).
# - Stopping is "still in flight" so we report Running while SageMaker tears the job down.
# - Stopped covers both user-stop and MaxRuntimeExceeded / MaxWaitTimeExceeded — all of
#   these are user-visible failures from the workflow's perspective.
# - Deleting is a terminal admin state; treat as failure.
_STATE_MAP = {
    "InProgress": TaskExecution.RUNNING,
    "Stopping": TaskExecution.RUNNING,
    "Completed": TaskExecution.SUCCEEDED,
    "Failed": TaskExecution.FAILED,
    "Stopped": TaskExecution.FAILED,
    "Deleting": TaskExecution.FAILED,
}


def _isoformat(value: Any) -> Any:
    """Best-effort ISO8601 string for datetime values, leave everything else alone."""
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _build_outputs(describe_response: Dict[str, Any]) -> Dict[str, Any]:
    """Project the describe_training_job response down to a stable, downstream-friendly dict."""
    metrics = []
    for metric in describe_response.get("FinalMetricDataList") or []:
        metrics.append(
            {
                "MetricName": metric.get("MetricName"),
                "Value": metric.get("Value"),
                "Timestamp": _isoformat(metric.get("Timestamp")),
            }
        )

    model_artifacts = describe_response.get("ModelArtifacts") or {}
    output_data_config = describe_response.get("OutputDataConfig") or {}

    return {
        "TrainingJobArn": describe_response.get("TrainingJobArn"),
        "TrainingJobName": describe_response.get("TrainingJobName"),
        "ModelArtifacts": {"S3ModelArtifacts": model_artifacts.get("S3ModelArtifacts")},
        "OutputDataConfig": {"S3OutputPath": output_data_config.get("S3OutputPath")},
        "FinalMetricDataList": metrics,
        "BillableTimeInSeconds": describe_response.get("BillableTimeInSeconds"),
        "TrainingTimeInSeconds": describe_response.get("TrainingTimeInSeconds"),
    }


class SageMakerTrainingJobConnector(Boto3ConnectorMixin, AsyncConnectorBase):
    """Long-running connector for SageMaker training jobs."""

    name = "SageMaker Training Job Connector"

    def __init__(self):
        super().__init__(
            service="sagemaker",
            task_type_name="sagemaker-training-job",
            metadata_type=SageMakerTrainingJobMetadata,
        )

    async def create(
        self, task_template: TaskTemplate, inputs: Optional[LiteralMap] = None, **kwargs
    ) -> SageMakerTrainingJobMetadata:
        custom = task_template.custom
        config = custom.get("config")
        region = custom.get("region")
        images = custom.get("images")

        try:
            await self._call(
                method="create_training_job",
                config=config,
                images=images,
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
                return SageMakerTrainingJobMetadata(config=config, region=region, inputs=inputs)
            raise e

        return SageMakerTrainingJobMetadata(config=config, region=region, inputs=inputs)

    async def get(self, resource_meta: SageMakerTrainingJobMetadata, **kwargs) -> Resource:
        describe_response, _ = await self._call(
            method="describe_training_job",
            config={"TrainingJobName": resource_meta.config.get("TrainingJobName")},
            inputs=resource_meta.inputs,
            region=resource_meta.region,
        )

        current_state = describe_response.get("TrainingJobStatus")
        flyte_phase = _STATE_MAP.get(current_state, TaskExecution.RUNNING)

        # Surface SecondaryStatus while running so the Flyte UI shows live progress
        # (Starting → Downloading → Training → Uploading → Completed). On Failed/Stopped,
        # FailureReason is the most useful single line.
        message: Optional[str] = None
        if current_state == "InProgress":
            message = describe_response.get("SecondaryStatus")
        elif current_state in ("Failed", "Stopped"):
            message = describe_response.get("FailureReason") or describe_response.get("SecondaryStatus")

        outputs: Optional[Dict[str, Any]] = None
        if current_state == "Completed":
            outputs = {"result": _build_outputs(describe_response)}

        return Resource(phase=flyte_phase, outputs=outputs, message=message)

    async def delete(self, resource_meta: SageMakerTrainingJobMetadata, **kwargs):
        try:
            await self._call(
                method="stop_training_job",
                config={"TrainingJobName": resource_meta.config.get("TrainingJobName")},
                region=resource_meta.region,
                inputs=resource_meta.inputs,
            )
        except CustomException as e:
            original_exception = e.original_exception
            error_code = original_exception.response["Error"]["Code"]
            error_message = original_exception.response["Error"]["Message"]

            # Flyte may invoke delete() after the job has naturally completed (or already
            # been stopped). SageMaker rejects stop on a non-running job — swallow that
            # specific error since there's nothing to cancel.
            if error_code == "ResourceNotFound" or (
                error_code == "ValidationException" and "non-running" in error_message
            ):
                return
            raise e


ConnectorRegistry.register(SageMakerTrainingJobConnector())


class SageMakerTrainingTaskConnector(PythonicSageMakerJobConnector):
    """Pythonic-mode connector: runs a Flyte ``@task`` function inside a training job."""

    name = "SageMaker Training Task Connector"

    task_type_name = "sagemaker-training-task"
    create_method = "create_training_job"
    describe_method = "describe_training_job"
    stop_method = "stop_training_job"
    job_name_key = "TrainingJobName"
    status_key = "TrainingJobStatus"
    secondary_status_key = "SecondaryStatus"
    _state_map = _STATE_MAP

    def _build_request(self, *, container, config, job_name, output_prefix):
        resource_config: Dict[str, Any] = {
            "InstanceType": config.get("instance_type", "ml.m5.large"),
            "InstanceCount": config.get("instance_count", 1),
            "VolumeSizeInGB": config.get("volume_size_in_gb", 30),
        }
        kms_key_id = config.get("kms_key_id")
        if kms_key_id:
            resource_config["VolumeKmsKeyId"] = kms_key_id

        # Pythonic mode returns results via Flyte outputs.pb; SageMaker still
        # requires OutputDataConfig, so default it to the Flyte output prefix
        # (the resulting model.tar.gz is harmless and unused).
        output_s3_path = config.get("output_s3_path") or f"{output_prefix}/_sagemaker_model"

        request: Dict[str, Any] = {
            "TrainingJobName": job_name,
            "RoleArn": config["execution_role_arn"],
            "AlgorithmSpecification": {
                "TrainingImage": container.image,
                "ContainerEntrypoint": _container_entrypoint(container),
                "TrainingInputMode": "File",
            },
            "ResourceConfig": resource_config,
            "OutputDataConfig": {"S3OutputPath": output_s3_path},
            "StoppingCondition": {"MaxRuntimeInSeconds": config.get("max_runtime_in_seconds", 3600)},
        }

        request["Environment"] = _build_environment(container, config)
        vpc_config = config.get("vpc_config")
        if vpc_config:
            request["VpcConfig"] = vpc_config
        tags = _tags_to_list(config.get("tags"))
        if tags:
            request["Tags"] = tags
        return request


ConnectorRegistry.register(SageMakerTrainingTaskConnector())

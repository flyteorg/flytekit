"""SageMaker processing-job connector.

Mirrors ``awssagemaker_training.connector`` (long-running async lifecycle:
create → describe-poll → stop) but targets ``CreateProcessingJob`` instead of
``CreateTrainingJob``. Surfaces the processed-output S3 URIs in outputs so
downstream Flyte tasks (a ``SageMakerTrainingJobTask`` consuming engineered
features, a ``SageMakerModelTask``, or a custom gate task) can consume them
without any extra plumbing.

Note: ``ProcessingJobStatus`` values (``InProgress`` / ``Completed`` /
``Failed`` / ``Stopping`` / ``Stopped``) match the PascalCase training-job
convention, but processing jobs have no ``SecondaryStatus`` — ``ExitMessage`` /
``FailureReason`` are the useful single lines on terminal states.
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
class SageMakerProcessingJobMetadata(ResourceMeta):
    config: Dict[str, Any]
    region: Optional[str] = None
    inputs: Optional[LiteralMap] = None

    def encode(self) -> bytes:
        return cloudpickle.dumps(self)

    @classmethod
    def decode(cls, data: bytes) -> "SageMakerProcessingJobMetadata":
        return cloudpickle.loads(data)


# ProcessingJobStatus → Flyte phase (verified against current boto3 reference).
# - Stopping is "still in flight" so we report Running while SageMaker tears the job down.
# - Stopped covers both user-stop and MaxRuntimeExceeded — a user-visible failure from
#   the workflow's perspective.
# Processing jobs have no "Deleting" state (unlike training jobs).
_STATE_MAP = {
    "InProgress": TaskExecution.RUNNING,
    "Stopping": TaskExecution.RUNNING,
    "Completed": TaskExecution.SUCCEEDED,
    "Failed": TaskExecution.FAILED,
    "Stopped": TaskExecution.FAILED,
}


def _isoformat(value: Any) -> Any:
    """Best-effort ISO8601 string for datetime values, leave everything else alone."""
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _build_outputs(describe_response: Dict[str, Any]) -> Dict[str, Any]:
    """Project the describe_processing_job response down to a stable, downstream-friendly dict."""
    output_config = describe_response.get("ProcessingOutputConfig") or {}
    outputs: List[Dict[str, Any]] = []
    for output in output_config.get("Outputs") or []:
        s3_output = output.get("S3Output") or {}
        feature_store_output = output.get("FeatureStoreOutput") or {}
        projected_output = {"OutputName": output.get("OutputName")}
        if s3_output:
            projected_output["S3Uri"] = s3_output.get("S3Uri")
        if feature_store_output:
            projected_output["FeatureGroupName"] = feature_store_output.get("FeatureGroupName")
        outputs.append(projected_output)

    return {
        "ProcessingJobArn": describe_response.get("ProcessingJobArn"),
        "ProcessingJobName": describe_response.get("ProcessingJobName"),
        "Outputs": outputs,
        "ExitMessage": describe_response.get("ExitMessage"),
        "ProcessingStartTime": _isoformat(describe_response.get("ProcessingStartTime")),
        "ProcessingEndTime": _isoformat(describe_response.get("ProcessingEndTime")),
    }


class SageMakerProcessingJobConnector(Boto3ConnectorMixin, AsyncConnectorBase):
    """Long-running connector for SageMaker processing jobs."""

    name = "SageMaker Processing Job Connector"

    def __init__(self):
        super().__init__(
            service="sagemaker",
            task_type_name="sagemaker-processing-job",
            metadata_type=SageMakerProcessingJobMetadata,
        )

    async def create(
        self, task_template: TaskTemplate, inputs: Optional[LiteralMap] = None, **kwargs
    ) -> SageMakerProcessingJobMetadata:
        custom = task_template.custom
        config = custom.get("config")
        region = custom.get("region")
        images = custom.get("images")

        try:
            await self._call(
                method="create_processing_job",
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
                return SageMakerProcessingJobMetadata(config=config, region=region, inputs=inputs)
            raise e

        return SageMakerProcessingJobMetadata(config=config, region=region, inputs=inputs)

    async def get(self, resource_meta: SageMakerProcessingJobMetadata, **kwargs) -> Resource:
        describe_response, _ = await self._call(
            method="describe_processing_job",
            config={"ProcessingJobName": resource_meta.config.get("ProcessingJobName")},
            inputs=resource_meta.inputs,
            region=resource_meta.region,
        )

        current_state = describe_response.get("ProcessingJobStatus")
        flyte_phase = _STATE_MAP.get(current_state, TaskExecution.RUNNING)

        # Processing jobs expose no SecondaryStatus, so there's no live sub-status to
        # surface while running. On Failed/Stopped, FailureReason (falling back to
        # ExitMessage) is the most useful single line.
        message: Optional[str] = None
        if current_state in ("Failed", "Stopped"):
            message = describe_response.get("FailureReason") or describe_response.get("ExitMessage")

        outputs: Optional[Dict[str, Any]] = None
        if current_state == "Completed":
            outputs = {"result": _build_outputs(describe_response)}

        return Resource(phase=flyte_phase, outputs=outputs, message=message)

    async def delete(self, resource_meta: SageMakerProcessingJobMetadata, **kwargs):
        try:
            await self._call(
                method="stop_processing_job",
                config={"ProcessingJobName": resource_meta.config.get("ProcessingJobName")},
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


ConnectorRegistry.register(SageMakerProcessingJobConnector())


class SageMakerProcessingTaskConnector(PythonicSageMakerJobConnector):
    """Pythonic-mode connector: runs a Flyte ``@task`` function inside a processing job."""

    name = "SageMaker Processing Task Connector"

    task_type_name = "sagemaker-processing-task"
    create_method = "create_processing_job"
    describe_method = "describe_processing_job"
    stop_method = "stop_processing_job"
    job_name_key = "ProcessingJobName"
    status_key = "ProcessingJobStatus"
    secondary_status_key = None  # processing jobs have no SecondaryStatus
    _state_map = _STATE_MAP

    def _build_request(self, *, container, config, job_name, output_prefix):
        cluster_config: Dict[str, Any] = {
            "InstanceType": config.get("instance_type", "ml.m5.large"),
            "InstanceCount": config.get("instance_count", 1),
            "VolumeSizeInGB": config.get("volume_size_in_gb", 30),
        }
        kms_key_id = config.get("kms_key_id")
        if kms_key_id:
            cluster_config["VolumeKmsKeyId"] = kms_key_id

        request: Dict[str, Any] = {
            "ProcessingJobName": job_name,
            "RoleArn": config["execution_role_arn"],
            "AppSpecification": {
                "ImageUri": container.image,
                "ContainerEntrypoint": _container_entrypoint(container),
            },
            "ProcessingResources": {"ClusterConfig": cluster_config},
            "StoppingCondition": {"MaxRuntimeInSeconds": config.get("max_runtime_in_seconds", 3600)},
        }

        request["Environment"] = _build_environment(container, config)
        network_config = config.get("network_config")
        if network_config:
            if network_config.get("EnableNetworkIsolation"):
                raise ValueError(
                    "EnableNetworkIsolation=True is incompatible with Pythonic mode because pyflyte-execute "
                    "must access Flyte's S3 input and output prefixes."
                )
            request["NetworkConfig"] = network_config
        tags = _tags_to_list(config.get("tags"))
        if tags:
            request["Tags"] = tags
        return request


ConnectorRegistry.register(SageMakerProcessingTaskConnector())

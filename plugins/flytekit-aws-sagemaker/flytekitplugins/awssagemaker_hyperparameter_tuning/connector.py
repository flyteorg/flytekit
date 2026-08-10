"""SageMaker hyperparameter-tuning-job connector.

Mirrors the training-job connector: same long-running async lifecycle
(create -> describe-poll -> stop) but targets ``CreateHyperParameterTuningJob``.
On completion, surfaces ``BestTrainingJob`` plus the trained
``S3ModelArtifacts`` (looked up via a single follow-up
``describe_training_job`` call, since ``DescribeHyperParameterTuningJob`` does
not include it) so the result chains straight into ``SageMakerModelTask``
without any extra workflow plumbing.
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
class SageMakerHyperParameterTuningJobMetadata(ResourceMeta):
    config: Dict[str, Any]
    region: Optional[str] = None
    inputs: Optional[LiteralMap] = None

    def encode(self) -> bytes:
        return cloudpickle.dumps(self)

    @classmethod
    def decode(cls, data: bytes) -> "SageMakerHyperParameterTuningJobMetadata":
        return cloudpickle.loads(data)


# HyperParameterTuningJobStatus -> Flyte phase.
# - Stopping is "still in flight" (SageMaker is asking each child training job to stop
#   gracefully) so we report Running while the tear-down happens.
# - Stopped / Deleting / DeleteFailed are terminal admin states; treat as failure.
# - Failed covers both genuine job failure and warm-start parent failure.
_STATE_MAP = {
    "InProgress": TaskExecution.RUNNING,
    "Stopping": TaskExecution.RUNNING,
    "Completed": TaskExecution.SUCCEEDED,
    "Failed": TaskExecution.FAILED,
    "Stopped": TaskExecution.FAILED,
    "Deleting": TaskExecution.FAILED,
    "DeleteFailed": TaskExecution.FAILED,
}


def _isoformat(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _project_best_training_job(best: Dict[str, Any], s3_model_artifacts: Optional[str]) -> Dict[str, Any]:
    """Trim BestTrainingJob to the fields downstream tasks key off of."""
    objective_metric = best.get("FinalHyperParameterTuningJobObjectiveMetric") or {}
    return {
        "TrainingJobName": best.get("TrainingJobName"),
        "TrainingJobArn": best.get("TrainingJobArn"),
        "TrainingJobStatus": best.get("TrainingJobStatus"),
        "ObjectiveStatus": best.get("ObjectiveStatus"),
        "FinalHyperParameterTuningJobObjectiveMetric": {
            "MetricName": objective_metric.get("MetricName"),
            "Value": objective_metric.get("Value"),
        },
        "TunedHyperParameters": dict(best.get("TunedHyperParameters") or {}),
        # Carry the same nested shape Training emits so downstream tasks that
        # consume train_result["ModelArtifacts"]["S3ModelArtifacts"] work
        # unchanged against an HPO result.
        "ModelArtifacts": {"S3ModelArtifacts": s3_model_artifacts},
        "TrainingStartTime": _isoformat(best.get("TrainingStartTime")),
        "TrainingEndTime": _isoformat(best.get("TrainingEndTime")),
    }


def _build_outputs(describe_response: Dict[str, Any], best_s3_model_artifacts: Optional[str]) -> Dict[str, Any]:
    """Project DescribeHyperParameterTuningJob into a stable, downstream-friendly dict."""
    best = describe_response.get("BestTrainingJob") or {}
    counters = describe_response.get("TrainingJobStatusCounters") or {}
    obj_counters = describe_response.get("ObjectiveStatusCounters") or {}

    return {
        "HyperParameterTuningJobArn": describe_response.get("HyperParameterTuningJobArn"),
        "HyperParameterTuningJobName": describe_response.get("HyperParameterTuningJobName"),
        "BestTrainingJob": _project_best_training_job(best, best_s3_model_artifacts),
        # Same nested shape as ModelArtifacts above — promotes BestTrainingJob's
        # artifacts to the top level so `result["ModelArtifacts"]["S3ModelArtifacts"]`
        # is symmetric with the plain training-job task's output.
        "ModelArtifacts": {"S3ModelArtifacts": best_s3_model_artifacts},
        "TrainingJobStatusCounters": {
            "Completed": counters.get("Completed"),
            "InProgress": counters.get("InProgress"),
            "RetryableError": counters.get("RetryableError"),
            "NonRetryableError": counters.get("NonRetryableError"),
            "Stopped": counters.get("Stopped"),
        },
        "ObjectiveStatusCounters": {
            "Succeeded": obj_counters.get("Succeeded"),
            "Pending": obj_counters.get("Pending"),
            "Failed": obj_counters.get("Failed"),
        },
    }


def _running_message(describe_response: Dict[str, Any]) -> Optional[str]:
    """Compact 'N completed / M in-progress / K failed' status line for the Flyte UI."""
    counters = describe_response.get("TrainingJobStatusCounters") or {}
    completed = counters.get("Completed") or 0
    in_progress = counters.get("InProgress") or 0
    failed = (counters.get("RetryableError") or 0) + (counters.get("NonRetryableError") or 0)
    return f"{completed} Completed / {in_progress} InProgress / {failed} Failed trials"


class SageMakerHyperParameterTuningJobConnector(Boto3ConnectorMixin, AsyncConnectorBase):
    """Long-running connector for SageMaker hyperparameter-tuning jobs."""

    name = "SageMaker Hyperparameter Tuning Job Connector"

    def __init__(self):
        super().__init__(
            service="sagemaker",
            task_type_name="sagemaker-hyperparameter-tuning-job",
            metadata_type=SageMakerHyperParameterTuningJobMetadata,
        )

    async def create(
        self, task_template: TaskTemplate, inputs: Optional[LiteralMap] = None, **kwargs
    ) -> SageMakerHyperParameterTuningJobMetadata:
        custom = task_template.custom
        config = custom.get("config")
        region = custom.get("region")
        images = custom.get("images")

        try:
            await self._call(
                method="create_hyper_parameter_tuning_job",
                config=config,
                images=images,
                inputs=inputs,
                region=region,
            )
        except CustomException as e:
            original_exception = e.original_exception
            error_code = original_exception.response["Error"]["Code"]
            error_message = original_exception.response["Error"]["Message"]

            # Idempotent re-runs: SageMaker rejects duplicate tuning job names. Treat as already-running.
            if e.idempotence_token and (
                error_code == "ResourceInUse"
                or (error_code == "ValidationException" and "Cannot create already existing" in error_message)
            ):
                return SageMakerHyperParameterTuningJobMetadata(config=config, region=region, inputs=inputs)
            raise e

        return SageMakerHyperParameterTuningJobMetadata(config=config, region=region, inputs=inputs)

    async def _best_training_job_artifacts(
        self,
        describe_response: Dict[str, Any],
        resource_meta: SageMakerHyperParameterTuningJobMetadata,
    ) -> Optional[str]:
        """Resolve BestTrainingJob -> S3ModelArtifacts via one extra describe call.

        ``DescribeHyperParameterTuningJob`` returns the best job's name and
        tuned hyperparameters but NOT its ``ModelArtifacts`` — that lives on
        ``DescribeTrainingJob``. We do the follow-up here so the connector's
        ``result`` dict is self-contained: downstream tasks can chain on
        ``result["ModelArtifacts"]["S3ModelArtifacts"]`` exactly like they do
        for the plain training-job task.
        """
        best = describe_response.get("BestTrainingJob") or {}
        best_name = best.get("TrainingJobName")
        if not best_name:
            return None
        training_describe, _ = await self._call(
            method="describe_training_job",
            config={"TrainingJobName": best_name},
            inputs=resource_meta.inputs,
            region=resource_meta.region,
        )
        return (training_describe.get("ModelArtifacts") or {}).get("S3ModelArtifacts")

    async def get(self, resource_meta: SageMakerHyperParameterTuningJobMetadata, **kwargs) -> Resource:
        describe_response, _ = await self._call(
            method="describe_hyper_parameter_tuning_job",
            config={"HyperParameterTuningJobName": resource_meta.config.get("HyperParameterTuningJobName")},
            inputs=resource_meta.inputs,
            region=resource_meta.region,
        )

        current_state = describe_response.get("HyperParameterTuningJobStatus")
        flyte_phase = _STATE_MAP.get(current_state, TaskExecution.RUNNING)

        # While running we surface the trial counters so users see live progress
        # ("3 Completed / 1 InProgress / 0 Failed trials"). On terminal failure
        # FailureReason is the most useful single line.
        message: Optional[str] = None
        if current_state == "InProgress":
            message = _running_message(describe_response)
        elif current_state in ("Failed", "Stopped", "Deleting", "DeleteFailed"):
            message = describe_response.get("FailureReason") or _running_message(describe_response)

        outputs: Optional[Dict[str, Any]] = None
        if current_state == "Completed":
            s3_model_artifacts = await self._best_training_job_artifacts(describe_response, resource_meta)
            outputs = {"result": _build_outputs(describe_response, s3_model_artifacts)}

        return Resource(phase=flyte_phase, outputs=outputs, message=message)

    async def delete(self, resource_meta: SageMakerHyperParameterTuningJobMetadata, **kwargs):
        try:
            await self._call(
                method="stop_hyper_parameter_tuning_job",
                config={"HyperParameterTuningJobName": resource_meta.config.get("HyperParameterTuningJobName")},
                region=resource_meta.region,
                inputs=resource_meta.inputs,
            )
        except CustomException as e:
            original_exception = e.original_exception
            error_code = original_exception.response["Error"]["Code"]
            error_message = original_exception.response["Error"]["Message"]

            # Same swallow-on-already-terminal behaviour as the training connector.
            if error_code == "ResourceNotFound" or (
                error_code == "ValidationException" and "non-running" in error_message
            ):
                return
            raise e


ConnectorRegistry.register(SageMakerHyperParameterTuningJobConnector())

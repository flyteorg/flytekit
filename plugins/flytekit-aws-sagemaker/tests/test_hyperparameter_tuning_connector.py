from datetime import datetime, timedelta
from unittest import mock

import pytest
from botocore.exceptions import ClientError
from flyteidl.core.execution_pb2 import TaskExecution
from flytekitplugins.awssagemaker_hyperparameter_tuning.connector import (
    SageMakerHyperParameterTuningJobMetadata,
)
from flytekitplugins.awssagemaker_inference.boto3_mixin import CustomException

from flytekit.extend.backend.base_connector import ConnectorRegistry
from flytekit.interfaces.cli_identifiers import Identifier
from flytekit.models import literals
from flytekit.models.core.identifier import ResourceType
from flytekit.models.task import RuntimeMetadata, TaskMetadata, TaskTemplate

idempotence_token = "74443947857331f7"

REGION = "us-east-2"
TUNING_JOB_NAME = "xgb-tune-{idempotence_token}"
TUNING_JOB_ARN = (
    "arn:aws:sagemaker:us-east-2:1234567890:hyper-parameter-tuning-job/"
    "xgb-tune-74443947857331f7"
)
BEST_TRAINING_JOB_NAME = "xgb-tune-74443947857331f7-007-3f4a5b6c"
BEST_TRAINING_JOB_ARN = (
    "arn:aws:sagemaker:us-east-2:1234567890:training-job/"
    "xgb-tune-74443947857331f7-007-3f4a5b6c"
)
S3_MODEL_ARTIFACTS = (
    "s3://my-bucket/output/xgb-tune-74443947857331f7-007-3f4a5b6c/output/model.tar.gz"
)


def _task_config():
    return {
        "config": {
            "HyperParameterTuningJobName": TUNING_JOB_NAME,
            "HyperParameterTuningJobConfig": {
                "Strategy": "Bayesian",
                "HyperParameterTuningJobObjective": {
                    "Type": "Maximize",
                    "MetricName": "validation:auc",
                },
                "ResourceLimits": {
                    "MaxNumberOfTrainingJobs": 4,
                    "MaxParallelTrainingJobs": 2,
                },
                "ParameterRanges": {
                    "ContinuousParameterRanges": [
                        {"Name": "eta", "MinValue": "0.01", "MaxValue": "0.5"},
                    ],
                    "IntegerParameterRanges": [
                        {"Name": "max_depth", "MinValue": "3", "MaxValue": "9"},
                    ],
                },
            },
            "TrainingJobDefinition": {
                "AlgorithmSpecification": {
                    "TrainingImage": "{images.training_image}",
                    "TrainingInputMode": "File",
                    "MetricDefinitions": [
                        {"Name": "validation:auc", "Regex": "auc=([0-9\\.]+)"},
                    ],
                },
                "RoleArn": "{inputs.execution_role_arn}",
                "OutputDataConfig": {"S3OutputPath": "{inputs.output_prefix}"},
                "ResourceConfig": {
                    "InstanceType": "ml.m5.large",
                    "InstanceCount": 1,
                    "VolumeSizeInGB": 10,
                },
                "StoppingCondition": {"MaxRuntimeInSeconds": 1800},
            },
        },
        "region": REGION,
        "images": {
            "training_image": "1234567890.dkr.ecr.us-east-2.amazonaws.com/xgboost:latest"
        },
    }


def _task_template():
    task_id = Identifier(
        resource_type=ResourceType.TASK,
        project="project",
        domain="domain",
        name="name",
        version="version",
    )
    task_metadata = TaskMetadata(
        discoverable=True,
        runtime=RuntimeMetadata(RuntimeMetadata.RuntimeType.FLYTE_SDK, "1.0.0", "python"),
        timeout=timedelta(days=1),
        retries=literals.RetryStrategy(3),
        interruptible=True,
        discovery_version="0.1.1b0",
        deprecated_error_message="This is deprecated!",
        cache_serializable=True,
        pod_template_name="A",
        cache_ignore_input_vars=(),
    )
    return TaskTemplate(
        id=task_id,
        custom=_task_config(),
        metadata=task_metadata,
        interface=None,
        type="sagemaker-hyperparameter-tuning-job",
    )


def _completed_describe_tuning_response():
    return {
        "HyperParameterTuningJobName": "xgb-tune-74443947857331f7",
        "HyperParameterTuningJobArn": TUNING_JOB_ARN,
        "HyperParameterTuningJobStatus": "Completed",
        "TrainingJobStatusCounters": {
            "Completed": 4,
            "InProgress": 0,
            "RetryableError": 0,
            "NonRetryableError": 0,
            "Stopped": 0,
        },
        "ObjectiveStatusCounters": {"Succeeded": 4, "Pending": 0, "Failed": 0},
        "BestTrainingJob": {
            "TrainingJobName": BEST_TRAINING_JOB_NAME,
            "TrainingJobArn": BEST_TRAINING_JOB_ARN,
            "TrainingJobStatus": "Completed",
            "ObjectiveStatus": "Succeeded",
            "FinalHyperParameterTuningJobObjectiveMetric": {
                "MetricName": "validation:auc",
                "Value": 0.93,
            },
            "TunedHyperParameters": {"eta": "0.21", "max_depth": "7"},
            "TrainingStartTime": datetime(2026, 4, 30, 12, 0, 0),
            "TrainingEndTime": datetime(2026, 4, 30, 12, 8, 0),
        },
    }


def _describe_training_response():
    return {
        "TrainingJobName": BEST_TRAINING_JOB_NAME,
        "TrainingJobArn": BEST_TRAINING_JOB_ARN,
        "TrainingJobStatus": "Completed",
        "ModelArtifacts": {"S3ModelArtifacts": S3_MODEL_ARTIFACTS},
    }


def _routing_side_effect(method_to_response):
    """Build a side_effect that dispatches on the ``method`` kwarg of _call.

    The HPO connector's get() makes up to two boto3 calls per poll:
    describe_hyper_parameter_tuning_job, and on Completed, describe_training_job.
    Tests need to return the right payload for each.
    """

    async def _side_effect(*args, **kwargs):
        method = kwargs.get("method")
        if method not in method_to_response:
            raise AssertionError(f"unexpected _call(method={method!r})")
        return method_to_response[method]

    return _side_effect


@pytest.mark.asyncio
@mock.patch(
    "flytekitplugins.awssagemaker_hyperparameter_tuning.connector.Boto3ConnectorMixin._call"
)
async def test_create_get_delete_happy_path(mock_call):
    mock_call.side_effect = _routing_side_effect(
        {
            "create_hyper_parameter_tuning_job": (None, idempotence_token),
            "describe_hyper_parameter_tuning_job": (
                _completed_describe_tuning_response(),
                idempotence_token,
            ),
            "describe_training_job": (
                _describe_training_response(),
                idempotence_token,
            ),
            "stop_hyper_parameter_tuning_job": (None, idempotence_token),
        }
    )

    connector = ConnectorRegistry.get_connector("sagemaker-hyperparameter-tuning-job")
    metadata = SageMakerHyperParameterTuningJobMetadata(
        config=_task_config()["config"], region=REGION
    )

    response = await connector.create(_task_template())
    assert response == metadata

    resource = await connector.get(metadata)
    assert resource.phase == TaskExecution.SUCCEEDED

    result = resource.outputs["result"]
    assert result["HyperParameterTuningJobArn"] == TUNING_JOB_ARN
    assert result["HyperParameterTuningJobName"] == "xgb-tune-74443947857331f7"

    # BestTrainingJob carries the metric, tuned params, and — crucially — the
    # S3ModelArtifacts resolved via the follow-up describe_training_job call.
    best = result["BestTrainingJob"]
    assert best["TrainingJobName"] == BEST_TRAINING_JOB_NAME
    assert best["TrainingJobArn"] == BEST_TRAINING_JOB_ARN
    assert best["ObjectiveStatus"] == "Succeeded"
    assert best["FinalHyperParameterTuningJobObjectiveMetric"] == {
        "MetricName": "validation:auc",
        "Value": 0.93,
    }
    assert best["TunedHyperParameters"] == {"eta": "0.21", "max_depth": "7"}
    assert best["ModelArtifacts"] == {"S3ModelArtifacts": S3_MODEL_ARTIFACTS}
    assert best["TrainingStartTime"] == "2026-04-30T12:00:00"
    assert best["TrainingEndTime"] == "2026-04-30T12:08:00"

    # Top-level convenience copy of the best artifacts so callers can consume
    # `result["ModelArtifacts"]["S3ModelArtifacts"]` symmetric with training.
    assert result["ModelArtifacts"] == {"S3ModelArtifacts": S3_MODEL_ARTIFACTS}

    assert result["TrainingJobStatusCounters"]["Completed"] == 4
    assert result["ObjectiveStatusCounters"] == {
        "Succeeded": 4,
        "Pending": 0,
        "Failed": 0,
    }

    assert await connector.delete(metadata) is None


@pytest.mark.asyncio
@mock.patch(
    "flytekitplugins.awssagemaker_hyperparameter_tuning.connector.Boto3ConnectorMixin._call"
)
async def test_get_inprogress_surfaces_counter_summary(mock_call):
    """While running we surface a compact 'N Completed / M InProgress / K Failed' line."""
    mock_call.side_effect = _routing_side_effect(
        {
            "describe_hyper_parameter_tuning_job": (
                {
                    "HyperParameterTuningJobName": "xgb-tune-x",
                    "HyperParameterTuningJobArn": TUNING_JOB_ARN,
                    "HyperParameterTuningJobStatus": "InProgress",
                    "TrainingJobStatusCounters": {
                        "Completed": 3,
                        "InProgress": 1,
                        "RetryableError": 0,
                        "NonRetryableError": 1,
                        "Stopped": 0,
                    },
                },
                idempotence_token,
            ),
        }
    )

    connector = ConnectorRegistry.get_connector("sagemaker-hyperparameter-tuning-job")
    metadata = SageMakerHyperParameterTuningJobMetadata(
        config={"HyperParameterTuningJobName": "xgb-tune-x"}, region=REGION
    )

    resource = await connector.get(metadata)
    assert resource.phase == TaskExecution.RUNNING
    assert resource.message == "3 Completed / 1 InProgress / 1 Failed trials"
    assert resource.outputs is None


@pytest.mark.asyncio
@mock.patch(
    "flytekitplugins.awssagemaker_hyperparameter_tuning.connector.Boto3ConnectorMixin._call"
)
async def test_get_failed_surfaces_failure_reason(mock_call):
    mock_call.side_effect = _routing_side_effect(
        {
            "describe_hyper_parameter_tuning_job": (
                {
                    "HyperParameterTuningJobName": "xgb-tune-x",
                    "HyperParameterTuningJobArn": TUNING_JOB_ARN,
                    "HyperParameterTuningJobStatus": "Failed",
                    "FailureReason": "All trials failed with ClientError",
                },
                idempotence_token,
            ),
        }
    )

    connector = ConnectorRegistry.get_connector("sagemaker-hyperparameter-tuning-job")
    metadata = SageMakerHyperParameterTuningJobMetadata(
        config={"HyperParameterTuningJobName": "xgb-tune-x"}, region=REGION
    )

    resource = await connector.get(metadata)
    assert resource.phase == TaskExecution.FAILED
    assert resource.message == "All trials failed with ClientError"
    assert resource.outputs is None


@pytest.mark.asyncio
@mock.patch(
    "flytekitplugins.awssagemaker_hyperparameter_tuning.connector.Boto3ConnectorMixin._call"
)
async def test_get_completed_propagates_describe_training_failure(mock_call):
    """The promised best-model artifact must not silently become ``None``."""

    async def _side_effect(*args, **kwargs):
        method = kwargs.get("method")
        if method == "describe_hyper_parameter_tuning_job":
            return (_completed_describe_tuning_response(), idempotence_token)
        if method == "describe_training_job":
            raise CustomException(
                message="An error occurred",
                idempotence_token=idempotence_token,
                original_exception=ClientError(
                    error_response={
                        "Error": {
                            "Code": "AccessDeniedException",
                            "Message": "secondary describe blocked",
                        }
                    },
                    operation_name="DescribeTrainingJob",
                ),
            )
        raise AssertionError(f"unexpected _call(method={method!r})")

    mock_call.side_effect = _side_effect

    connector = ConnectorRegistry.get_connector("sagemaker-hyperparameter-tuning-job")
    metadata = SageMakerHyperParameterTuningJobMetadata(
        config={"HyperParameterTuningJobName": "xgb-tune-x"}, region=REGION
    )
    with pytest.raises(CustomException):
        await connector.get(metadata)


@pytest.mark.asyncio
@mock.patch(
    "flytekitplugins.awssagemaker_hyperparameter_tuning.connector.Boto3ConnectorMixin._call"
)
async def test_get_completed_without_best_training_job(mock_call):
    """Edge case: completed tuning with no BestTrainingJob (every trial failed
    its objective). We still return a structured result with None artifacts."""
    mock_call.side_effect = _routing_side_effect(
        {
            "describe_hyper_parameter_tuning_job": (
                {
                    "HyperParameterTuningJobName": "xgb-tune-x",
                    "HyperParameterTuningJobArn": TUNING_JOB_ARN,
                    "HyperParameterTuningJobStatus": "Completed",
                    "TrainingJobStatusCounters": {
                        "Completed": 4,
                        "InProgress": 0,
                        "RetryableError": 0,
                        "NonRetryableError": 0,
                        "Stopped": 0,
                    },
                    "ObjectiveStatusCounters": {
                        "Succeeded": 0,
                        "Pending": 0,
                        "Failed": 4,
                    },
                },
                idempotence_token,
            ),
        }
    )

    connector = ConnectorRegistry.get_connector("sagemaker-hyperparameter-tuning-job")
    metadata = SageMakerHyperParameterTuningJobMetadata(
        config={"HyperParameterTuningJobName": "xgb-tune-x"}, region=REGION
    )
    resource = await connector.get(metadata)
    assert resource.phase == TaskExecution.SUCCEEDED

    result = resource.outputs["result"]
    assert result["BestTrainingJob"]["TrainingJobName"] is None
    assert result["BestTrainingJob"]["ModelArtifacts"] == {"S3ModelArtifacts": None}
    assert result["ModelArtifacts"] == {"S3ModelArtifacts": None}
    assert result["ObjectiveStatusCounters"]["Failed"] == 4


@pytest.mark.asyncio
@mock.patch(
    "flytekitplugins.awssagemaker_hyperparameter_tuning.connector.Boto3ConnectorMixin._call"
)
async def test_get_stopped_maps_to_failed(mock_call):
    mock_call.side_effect = _routing_side_effect(
        {
            "describe_hyper_parameter_tuning_job": (
                {
                    "HyperParameterTuningJobName": "xgb-tune-x",
                    "HyperParameterTuningJobArn": TUNING_JOB_ARN,
                    "HyperParameterTuningJobStatus": "Stopped",
                    "FailureReason": "User requested stop",
                },
                idempotence_token,
            ),
        }
    )

    connector = ConnectorRegistry.get_connector("sagemaker-hyperparameter-tuning-job")
    metadata = SageMakerHyperParameterTuningJobMetadata(
        config={"HyperParameterTuningJobName": "xgb-tune-x"}, region=REGION
    )

    resource = await connector.get(metadata)
    assert resource.phase == TaskExecution.FAILED
    assert resource.message == "User requested stop"


@pytest.mark.asyncio
@mock.patch(
    "flytekitplugins.awssagemaker_hyperparameter_tuning.connector.Boto3ConnectorMixin._call"
)
async def test_create_already_exists_returns_metadata(mock_call):
    mock_call.side_effect = CustomException(
        message="An error occurred",
        idempotence_token=idempotence_token,
        original_exception=ClientError(
            error_response={
                "Error": {
                    "Code": "ResourceInUse",
                    "Message": "Hyperparameter tuning job xgb-tune-74443947857331f7 already exists",
                }
            },
            operation_name="CreateHyperParameterTuningJob",
        ),
    )

    connector = ConnectorRegistry.get_connector("sagemaker-hyperparameter-tuning-job")
    response = await connector.create(_task_template())
    assert response.config == _task_config()["config"]
    assert response.region == REGION


@pytest.mark.asyncio
@mock.patch(
    "flytekitplugins.awssagemaker_hyperparameter_tuning.connector.Boto3ConnectorMixin._call"
)
async def test_create_resource_limit_propagates(mock_call):
    mock_call.side_effect = CustomException(
        message="An error occurred",
        idempotence_token=idempotence_token,
        original_exception=ClientError(
            error_response={
                "Error": {
                    "Code": "ResourceLimitExceeded",
                    "Message": (
                        "The account-level service limit ... has been reached. "
                        "Please use AWS Service Quotas to request an increase for this quota."
                    ),
                }
            },
            operation_name="CreateHyperParameterTuningJob",
        ),
    )

    connector = ConnectorRegistry.get_connector("sagemaker-hyperparameter-tuning-job")
    with pytest.raises(CustomException):
        await connector.create(_task_template())


@pytest.mark.asyncio
@mock.patch(
    "flytekitplugins.awssagemaker_hyperparameter_tuning.connector.Boto3ConnectorMixin._call"
)
async def test_create_unknown_error_propagates(mock_call):
    mock_call.side_effect = CustomException(
        message="An error occurred",
        idempotence_token=idempotence_token,
        original_exception=ClientError(
            error_response={
                "Error": {"Code": "AccessDeniedException", "Message": "nope"}
            },
            operation_name="CreateHyperParameterTuningJob",
        ),
    )

    connector = ConnectorRegistry.get_connector("sagemaker-hyperparameter-tuning-job")
    with pytest.raises(CustomException):
        await connector.create(_task_template())


@pytest.mark.asyncio
@mock.patch(
    "flytekitplugins.awssagemaker_hyperparameter_tuning.connector.Boto3ConnectorMixin._call"
)
async def test_delete_swallows_terminal_job_error(mock_call):
    mock_call.side_effect = CustomException(
        message="An error occurred",
        idempotence_token=idempotence_token,
        original_exception=ClientError(
            error_response={
                "Error": {
                    "Code": "ValidationException",
                    "Message": (
                        "The request was rejected because the hyperparameter "
                        "tuning job is not in a non-running state"
                    ),
                }
            },
            operation_name="StopHyperParameterTuningJob",
        ),
    )

    connector = ConnectorRegistry.get_connector("sagemaker-hyperparameter-tuning-job")
    metadata = SageMakerHyperParameterTuningJobMetadata(
        config={"HyperParameterTuningJobName": "xgb-tune-x"}, region=REGION
    )
    assert await connector.delete(metadata) is None


@pytest.mark.asyncio
@mock.patch(
    "flytekitplugins.awssagemaker_hyperparameter_tuning.connector.Boto3ConnectorMixin._call"
)
async def test_delete_swallows_resource_not_found(mock_call):
    mock_call.side_effect = CustomException(
        message="An error occurred",
        idempotence_token=idempotence_token,
        original_exception=ClientError(
            error_response={
                "Error": {
                    "Code": "ResourceNotFound",
                    "Message": "Hyperparameter tuning job does not exist",
                }
            },
            operation_name="StopHyperParameterTuningJob",
        ),
    )

    connector = ConnectorRegistry.get_connector("sagemaker-hyperparameter-tuning-job")
    metadata = SageMakerHyperParameterTuningJobMetadata(
        config={"HyperParameterTuningJobName": "xgb-tune-x"}, region=REGION
    )
    assert await connector.delete(metadata) is None


@pytest.mark.asyncio
@mock.patch(
    "flytekitplugins.awssagemaker_hyperparameter_tuning.connector.Boto3ConnectorMixin._call"
)
async def test_delete_propagates_other_errors(mock_call):
    mock_call.side_effect = CustomException(
        message="An error occurred",
        idempotence_token=idempotence_token,
        original_exception=ClientError(
            error_response={
                "Error": {"Code": "AccessDeniedException", "Message": "nope"}
            },
            operation_name="StopHyperParameterTuningJob",
        ),
    )

    connector = ConnectorRegistry.get_connector("sagemaker-hyperparameter-tuning-job")
    metadata = SageMakerHyperParameterTuningJobMetadata(
        config={"HyperParameterTuningJobName": "xgb-tune-x"}, region=REGION
    )
    with pytest.raises(CustomException):
        await connector.delete(metadata)

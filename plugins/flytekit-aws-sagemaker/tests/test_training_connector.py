from datetime import datetime, timedelta
from unittest import mock

import pytest
from botocore.exceptions import ClientError
from flyteidl.core.execution_pb2 import TaskExecution
from flytekitplugins.awssagemaker_inference.boto3_mixin import CustomException
from flytekitplugins.awssagemaker_training.connector import (
    SageMakerTrainingJobMetadata,
)

from flytekit.extend.backend.base_connector import ConnectorRegistry
from flytekit.interfaces.cli_identifiers import Identifier
from flytekit.models import literals
from flytekit.models.core.identifier import ResourceType
from flytekit.models.task import RuntimeMetadata, TaskMetadata, TaskTemplate

idempotence_token = "74443947857331f7"

REGION = "us-east-2"
TRAINING_JOB_NAME = "xgb-{idempotence_token}"
TRAINING_JOB_ARN = (
    "arn:aws:sagemaker:us-east-2:1234567890:training-job/xgb-74443947857331f7"
)
S3_MODEL_ARTIFACTS = "s3://my-bucket/output/xgb-74443947857331f7/output/model.tar.gz"


def _task_config():
    return {
        "config": {
            "TrainingJobName": TRAINING_JOB_NAME,
            "AlgorithmSpecification": {
                "TrainingImage": "{images.training_image}",
                "TrainingInputMode": "File",
            },
            "RoleArn": "{inputs.execution_role_arn}",
            "OutputDataConfig": {"S3OutputPath": "{inputs.output_prefix}"},
            "ResourceConfig": {
                "InstanceType": "ml.m5.xlarge",
                "InstanceCount": 1,
                "VolumeSizeInGB": 30,
            },
            "StoppingCondition": {"MaxRuntimeInSeconds": 3600},
        },
        "region": REGION,
        "images": {"training_image": "1234567890.dkr.ecr.us-east-2.amazonaws.com/xgboost:latest"},
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
        type="sagemaker-training-job",
    )


def _completed_describe_response():
    return {
        "TrainingJobName": "xgb-74443947857331f7",
        "TrainingJobArn": TRAINING_JOB_ARN,
        "TrainingJobStatus": "Completed",
        "SecondaryStatus": "Completed",
        "ModelArtifacts": {"S3ModelArtifacts": S3_MODEL_ARTIFACTS},
        "OutputDataConfig": {"S3OutputPath": "s3://my-bucket/output/"},
        "FinalMetricDataList": [
            {
                "MetricName": "validation:auc",
                "Value": 0.87,
                "Timestamp": datetime(2026, 4, 30, 12, 0, 0),
            }
        ],
        "BillableTimeInSeconds": 120,
        "TrainingTimeInSeconds": 100,
    }


@pytest.mark.asyncio
@mock.patch("flytekitplugins.awssagemaker_training.connector.Boto3ConnectorMixin._call")
async def test_create_get_delete_happy_path(mock_call):
    mock_call.return_value = (_completed_describe_response(), idempotence_token)

    connector = ConnectorRegistry.get_connector("sagemaker-training-job")
    metadata = SageMakerTrainingJobMetadata(
        config=_task_config()["config"], region=REGION
    )

    # CREATE — returns metadata; mock return value is ignored by create().
    response = await connector.create(_task_template())
    assert response == metadata

    # GET — parses describe response, returns Completed with structured outputs.
    resource = await connector.get(metadata)
    assert resource.phase == TaskExecution.SUCCEEDED

    result = resource.outputs["result"]
    assert result["TrainingJobArn"] == TRAINING_JOB_ARN
    assert result["TrainingJobName"] == "xgb-74443947857331f7"
    assert result["ModelArtifacts"] == {"S3ModelArtifacts": S3_MODEL_ARTIFACTS}
    assert result["OutputDataConfig"] == {"S3OutputPath": "s3://my-bucket/output/"}
    assert result["BillableTimeInSeconds"] == 120
    assert result["TrainingTimeInSeconds"] == 100

    # FinalMetricDataList timestamps must be ISO strings (datetime is not JSON-friendly).
    assert result["FinalMetricDataList"] == [
        {
            "MetricName": "validation:auc",
            "Value": 0.87,
            "Timestamp": "2026-04-30T12:00:00",
        }
    ]

    # DELETE — happy path returns None.
    assert await connector.delete(metadata) is None


@pytest.mark.asyncio
@mock.patch("flytekitplugins.awssagemaker_training.connector.Boto3ConnectorMixin._call")
async def test_get_inprogress_surfaces_secondary_status(mock_call):
    mock_call.return_value = (
        {
            "TrainingJobName": "xgb-x",
            "TrainingJobArn": TRAINING_JOB_ARN,
            "TrainingJobStatus": "InProgress",
            "SecondaryStatus": "Downloading",
        },
        idempotence_token,
    )

    connector = ConnectorRegistry.get_connector("sagemaker-training-job")
    metadata = SageMakerTrainingJobMetadata(
        config={"TrainingJobName": "xgb-x"}, region=REGION
    )

    resource = await connector.get(metadata)
    assert resource.phase == TaskExecution.RUNNING
    assert resource.message == "Downloading"
    assert resource.outputs is None


@pytest.mark.asyncio
@mock.patch("flytekitplugins.awssagemaker_training.connector.Boto3ConnectorMixin._call")
async def test_get_failed_surfaces_failure_reason(mock_call):
    mock_call.return_value = (
        {
            "TrainingJobName": "xgb-x",
            "TrainingJobArn": TRAINING_JOB_ARN,
            "TrainingJobStatus": "Failed",
            "FailureReason": "AlgorithmError: out of memory",
        },
        idempotence_token,
    )

    connector = ConnectorRegistry.get_connector("sagemaker-training-job")
    metadata = SageMakerTrainingJobMetadata(
        config={"TrainingJobName": "xgb-x"}, region=REGION
    )

    resource = await connector.get(metadata)
    assert resource.phase == TaskExecution.FAILED
    assert resource.message == "AlgorithmError: out of memory"
    assert resource.outputs is None


@pytest.mark.asyncio
@mock.patch("flytekitplugins.awssagemaker_training.connector.Boto3ConnectorMixin._call")
async def test_get_stopped_maps_to_failed(mock_call):
    mock_call.return_value = (
        {
            "TrainingJobName": "xgb-x",
            "TrainingJobArn": TRAINING_JOB_ARN,
            "TrainingJobStatus": "Stopped",
            "FailureReason": "MaxRuntimeExceeded",
        },
        idempotence_token,
    )

    connector = ConnectorRegistry.get_connector("sagemaker-training-job")
    metadata = SageMakerTrainingJobMetadata(
        config={"TrainingJobName": "xgb-x"}, region=REGION
    )

    resource = await connector.get(metadata)
    assert resource.phase == TaskExecution.FAILED
    assert resource.message == "MaxRuntimeExceeded"


@pytest.mark.asyncio
@mock.patch("flytekitplugins.awssagemaker_training.connector.Boto3ConnectorMixin._call")
async def test_create_already_exists_returns_metadata(mock_call):
    mock_call.side_effect = CustomException(
        message="An error occurred",
        idempotence_token=idempotence_token,
        original_exception=ClientError(
            error_response={
                "Error": {
                    "Code": "ResourceInUse",
                    "Message": "Training job xgb-74443947857331f7 already exists",
                }
            },
            operation_name="CreateTrainingJob",
        ),
    )

    connector = ConnectorRegistry.get_connector("sagemaker-training-job")
    response = await connector.create(_task_template())
    assert response.config == _task_config()["config"]
    assert response.region == REGION


@pytest.mark.asyncio
@mock.patch("flytekitplugins.awssagemaker_training.connector.Boto3ConnectorMixin._call")
async def test_create_static_name_resource_in_use_propagates(mock_call):
    mock_call.side_effect = CustomException(
        message="An error occurred",
        idempotence_token="",
        original_exception=ClientError(
            error_response={
                "Error": {
                    "Code": "ResourceInUse",
                    "Message": "Training job static-name already exists",
                }
            },
            operation_name="CreateTrainingJob",
        ),
    )

    connector = ConnectorRegistry.get_connector("sagemaker-training-job")
    with pytest.raises(CustomException):
        await connector.create(_task_template())


@pytest.mark.asyncio
@mock.patch("flytekitplugins.awssagemaker_training.connector.Boto3ConnectorMixin._call")
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
            operation_name="CreateTrainingJob",
        ),
    )

    connector = ConnectorRegistry.get_connector("sagemaker-training-job")
    with pytest.raises(CustomException):
        await connector.create(_task_template())


@pytest.mark.asyncio
@mock.patch("flytekitplugins.awssagemaker_training.connector.Boto3ConnectorMixin._call")
async def test_create_unknown_error_propagates(mock_call):
    mock_call.side_effect = CustomException(
        message="An error occurred",
        idempotence_token=idempotence_token,
        original_exception=ClientError(
            error_response={
                "Error": {"Code": "AccessDeniedException", "Message": "nope"}
            },
            operation_name="CreateTrainingJob",
        ),
    )

    connector = ConnectorRegistry.get_connector("sagemaker-training-job")
    with pytest.raises(CustomException):
        await connector.create(_task_template())


@pytest.mark.asyncio
@mock.patch("flytekitplugins.awssagemaker_training.connector.Boto3ConnectorMixin._call")
async def test_delete_swallows_terminal_job_error(mock_call):
    """If Flyte calls delete() after the job naturally finished, stop_training_job
    raises ValidationException — the connector must swallow that specific case."""
    mock_call.side_effect = CustomException(
        message="An error occurred",
        idempotence_token=idempotence_token,
        original_exception=ClientError(
            error_response={
                "Error": {
                    "Code": "ValidationException",
                    "Message": "The request was rejected because the training job is not in a non-running state",
                }
            },
            operation_name="StopTrainingJob",
        ),
    )

    connector = ConnectorRegistry.get_connector("sagemaker-training-job")
    metadata = SageMakerTrainingJobMetadata(
        config={"TrainingJobName": "xgb-x"}, region=REGION
    )

    # Should NOT raise.
    assert await connector.delete(metadata) is None


@pytest.mark.asyncio
@mock.patch("flytekitplugins.awssagemaker_training.connector.Boto3ConnectorMixin._call")
async def test_delete_swallows_resource_not_found(mock_call):
    mock_call.side_effect = CustomException(
        message="An error occurred",
        idempotence_token=idempotence_token,
        original_exception=ClientError(
            error_response={
                "Error": {
                    "Code": "ResourceNotFound",
                    "Message": "Training job does not exist",
                }
            },
            operation_name="StopTrainingJob",
        ),
    )

    connector = ConnectorRegistry.get_connector("sagemaker-training-job")
    metadata = SageMakerTrainingJobMetadata(
        config={"TrainingJobName": "xgb-x"}, region=REGION
    )
    assert await connector.delete(metadata) is None


@pytest.mark.asyncio
@mock.patch("flytekitplugins.awssagemaker_training.connector.Boto3ConnectorMixin._call")
async def test_delete_propagates_other_errors(mock_call):
    mock_call.side_effect = CustomException(
        message="An error occurred",
        idempotence_token=idempotence_token,
        original_exception=ClientError(
            error_response={
                "Error": {"Code": "AccessDeniedException", "Message": "nope"}
            },
            operation_name="StopTrainingJob",
        ),
    )

    connector = ConnectorRegistry.get_connector("sagemaker-training-job")
    metadata = SageMakerTrainingJobMetadata(
        config={"TrainingJobName": "xgb-x"}, region=REGION
    )

    with pytest.raises(CustomException):
        await connector.delete(metadata)

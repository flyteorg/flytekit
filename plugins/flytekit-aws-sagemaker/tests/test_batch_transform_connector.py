from datetime import datetime, timedelta
from unittest import mock

import pytest
from botocore.exceptions import ClientError
from flyteidl.core.execution_pb2 import TaskExecution
from flytekitplugins.awssagemaker_batch_transform.connector import (
    SageMakerTransformJobMetadata,
)
from flytekitplugins.awssagemaker_inference.boto3_mixin import CustomException

from flytekit.extend.backend.base_connector import ConnectorRegistry
from flytekit.interfaces.cli_identifiers import Identifier
from flytekit.models import literals
from flytekit.models.core.identifier import ResourceType
from flytekit.models.task import RuntimeMetadata, TaskMetadata, TaskTemplate

idempotence_token = "74443947857331f7"

REGION = "us-east-2"
TRANSFORM_JOB_ARN = (
    "arn:aws:sagemaker:us-east-2:1234567890:transform-job/score-74443947857331f7"
)
S3_OUTPUT_PATH = "s3://my-bucket/predictions/score-74443947857331f7/"


def _task_config():
    return {
        "config": {
            "TransformJobName": "score-{idempotence_token}",
            "ModelName": "{inputs.model_name}",
            "TransformInput": {
                "DataSource": {
                    "S3DataSource": {
                        "S3DataType": "S3Prefix",
                        "S3Uri": "{inputs.input_data}",
                    }
                },
                "ContentType": "text/csv",
                "SplitType": "Line",
            },
            "TransformOutput": {
                "S3OutputPath": "{inputs.output_prefix}",
                "AssembleWith": "Line",
            },
            "TransformResources": {
                "InstanceType": "ml.m5.xlarge",
                "InstanceCount": 1,
            },
            "DataProcessing": {"JoinSource": "Input"},
        },
        "region": REGION,
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
        type="sagemaker-transform-job",
    )


def _completed_describe_response():
    return {
        "TransformJobName": "score-74443947857331f7",
        "TransformJobArn": TRANSFORM_JOB_ARN,
        "TransformJobStatus": "Completed",
        "ModelName": "ranker-prod",
        "TransformOutput": {"S3OutputPath": S3_OUTPUT_PATH, "AssembleWith": "Line"},
        "TransformStartTime": datetime(2026, 4, 30, 10, 0, 0),
        "TransformEndTime": datetime(2026, 4, 30, 10, 12, 0),
    }


@pytest.mark.asyncio
@mock.patch("flytekitplugins.awssagemaker_batch_transform.connector.Boto3ConnectorMixin._call")
async def test_create_get_delete_happy_path(mock_call):
    mock_call.return_value = (_completed_describe_response(), idempotence_token)

    connector = ConnectorRegistry.get_connector("sagemaker-transform-job")
    metadata = SageMakerTransformJobMetadata(
        config=_task_config()["config"], region=REGION
    )

    response = await connector.create(_task_template())
    assert response == metadata

    resource = await connector.get(metadata)
    assert resource.phase == TaskExecution.SUCCEEDED

    result = resource.outputs["result"]
    assert result["TransformJobArn"] == TRANSFORM_JOB_ARN
    assert result["TransformJobName"] == "score-74443947857331f7"
    assert result["ModelName"] == "ranker-prod"
    assert result["TransformOutput"] == {"S3OutputPath": S3_OUTPUT_PATH}
    assert result["TransformStartTime"] == "2026-04-30T10:00:00"
    assert result["TransformEndTime"] == "2026-04-30T10:12:00"

    assert await connector.delete(metadata) is None


@pytest.mark.asyncio
@mock.patch("flytekitplugins.awssagemaker_batch_transform.connector.Boto3ConnectorMixin._call")
async def test_get_inprogress_no_message(mock_call):
    """Transform jobs have no SecondaryStatus, so message stays None during InProgress."""
    mock_call.return_value = (
        {
            "TransformJobName": "score-x",
            "TransformJobArn": TRANSFORM_JOB_ARN,
            "TransformJobStatus": "InProgress",
        },
        idempotence_token,
    )

    connector = ConnectorRegistry.get_connector("sagemaker-transform-job")
    metadata = SageMakerTransformJobMetadata(
        config={"TransformJobName": "score-x"}, region=REGION
    )

    resource = await connector.get(metadata)
    assert resource.phase == TaskExecution.RUNNING
    assert resource.message is None
    assert resource.outputs is None


@pytest.mark.asyncio
@mock.patch("flytekitplugins.awssagemaker_batch_transform.connector.Boto3ConnectorMixin._call")
async def test_get_failed_surfaces_failure_reason(mock_call):
    mock_call.return_value = (
        {
            "TransformJobName": "score-x",
            "TransformJobArn": TRANSFORM_JOB_ARN,
            "TransformJobStatus": "Failed",
            "FailureReason": "ClientError: container exited with code 1",
        },
        idempotence_token,
    )

    connector = ConnectorRegistry.get_connector("sagemaker-transform-job")
    metadata = SageMakerTransformJobMetadata(
        config={"TransformJobName": "score-x"}, region=REGION
    )

    resource = await connector.get(metadata)
    assert resource.phase == TaskExecution.FAILED
    assert resource.message == "ClientError: container exited with code 1"


@pytest.mark.asyncio
@mock.patch("flytekitplugins.awssagemaker_batch_transform.connector.Boto3ConnectorMixin._call")
async def test_create_already_exists_returns_metadata(mock_call):
    mock_call.side_effect = CustomException(
        message="An error occurred",
        idempotence_token=idempotence_token,
        original_exception=ClientError(
            error_response={
                "Error": {
                    "Code": "ResourceInUse",
                    "Message": "Transform job score-74443947857331f7 already exists",
                }
            },
            operation_name="CreateTransformJob",
        ),
    )

    connector = ConnectorRegistry.get_connector("sagemaker-transform-job")
    response = await connector.create(_task_template())
    assert response.config == _task_config()["config"]
    assert response.region == REGION


@pytest.mark.asyncio
@mock.patch("flytekitplugins.awssagemaker_batch_transform.connector.Boto3ConnectorMixin._call")
async def test_create_resource_limit_propagates(mock_call):
    mock_call.side_effect = CustomException(
        message="An error occurred",
        idempotence_token=idempotence_token,
        original_exception=ClientError(
            error_response={
                "Error": {
                    "Code": "ResourceLimitExceeded",
                    "Message": "Transform job quota exceeded",
                }
            },
            operation_name="CreateTransformJob",
        ),
    )

    connector = ConnectorRegistry.get_connector("sagemaker-transform-job")
    with pytest.raises(CustomException):
        await connector.create(_task_template())


@pytest.mark.asyncio
@mock.patch("flytekitplugins.awssagemaker_batch_transform.connector.Boto3ConnectorMixin._call")
async def test_delete_swallows_terminal_job_error(mock_call):
    mock_call.side_effect = CustomException(
        message="An error occurred",
        idempotence_token=idempotence_token,
        original_exception=ClientError(
            error_response={
                "Error": {
                    "Code": "ValidationException",
                    "Message": "The request was rejected because the transform job is not in a non-running state",
                }
            },
            operation_name="StopTransformJob",
        ),
    )

    connector = ConnectorRegistry.get_connector("sagemaker-transform-job")
    metadata = SageMakerTransformJobMetadata(
        config={"TransformJobName": "score-x"}, region=REGION
    )

    assert await connector.delete(metadata) is None


@pytest.mark.asyncio
@mock.patch("flytekitplugins.awssagemaker_batch_transform.connector.Boto3ConnectorMixin._call")
async def test_delete_swallows_resource_not_found(mock_call):
    mock_call.side_effect = CustomException(
        message="An error occurred",
        idempotence_token=idempotence_token,
        original_exception=ClientError(
            error_response={
                "Error": {
                    "Code": "ResourceNotFound",
                    "Message": "Transform job does not exist",
                }
            },
            operation_name="StopTransformJob",
        ),
    )

    connector = ConnectorRegistry.get_connector("sagemaker-transform-job")
    metadata = SageMakerTransformJobMetadata(
        config={"TransformJobName": "score-x"}, region=REGION
    )
    assert await connector.delete(metadata) is None

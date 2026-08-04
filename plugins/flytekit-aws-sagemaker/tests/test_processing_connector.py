from datetime import datetime, timedelta
from unittest import mock

import pytest
from botocore.exceptions import ClientError
from flyteidl.core.execution_pb2 import TaskExecution
from flytekitplugins.awssagemaker_inference.boto3_mixin import CustomException
from flytekitplugins.awssagemaker_processing.connector import (
    SageMakerProcessingJobMetadata,
    _build_outputs,
)

from flytekit.extend.backend.base_connector import ConnectorRegistry
from flytekit.interfaces.cli_identifiers import Identifier
from flytekit.models import literals
from flytekit.models.core.identifier import ResourceType
from flytekit.models.task import RuntimeMetadata, TaskMetadata, TaskTemplate

idempotence_token = "74443947857331f7"

REGION = "us-east-2"
PROCESSING_JOB_NAME = "prep-{idempotence_token}"
PROCESSING_JOB_ARN = (
    "arn:aws:sagemaker:us-east-2:1234567890:processing-job/prep-74443947857331f7"
)
S3_OUTPUT = "s3://my-bucket/processing/prep-74443947857331f7/output/train"


def _task_config():
    return {
        "config": {
            "ProcessingJobName": PROCESSING_JOB_NAME,
            "AppSpecification": {
                "ImageUri": "{images.processing_image}",
                "ContainerEntrypoint": ["python3", "/opt/ml/processing/preprocess.py"],
            },
            "RoleArn": "{inputs.execution_role_arn}",
            "ProcessingResources": {
                "ClusterConfig": {
                    "InstanceType": "ml.m5.xlarge",
                    "InstanceCount": 1,
                    "VolumeSizeInGB": 30,
                }
            },
            "ProcessingOutputConfig": {
                "Outputs": [
                    {
                        "OutputName": "train",
                        "S3Output": {
                            "S3Uri": "{inputs.output_prefix}",
                            "LocalPath": "/opt/ml/processing/output",
                            "S3UploadMode": "EndOfJob",
                        },
                    }
                ]
            },
            "StoppingCondition": {"MaxRuntimeInSeconds": 3600},
        },
        "region": REGION,
        "images": {"processing_image": "1234567890.dkr.ecr.us-east-2.amazonaws.com/sklearn:latest"},
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
        type="sagemaker-processing-job",
    )


def _completed_describe_response():
    return {
        "ProcessingJobName": "prep-74443947857331f7",
        "ProcessingJobArn": PROCESSING_JOB_ARN,
        "ProcessingJobStatus": "Completed",
        "ProcessingOutputConfig": {
            "Outputs": [
                {
                    "OutputName": "train",
                    "S3Output": {"S3Uri": S3_OUTPUT, "LocalPath": "/opt/ml/processing/output"},
                }
            ]
        },
        "ExitMessage": "Completed: Job completed successfully",
        "ProcessingStartTime": datetime(2026, 4, 30, 12, 0, 0),
        "ProcessingEndTime": datetime(2026, 4, 30, 12, 5, 0),
    }


def test_build_outputs_preserves_feature_store_destination():
    result = _build_outputs(
        {
            "ProcessingOutputConfig": {
                "Outputs": [
                    {
                        "OutputName": "features",
                        "FeatureStoreOutput": {"FeatureGroupName": "customer-features"},
                    }
                ]
            }
        }
    )

    assert result["Outputs"] == [
        {
            "OutputName": "features",
            "FeatureGroupName": "customer-features",
        }
    ]


@pytest.mark.asyncio
@mock.patch("flytekitplugins.awssagemaker_processing.connector.Boto3ConnectorMixin._call")
async def test_create_get_delete_happy_path(mock_call):
    mock_call.return_value = (_completed_describe_response(), idempotence_token)

    connector = ConnectorRegistry.get_connector("sagemaker-processing-job")
    metadata = SageMakerProcessingJobMetadata(
        config=_task_config()["config"], region=REGION
    )

    # CREATE — returns metadata; mock return value is ignored by create().
    response = await connector.create(_task_template())
    assert response == metadata

    # GET — parses describe response, returns Completed with structured outputs.
    resource = await connector.get(metadata)
    assert resource.phase == TaskExecution.SUCCEEDED

    result = resource.outputs["result"]
    assert result["ProcessingJobArn"] == PROCESSING_JOB_ARN
    assert result["ProcessingJobName"] == "prep-74443947857331f7"
    assert result["Outputs"] == [{"OutputName": "train", "S3Uri": S3_OUTPUT}]
    assert result["ExitMessage"] == "Completed: Job completed successfully"

    # Timestamps must be ISO strings (datetime is not JSON-friendly).
    assert result["ProcessingStartTime"] == "2026-04-30T12:00:00"
    assert result["ProcessingEndTime"] == "2026-04-30T12:05:00"

    # DELETE — happy path returns None.
    assert await connector.delete(metadata) is None


@pytest.mark.asyncio
@mock.patch("flytekitplugins.awssagemaker_processing.connector.Boto3ConnectorMixin._call")
async def test_get_inprogress_has_no_outputs(mock_call):
    mock_call.return_value = (
        {
            "ProcessingJobName": "prep-x",
            "ProcessingJobArn": PROCESSING_JOB_ARN,
            "ProcessingJobStatus": "InProgress",
        },
        idempotence_token,
    )

    connector = ConnectorRegistry.get_connector("sagemaker-processing-job")
    metadata = SageMakerProcessingJobMetadata(
        config={"ProcessingJobName": "prep-x"}, region=REGION
    )

    resource = await connector.get(metadata)
    assert resource.phase == TaskExecution.RUNNING
    assert resource.outputs is None


@pytest.mark.asyncio
@mock.patch("flytekitplugins.awssagemaker_processing.connector.Boto3ConnectorMixin._call")
async def test_get_failed_surfaces_failure_reason(mock_call):
    mock_call.return_value = (
        {
            "ProcessingJobName": "prep-x",
            "ProcessingJobArn": PROCESSING_JOB_ARN,
            "ProcessingJobStatus": "Failed",
            "FailureReason": "AlgorithmError: script returned non-zero exit code",
            "ExitMessage": "Traceback ...",
        },
        idempotence_token,
    )

    connector = ConnectorRegistry.get_connector("sagemaker-processing-job")
    metadata = SageMakerProcessingJobMetadata(
        config={"ProcessingJobName": "prep-x"}, region=REGION
    )

    resource = await connector.get(metadata)
    assert resource.phase == TaskExecution.FAILED
    assert resource.message == "AlgorithmError: script returned non-zero exit code"
    assert resource.outputs is None


@pytest.mark.asyncio
@mock.patch("flytekitplugins.awssagemaker_processing.connector.Boto3ConnectorMixin._call")
async def test_get_failed_falls_back_to_exit_message(mock_call):
    mock_call.return_value = (
        {
            "ProcessingJobName": "prep-x",
            "ProcessingJobArn": PROCESSING_JOB_ARN,
            "ProcessingJobStatus": "Failed",
            "ExitMessage": "Container exited with code 1",
        },
        idempotence_token,
    )

    connector = ConnectorRegistry.get_connector("sagemaker-processing-job")
    metadata = SageMakerProcessingJobMetadata(
        config={"ProcessingJobName": "prep-x"}, region=REGION
    )

    resource = await connector.get(metadata)
    assert resource.phase == TaskExecution.FAILED
    assert resource.message == "Container exited with code 1"


@pytest.mark.asyncio
@mock.patch("flytekitplugins.awssagemaker_processing.connector.Boto3ConnectorMixin._call")
async def test_get_stopped_maps_to_failed(mock_call):
    mock_call.return_value = (
        {
            "ProcessingJobName": "prep-x",
            "ProcessingJobArn": PROCESSING_JOB_ARN,
            "ProcessingJobStatus": "Stopped",
            "FailureReason": "MaxRuntimeExceeded",
        },
        idempotence_token,
    )

    connector = ConnectorRegistry.get_connector("sagemaker-processing-job")
    metadata = SageMakerProcessingJobMetadata(
        config={"ProcessingJobName": "prep-x"}, region=REGION
    )

    resource = await connector.get(metadata)
    assert resource.phase == TaskExecution.FAILED
    assert resource.message == "MaxRuntimeExceeded"


@pytest.mark.asyncio
@mock.patch("flytekitplugins.awssagemaker_processing.connector.Boto3ConnectorMixin._call")
async def test_create_already_exists_returns_metadata(mock_call):
    mock_call.side_effect = CustomException(
        message="An error occurred",
        idempotence_token=idempotence_token,
        original_exception=ClientError(
            error_response={
                "Error": {
                    "Code": "ResourceInUse",
                    "Message": "Processing job prep-74443947857331f7 already exists",
                }
            },
            operation_name="CreateProcessingJob",
        ),
    )

    connector = ConnectorRegistry.get_connector("sagemaker-processing-job")
    response = await connector.create(_task_template())
    assert response.config == _task_config()["config"]
    assert response.region == REGION


@pytest.mark.asyncio
@mock.patch("flytekitplugins.awssagemaker_processing.connector.Boto3ConnectorMixin._call")
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
            operation_name="CreateProcessingJob",
        ),
    )

    connector = ConnectorRegistry.get_connector("sagemaker-processing-job")
    with pytest.raises(CustomException):
        await connector.create(_task_template())


@pytest.mark.asyncio
@mock.patch("flytekitplugins.awssagemaker_processing.connector.Boto3ConnectorMixin._call")
async def test_create_unknown_error_propagates(mock_call):
    mock_call.side_effect = CustomException(
        message="An error occurred",
        idempotence_token=idempotence_token,
        original_exception=ClientError(
            error_response={
                "Error": {"Code": "AccessDeniedException", "Message": "nope"}
            },
            operation_name="CreateProcessingJob",
        ),
    )

    connector = ConnectorRegistry.get_connector("sagemaker-processing-job")
    with pytest.raises(CustomException):
        await connector.create(_task_template())


@pytest.mark.asyncio
@mock.patch("flytekitplugins.awssagemaker_processing.connector.Boto3ConnectorMixin._call")
async def test_delete_swallows_terminal_job_error(mock_call):
    """If Flyte calls delete() after the job naturally finished, stop_processing_job
    raises ValidationException — the connector must swallow that specific case."""
    mock_call.side_effect = CustomException(
        message="An error occurred",
        idempotence_token=idempotence_token,
        original_exception=ClientError(
            error_response={
                "Error": {
                    "Code": "ValidationException",
                    "Message": "The request was rejected because the processing job is not in a non-running state",
                }
            },
            operation_name="StopProcessingJob",
        ),
    )

    connector = ConnectorRegistry.get_connector("sagemaker-processing-job")
    metadata = SageMakerProcessingJobMetadata(
        config={"ProcessingJobName": "prep-x"}, region=REGION
    )

    # Should NOT raise.
    assert await connector.delete(metadata) is None


@pytest.mark.asyncio
@mock.patch("flytekitplugins.awssagemaker_processing.connector.Boto3ConnectorMixin._call")
async def test_delete_swallows_resource_not_found(mock_call):
    mock_call.side_effect = CustomException(
        message="An error occurred",
        idempotence_token=idempotence_token,
        original_exception=ClientError(
            error_response={
                "Error": {
                    "Code": "ResourceNotFound",
                    "Message": "Processing job does not exist",
                }
            },
            operation_name="StopProcessingJob",
        ),
    )

    connector = ConnectorRegistry.get_connector("sagemaker-processing-job")
    metadata = SageMakerProcessingJobMetadata(
        config={"ProcessingJobName": "prep-x"}, region=REGION
    )
    assert await connector.delete(metadata) is None


@pytest.mark.asyncio
@mock.patch("flytekitplugins.awssagemaker_processing.connector.Boto3ConnectorMixin._call")
async def test_delete_propagates_other_errors(mock_call):
    mock_call.side_effect = CustomException(
        message="An error occurred",
        idempotence_token=idempotence_token,
        original_exception=ClientError(
            error_response={
                "Error": {"Code": "AccessDeniedException", "Message": "nope"}
            },
            operation_name="StopProcessingJob",
        ),
    )

    connector = ConnectorRegistry.get_connector("sagemaker-processing-job")
    metadata = SageMakerProcessingJobMetadata(
        config={"ProcessingJobName": "prep-x"}, region=REGION
    )

    with pytest.raises(CustomException):
        await connector.delete(metadata)
